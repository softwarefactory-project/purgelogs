#!/usr/bin/env python3
#
# Copyright 2020 Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import gzip
import json
import logging
import shutil
import sys
import time
import yaml

from datetime import datetime, timedelta
from logging import Logger
from pathlib import Path
from typing import List, Generator, Optional, Tuple, Set, Dict

# DirContent now holds sets of Path objects for both directories and files
DirContent = Tuple[Set[Path], Set[Path]]

def check_dir_path(log_path: str) -> Optional[Path]:
    """Ensures the initial directory is valid."""
    p = Path(log_path)
    if not p.exists():
        return None
    return p.resolve()

def delete_dir(dir_path: Path) -> None:
    """Recursively deletes a path."""
    shutil.rmtree(dir_path)

def get_jobdir(dirs: Set[Path], files: Set[Path]) -> bool:
    """Check if directory content is a job dir."""
    dirs_name = {d.name for d in dirs}
    files_name = {f.name for f in files}
    is_zuul = 'zuul-info' in dirs_name
    is_jenkins = 'ara-database' in dirs_name
    is_jenkins_console = 'consoleText.txt' in files_name
    is_empty_dir = not files and not dirs

    return is_zuul or is_jenkins or is_jenkins_console or is_empty_dir

def ls(dir_path: Path) -> DirContent:
    """Returns the absolute list of directories and files in a directory, ignoring symlinks."""
    dirs: Set[Path] = set()
    files: Set[Path] = set()
    try:
        for entry in dir_path.iterdir():
            if not entry.is_symlink():
                if entry.is_dir():
                    dirs.add(entry)
                elif entry.is_file():
                    files.add(entry)
    except FileNotFoundError:
        # A directory could be deleted by a parallel run
        pass
    return (dirs, files)

def extract_buildset_from_inventory(job_dir: Path) -> Optional[str]:
    """Extract buildset value from zuul-info/inventory.yaml file."""
    inventory_path = job_dir / "zuul-info" / "inventory.yaml"
    if not inventory_path.exists():
        return None
    
    try:
        with open(inventory_path, 'r') as f:
            inventory_data = yaml.safe_load(f)
            return inventory_data.get('all', {}).get('vars', {}).get('zuul', {}).get('buildset')
    except (yaml.YAMLError, IOError, KeyError):
        return None

def extract_project_canonical_name_from_inventory(job_dir: Path) -> Optional[str]:
    """Extract project canonical name from zuul-info/inventory.yaml file."""
    inventory_path = job_dir / "zuul-info" / "inventory.yaml"
    if not inventory_path.exists():
        return None
    
    try:
        with open(inventory_path, 'r') as f:
            inventory_data = yaml.safe_load(f)
            return inventory_data.get('all', {}).get('vars', {}).get('zuul', {}).get('project', {}).get('canonical_name')
    except (yaml.YAMLError, IOError, KeyError):
        return None

def check_job_success(job_dir: Path) -> bool:
    """Check if a job is successful by examining job-output.json.gz for zero failures."""
    job_output_path = job_dir / "job-output.json.gz"
    if not job_output_path.exists():
        return False
    
    try:
        with gzip.open(job_output_path, 'rt') as f:
            job_data = json.load(f)
            # Check JSON path .[].stats.container.failures
            if isinstance(job_data, list):
                for item in job_data:
                    if isinstance(item, dict):
                        failures = item.get('stats', {}).get('container', {}).get('failures', 1)
                        if failures != 0:
                            return False
                return True
            return False
    except (json.JSONDecodeError, IOError, KeyError):
        return False

def find_all_job_dirs(log: Logger, log_path: Path) -> Generator[Path, None, None]:
    """Finds all job directories in the log path."""
    queue: Set[Path] = {log_path}
    while queue:
        root = queue.pop()
        current_dirs, current_files = ls(root)

        if get_jobdir(current_dirs, current_files):
            log.debug("%s : is a job dir", root)
            yield root
        else:
            log.debug("%s : walking", root)
            queue.update(current_dirs)

def find_latest_successful_buildsets_per_project(log: Logger, log_path: Path) -> Set[str]:
    """Find the latest successful buildset for each project."""
    # Group jobs by project and buildset
    project_buildsets: Dict[str, Dict[str, List[Path]]] = {}
    
    for job_dir in find_all_job_dirs(log, log_path):
        project_name = extract_project_canonical_name_from_inventory(job_dir)
        buildset = extract_buildset_from_inventory(job_dir)
        
        if project_name and buildset:
            if project_name not in project_buildsets:
                project_buildsets[project_name] = {}
            if buildset not in project_buildsets[project_name]:
                project_buildsets[project_name][buildset] = []
            project_buildsets[project_name][buildset].append(job_dir)
    
    latest_successful_buildsets: Set[str] = set()
    
    for project_name, buildsets in project_buildsets.items():
        log.debug("Checking project %s with %d buildsets", project_name, len(buildsets))
        
        # Find successful buildsets with their timestamps
        successful_buildsets_with_time: List[Tuple[str, datetime]] = []
        
        for buildset, job_dirs in buildsets.items():
            log.debug("Checking buildset %s with %d jobs", buildset, len(job_dirs))
            
            # Check if all jobs in this buildset are successful
            all_successful = True
            buildset_time = datetime.min
            
            for job_dir in job_dirs:
                if not check_job_success(job_dir):
                    log.debug("Job %s in buildset %s has failures", job_dir, buildset)
                    all_successful = False
                    break
                    
                # Track the most recent job time in this buildset
                try:
                    job_time = datetime.fromtimestamp(job_dir.stat().st_mtime)
                    if job_time > buildset_time:
                        buildset_time = job_time
                except FileNotFoundError:
                    continue
            
            if all_successful and job_dirs and buildset_time != datetime.min:
                successful_buildsets_with_time.append((buildset, buildset_time))
                log.debug("Buildset %s is successful with time %s", buildset, buildset_time)
        
        # Find the latest successful buildset for this project
        if successful_buildsets_with_time:
            # Sort by time and get the most recent
            latest_buildset = max(successful_buildsets_with_time, key=lambda x: x[1])
            latest_successful_buildsets.add(latest_buildset[0])
            log.info("Project %s: keeping latest successful buildset %s (time: %s)", 
                    project_name, latest_buildset[0], latest_buildset[1])
            
            # Log older successful buildsets that will not be protected
            older_buildsets = [bs[0] for bs in successful_buildsets_with_time if bs[0] != latest_buildset[0]]
            if older_buildsets:
                log.info("Project %s: older successful buildsets will not be protected: %s", 
                        project_name, ", ".join(older_buildsets))
        else:
            log.debug("Project %s has no successful buildsets", project_name)
    
    return latest_successful_buildsets

def find_old_files(log: Logger, calculated_time: datetime, log_path: Path, protect_successful_buildsets: bool = False) -> Generator[Path, None, None]:
    """Finds old files in the log path, stopping when a directory is a jobdir."""
    protected_buildsets: Set[str] = set()
    if protect_successful_buildsets:
        protected_buildsets = find_latest_successful_buildsets_per_project(log, log_path)
    
    queue: Set[Path] = {log_path}
    while queue:
        root = queue.pop()
        current_dirs, current_files = ls(root)

        if get_jobdir(current_dirs, current_files):
            log.debug("%s : is a job dir", root)
            try:
                dir_date = datetime.fromtimestamp(root.stat().st_mtime)
                if dir_date < calculated_time:
                    # Check if this job belongs to a protected buildset (latest successful per project)
                    if protect_successful_buildsets:
                        buildset = extract_buildset_from_inventory(root)
                        if buildset and buildset in protected_buildsets:
                            project_name = extract_project_canonical_name_from_inventory(root)
                            log.info("%s : protected as latest successful buildset %s for project %s", 
                                   root, buildset, project_name)
                            continue
                    yield root
            except FileNotFoundError:
                # The directory might have been deleted since it was listed
                continue
        else:
            log.debug("%s : walking", root)
            queue.update(current_dirs)

def search_and_destroy(log: Logger, calculated_time: datetime, dry_run: bool, log_path: Path, protect_successful_buildsets: bool = False) -> None:
    """Removes log directories that are older than the calculated time."""
    for job_dir in find_old_files(log, calculated_time, log_path, protect_successful_buildsets):
        log.info("Removing old log directory: %s", job_dir)
        if not dry_run and log_path != job_dir:
            delete_dir(job_dir)

def parse_args(argv: List[str]) -> argparse.Namespace:
    """Parse command line arguments.

    >>> parse_args([])
    Namespace(debug=False, dry_run=False, log_path_dir='/var/www/logs', loop=None, retention_days=31, build_success=False)
    """
    parser = argparse.ArgumentParser(description="Purge old logs")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging.")
    parser.add_argument('--dry-run', action='store_true', help="Do not delete anything, just print what would be done.")
    parser.add_argument('--log-path-dir', default='/var/www/logs', help="The root directory of the logs to purge.")
    parser.add_argument('--loop', type=int, metavar="SECONDS", help="Continuously run every SECONDS.")
    parser.add_argument('--retention-days', type=int, default=31, help="The age in days of logs to keep.")
    parser.add_argument('--build-success', action='store_true', help="Protect only the latest successful buildset per project from deletion (projects identified by .all.vars.zuul.project.canonical_name).")
    return parser.parse_args(argv)

def setup_logging(debug: bool) -> Logger:
    """Create the logger with a nice format string."""
    logging.basicConfig(
        format='%(asctime)s %(levelname)-5.5s %(message)s',
        level=logging.DEBUG if debug else logging.INFO)
    return logging.getLogger()

def main() -> None:
    """The script entrypoint."""
    args = parse_args(sys.argv[1:])
    root = check_dir_path(args.log_path_dir)
    log = setup_logging(args.debug)
    if not root:
        log.error("The provided log path dir does not exist: %s", args.log_path_dir)
        sys.exit(1)

    while True:
        log.info("Starting cleaning-up log dir: %s", root)
        calculated_time = datetime.now() - timedelta(days=args.retention_days)
        search_and_destroy(log, calculated_time, args.dry_run, root, args.build_success)
        if not args.loop:
            break
        log.info("Cleanup done! Sleeping for %s seconds...", args.loop)
        time.sleep(args.loop)

if __name__ == "__main__":
    main()
