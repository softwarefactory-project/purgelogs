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
import argparse
import logging
import os
import shutil
import sys
import time

from datetime import datetime, timedelta
from logging import Logger
from pathlib import Path
from typing import List, Generator, Optional, Tuple, Set

DirContent = Tuple[Set[Path], Set[str]]

def check_dir_path(log_path: str) -> Optional[Path]:
    """Ensures the initial directory is valid"""
    p = Path(log_path)
    if not p.exists():
        return None
    return p.resolve()

def delete_dir(dir_path: Path) -> None:
    """Recursively deletes a path"""
    shutil.rmtree(dir_path)

def get_jobdir(dirs: Set[Path], files: Set[str]) -> bool:
    """Check if directory content is a job dir"""
    dirs_name = set(map(lambda s: s.name, dirs))
    is_zuul = 'zuul-info' in dirs_name
    is_jenkins = 'ara-database' in dirs_name
    is_jenkins_console = 'consoleText.txt' in files
    is_empty_dir = not files and not dirs

    return is_zuul or is_jenkins or is_jenkins_console or is_empty_dir

def ls(dir_path: Path) -> DirContent:
    """Returns the absolute list of directories and files in a directory, ignoring symlinks"""
    dirs = set()
    files = set()
    for entry in os.listdir(dir_path):
        entry_path = dir_path / entry
        if not entry_path.is_symlink():
            if entry_path.is_dir():
                dirs.add(entry_path)
            elif entry_path.exists():
                files.add(entry)
    return (dirs, files)

def find_old_files(log: Logger, calculated_time: datetime, log_path: Path) -> Generator[Path, None, None]:
    """Finds old files in the log path, stopping when a directory is a jobdir"""
    queue = set((log_path, ))
    while queue:
        root = queue.pop()
        current_dirs, current_files = ls(root)
        if get_jobdir(current_dirs, current_files):
            log.debug("%s : is a job dir", root)
            dir_date = datetime.fromtimestamp(os.path.getmtime(root))
            if dir_date < calculated_time:
                yield root
        else:
            log.debug("%s : walking", root)
            queue = queue.union(current_dirs)

def search_and_destroy(log: Logger, calculated_time: datetime, dry_run: bool, log_path: Path) -> None:
    """Removes log directories that are older than the calculated time"""
    for job_dir in find_old_files(log, calculated_time, log_path):
        log.debug("%s : removing old logs", job_dir)
        if not dry_run and log_path != job_dir:
            delete_dir(job_dir)

def usage(argv: List[str]) -> argparse.Namespace:
    """The script usage

    >>> usage([])
    Namespace(debug=False, dry_run=False, log_path_dir='/var/www/logs', loop=None, retention_days=31)
    """
    parser = argparse.ArgumentParser(description="Purge old logs")
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--log-path-dir', default='/var/www/logs')
    parser.add_argument('--loop', type=int, metavar="SECONDS", help="Continuously run every SECONDS")
    parser.add_argument('--retention-days', type=int, default=31)
    return parser.parse_args(argv)

def setup_logging(debug: bool) -> Logger:
    """Create the logger with a nice format string"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)-5.5s %(message)s',
        level=logging.DEBUG if debug else logging.INFO)
    return logging.getLogger()

def main() -> None:
    """The script entrypoint"""
    args = usage(sys.argv[1:])
    root = check_dir_path(args.log_path_dir)
    log = setup_logging(args.debug)
    if not root:
        log.error("The provided log path dir does not exist")
        exit(1)
    while True:
        logging.info("Starting cleaning-up log dir...")
        calculated_time = datetime.now() - timedelta(days=args.retention_days)
        search_and_destroy(log, calculated_time, args.dry_run, root)
        if not args.loop:
            break
        logging.info("Cleanup done! Sleeping %s..." % args.loop)
        time.sleep(args.loop)

if __name__ == "__main__":
    main()
