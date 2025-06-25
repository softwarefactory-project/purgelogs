# Copyright (c) 2020 Red Hat
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

import gzip
import json
import logging
import os
import shutil
import tempfile
import unittest
import yaml
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Generator

import purgelogs

# Define a fixed "now" for consistent testing
NOW = datetime.now()
OLD_TIME = NOW - timedelta(days=60)
RECENT_TIME = NOW - timedelta(days=1)
OLDER_TIME = NOW - timedelta(days=90)

def mkdir(path: Path) -> None:
    """Create a directory and any missing parent directories."""
    path.mkdir(parents=True, exist_ok=True)

def touch(path: Path, date: datetime) -> None:
    """Create a file and set its modification time."""
    path.touch(exist_ok=True)
    os.utime(path, (date.timestamp(), date.timestamp()), follow_symlinks=False)

def create_inventory_yaml(job_dir: Path, buildset: str, project_name: str = "test-project") -> None:
    """Create a zuul-info/inventory.yaml file with the specified buildset and project."""
    inventory_path = job_dir / "zuul-info" / "inventory.yaml"
    inventory_data = {
        'all': {
            'vars': {
                'zuul': {
                    'buildset': buildset,
                    'project': {
                        'canonical_name': project_name
                    }
                }
            }
        }
    }
    with open(inventory_path, 'w') as f:
        yaml.dump(inventory_data, f)

def create_job_output(job_dir: Path, failures: int = 0) -> None:
    """Create a job-output.json.gz file with specified failure count."""
    job_output_path = job_dir / "job-output.json.gz"
    job_data = [
        {
            'stats': {
                'container': {
                    'failures': failures
                }
            }
        }
    ]
    with gzip.open(job_output_path, 'wt') as f:
        json.dump(job_data, f)

@contextmanager
def setup_tree(tree: Callable[[Path], None]) -> Generator[Path, None, None]:
    """Context manager to create and clean up a temporary directory structure for testing."""
    root = Path(tempfile.mkdtemp(prefix="purgelogs-"))
    try:
        tree(root)
        yield root
    finally:
        shutil.rmtree(root)

class PurgeLogsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.log = logging.getLogger("purgelogs_test")
        self.log.setLevel(logging.CRITICAL)  # Suppress logs during tests
        self.cutoff_date = NOW - timedelta(days=30)

    def test_simple_purge(self) -> None:
        """Test that a simple, old job directory gets purged."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")
            touch(job_dir, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root)
            self.assertFalse((root / "job1").exists(), "Old job directory should have been purged")

    def test_keep_recent(self) -> None:
        """Test that a recent job directory is not purged."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")
            touch(job_dir, RECENT_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root)
            self.assertTrue((root / "job1").exists(), "Recent job directory should not be purged")

    def test_dry_run(self) -> None:
        """Test that --dry-run prevents any deletion."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")
            touch(job_dir, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, True, root)
            self.assertTrue((root / "job1").exists(), "Directory should not be deleted in dry-run mode")

    def test_symlink_is_ignored(self) -> None:
        """Test that symlinks are ignored and not deleted."""
        def tree(root: Path) -> None:
            real_dir = root / "real"
            mkdir(real_dir)
            touch(real_dir, OLD_TIME)

            link_dir = root / "link"
            link_dir.symlink_to(real_dir)
            touch(link_dir, OLD_TIME) # utime on symlink itself

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root)
            self.assertTrue((root / "real").exists(), "Real directory pointed to by symlink should still exist")
            self.assertTrue((root / "link").is_symlink(), "Symlink should still exist")

    def test_purge_empty_dir(self) -> None:
        """Test that an old, empty directory is purged."""
        def tree(root: Path) -> None:
            empty_dir = root / "empty"
            mkdir(empty_dir)
            touch(empty_dir, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root)
            self.assertFalse((root / "empty").exists(), "Old empty directory should be purged")

    def test_walk_non_job_dir(self) -> None:
        """Test the script walks through intermediate directories and purges a nested old job."""
        def tree(root: Path) -> None:
            parent = root / "parent"
            job = parent / "job"
            mkdir(job / "zuul-info")
            touch(job, OLD_TIME)
            touch(parent, RECENT_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root)
            self.assertFalse((root / "parent" / "job").exists(), "Nested old job should be purged")
            self.assertTrue((root / "parent").exists(), "Parent directory should not be purged")

    def test_extract_buildset_from_inventory(self) -> None:
        """Test that buildset values are correctly extracted from inventory.yaml files."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")
            create_inventory_yaml(job_dir, "abc123", "test-project")

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            buildset = purgelogs.extract_buildset_from_inventory(job_dir)
            self.assertEqual(buildset, "abc123", "Should extract correct buildset value")

    def test_extract_project_canonical_name_from_inventory(self) -> None:
        """Test that project canonical names are correctly extracted from inventory.yaml files."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")
            create_inventory_yaml(job_dir, "abc123", "my-project")

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            project_name = purgelogs.extract_project_canonical_name_from_inventory(job_dir)
            self.assertEqual(project_name, "my-project", "Should extract correct project canonical name")

    def test_extract_buildset_missing_file(self) -> None:
        """Test that extracting buildset returns None when inventory.yaml doesn't exist."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir / "zuul-info")

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            buildset = purgelogs.extract_buildset_from_inventory(job_dir)
            self.assertIsNone(buildset, "Should return None when inventory.yaml doesn't exist")

    def test_check_job_success_zero_failures(self) -> None:
        """Test that job with zero failures is considered successful."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir)
            create_job_output(job_dir, failures=0)

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            success = purgelogs.check_job_success(job_dir)
            self.assertTrue(success, "Job with zero failures should be successful")

    def test_check_job_success_with_failures(self) -> None:
        """Test that job with failures is not considered successful."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir)
            create_job_output(job_dir, failures=1)

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            success = purgelogs.check_job_success(job_dir)
            self.assertFalse(success, "Job with failures should not be successful")

    def test_check_job_success_missing_file(self) -> None:
        """Test that job without job-output.json.gz is not considered successful."""
        def tree(root: Path) -> None:
            job_dir = root / "job1"
            mkdir(job_dir)

        with setup_tree(tree) as root:
            job_dir = root / "job1"
            success = purgelogs.check_job_success(job_dir)
            self.assertFalse(success, "Job without job-output.json.gz should not be successful")

    def test_protect_latest_successful_buildset_per_project(self) -> None:
        """Test that only the latest successful buildset per project is protected."""
        def tree(root: Path) -> None:
            # Project A: older successful buildset (should be deleted)
            job1 = root / "project-a-job1-old"
            mkdir(job1 / "zuul-info")
            create_inventory_yaml(job1, "project-a-buildset-1", "project-a")
            create_job_output(job1, failures=0)
            touch(job1, OLDER_TIME)

            job2 = root / "project-a-job2-old"
            mkdir(job2 / "zuul-info")
            create_inventory_yaml(job2, "project-a-buildset-1", "project-a")
            create_job_output(job2, failures=0)
            touch(job2, OLDER_TIME)

            # Project A: latest successful buildset (should be protected)
            job3 = root / "project-a-job1-latest"
            mkdir(job3 / "zuul-info")
            create_inventory_yaml(job3, "project-a-buildset-2", "project-a")
            create_job_output(job3, failures=0)
            touch(job3, OLD_TIME)

            job4 = root / "project-a-job2-latest"
            mkdir(job4 / "zuul-info")
            create_inventory_yaml(job4, "project-a-buildset-2", "project-a")
            create_job_output(job4, failures=0)
            touch(job4, OLD_TIME)

            # Project B: only one successful buildset (should be protected)
            job5 = root / "project-b-job1"
            mkdir(job5 / "zuul-info")
            create_inventory_yaml(job5, "project-b-buildset-1", "project-b")
            create_job_output(job5, failures=0)
            touch(job5, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root, True)
            
            # Project A: older buildset should be deleted
            self.assertFalse((root / "project-a-job1-old").exists(), "Older successful buildset should be deleted")
            self.assertFalse((root / "project-a-job2-old").exists(), "Older successful buildset should be deleted")
            
            # Project A: latest buildset should be protected
            self.assertTrue((root / "project-a-job1-latest").exists(), "Latest successful buildset should be protected")
            self.assertTrue((root / "project-a-job2-latest").exists(), "Latest successful buildset should be protected")
            
            # Project B: only buildset should be protected
            self.assertTrue((root / "project-b-job1").exists(), "Only successful buildset should be protected")

    def test_failed_buildsets_not_protected(self) -> None:
        """Test that failed buildsets are not protected, even if they're the latest."""
        def tree(root: Path) -> None:
            # Latest buildset has failures (should not be protected)
            job1 = root / "project-job1-failed"
            mkdir(job1 / "zuul-info")
            create_inventory_yaml(job1, "project-buildset-1", "project-a")
            create_job_output(job1, failures=1)
            touch(job1, OLD_TIME)

            job2 = root / "project-job2-failed"
            mkdir(job2 / "zuul-info")
            create_inventory_yaml(job2, "project-buildset-1", "project-a")
            create_job_output(job2, failures=0)  # One successful, but buildset overall has failures
            touch(job2, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root, True)
            # Both jobs should be deleted since the buildset has failures
            self.assertFalse((root / "project-job1-failed").exists(), "Failed buildset should not be protected")
            self.assertFalse((root / "project-job2-failed").exists(), "Failed buildset should not be protected")

    def test_build_success_flag_disabled(self) -> None:
        """Test that without --build-success flag, successful buildsets are not protected."""
        def tree(root: Path) -> None:
            job1 = root / "job1"
            mkdir(job1 / "zuul-info")
            create_inventory_yaml(job1, "successful-buildset", "project-a")
            create_job_output(job1, failures=0)
            touch(job1, OLD_TIME)

        with setup_tree(tree) as root:
            # Run without build-success protection
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root, False)
            self.assertFalse((root / "job1").exists(), "Job should be deleted when --build-success is not enabled")

    def test_multiple_projects_latest_buildsets_protected(self) -> None:
        """Test that latest successful buildsets are correctly protected across multiple projects."""
        def tree(root: Path) -> None:
            # Project 1: multiple successful buildsets
            job1 = root / "proj1-old-job"
            mkdir(job1 / "zuul-info")
            create_inventory_yaml(job1, "proj1-buildset-old", "project-1")
            create_job_output(job1, failures=0)
            touch(job1, OLDER_TIME)

            job2 = root / "proj1-latest-job"
            mkdir(job2 / "zuul-info")
            create_inventory_yaml(job2, "proj1-buildset-latest", "project-1")
            create_job_output(job2, failures=0)
            touch(job2, OLD_TIME)

            # Project 2: multiple successful buildsets
            job3 = root / "proj2-old-job"
            mkdir(job3 / "zuul-info")
            create_inventory_yaml(job3, "proj2-buildset-old", "project-2")
            create_job_output(job3, failures=0)
            touch(job3, OLDER_TIME)

            job4 = root / "proj2-latest-job"
            mkdir(job4 / "zuul-info")
            create_inventory_yaml(job4, "proj2-buildset-latest", "project-2")
            create_job_output(job4, failures=0)
            touch(job4, OLD_TIME)

        with setup_tree(tree) as root:
            purgelogs.search_and_destroy(self.log, self.cutoff_date, False, root, True)
            
            # Only latest buildsets should be protected
            self.assertFalse((root / "proj1-old-job").exists(), "Older buildset should be deleted")
            self.assertTrue((root / "proj1-latest-job").exists(), "Latest buildset should be protected")
            self.assertFalse((root / "proj2-old-job").exists(), "Older buildset should be deleted")
            self.assertTrue((root / "proj2-latest-job").exists(), "Latest buildset should be protected")

if __name__ == '__main__':
    unittest.main()
