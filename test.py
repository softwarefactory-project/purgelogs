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

import logging
import os
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Generator, List, Union, NewType, Tuple

import purgelogs

yesterday = datetime.now() - timedelta(days=1)

def mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def touch(path: Path, date: datetime) -> None:
    path.touch(exist_ok=True)
    os.utime(path, (date.timestamp(), date.timestamp()), follow_symlinks=False)

def touch_old(path: Path) -> None:
    touch(path, datetime.fromtimestamp(0))

def mk_old_build(path: Path) -> None:
    mkdir(path / "zuul-info")
    touch_old(path)

@contextmanager
def setup_tree(tree: Callable[[Path], None]) -> Generator[Path, None, None]:
    root = Path(tempfile.mkdtemp(prefix="purgelogs"))
    try:
        tree(root)
        yield root
    finally:
        shutil.rmtree(root)

def test_purge_symlink() -> None:
    def tree(root: Path) -> None:
        mkdir(root / "common")
        (root / "common/current").symlink_to(root / "common")
        touch_old(root / "common" / "current")
        touch_old(root / "common")
        mkdir(root / "test")
    with setup_tree(tree) as root:
        purgelogs.search_and_destroy(logging.getLogger(), yesterday, False, root)
        test = unittest.TestCase()
        test.assertTrue((root / "test").is_dir())
        test.assertFalse((root / "common").is_dir())

def test_retain() -> None:
    def tree(root: Path) -> None:
        # Create fake builds
        mk_old_build(root / "build-1")
        mk_old_build(root / "build-2")
        # Mark the build-1 as a success-builds
        mkdir(root / "success-builds")
        (root / "success-builds" / "keep-me").symlink_to(root / "build-1")
        touch_old(root / "success-builds")
    with setup_tree(tree) as root:
        purgelogs.search_and_destroy(logging.getLogger(), yesterday, False, root)
        test = unittest.TestCase()
        # Check that build-1 is retained
        test.assertTrue((root / "build-1").is_dir())
        # Check that build-2 got purged
        test.assertFalse((root / "build-2").is_dir())
        # Check that success-builds is also preserved
        test.assertTrue((root / "success-builds" / "keep-me").is_symlink())


if __name__ == '__main__':
    unittest.main()
