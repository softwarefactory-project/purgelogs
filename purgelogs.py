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
import sys
from typing import List

def usage(argv: List[str]) -> argparse.Namespace:
    """The script usage

    >>> usage([])
    Namespace()
    """
    parser = argparse.ArgumentParser(description="Purge old logs")
    args = parser.parse_args(argv)
    return args

def main() -> None:
    """The script entrypoint"""
    args = usage(sys.argv[1:])

if __name__ == "__main__":
    main()
