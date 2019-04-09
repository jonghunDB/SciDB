#!/usr/bin/python
# encoding: utf-8
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2018 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

"""
Make sure all new Python code follows the PEP-8 style guide.
"""

import argparse
import os
import platform
import re
import subprocess as subp
import sys
import textwrap
import traceback

from scidblib import AppError
from simple_test_runner import (dbg, prt, warn)

_args = None                    # Parsed arguments
_pgm = None                     # Program name
_topdir = None                  # Current input directory
_exclusion_list = []            # Scripts to forgive (for now)


def relpath(path):
    """Return path relative to SCIDB_SOURCE_PATH."""
    if not path.startswith(_topdir):
        return path
    return path[len(_topdir):]


def check_compliance(path):
    """Run style checker on 'path', return (num_errors, is_excluded)"""
    rpath = relpath(path)
    # The following line tests itself!  Take *that*, Kurt Gŏdel!!!
    p = subp.Popen(['flake8', '--max-line-length=99', path], stdout=subp.PIPE, stderr=subp.PIPE)
    out, err = p.communicate()

    # Default output format prints one line per error.
    n_errs = len(out.splitlines())

    # Success.
    if p.returncode == 0:
        assert not n_errs, "Non-zero flake8 exit, but non-empty output"
        prt("Check:", rpath, "... ok")
        if rpath in _exclusion_list:
            warn("Remove", rpath, "from the exclusion list, it is fine!")
            return 0, True
        else:
            return 0, False

    # Errors, but ignore them.
    if rpath in _exclusion_list:
        prt("Check:", rpath, "... excluded,", n_errs, "errors")
        dbg(out)
        return n_errs, True

    # Get with the PEP-8 program!  Bad style detected, AROOOOOGAH!!!!
    prt("Check:", rpath, "... FAILED,", n_errs, "errors")
    dbg(out)
    return n_errs, False


def load_exclusions(filename):
    """Load the exclusion list."""
    global _exclusion_list
    if filename is None:
        base = os.path.splitext(_pgm)[0]
        filename = os.path.join(os.environ.get('TESTDIR', os.curdir),
                                os.extsep.join((base, 'exclude')))
    if not os.path.isfile(filename):
        warn("Exclusion file", filename, "is missing")
        return
    with open(filename) as F:
        for line in F:
            line = re.sub(r'#.*$', '', line).strip()
            if not line:
                continue
            _exclusion_list.append(line)
    prt("Loaded", len(_exclusion_list), "exclusions")


def process(topdir):
    """Run the style checker on all .py and .py.in files."""
    # Statistics.
    n_files = 0
    n_fails = 0
    n_p8errs = 0
    n_excl = 0

    # Set module variable so we don't have to pass topdir around
    # everywhere.
    global _topdir
    if not topdir.endswith(os.sep):
        topdir += os.sep
    _topdir = topdir

    # Walk the source tree.
    for dirpath, dirs, files in os.walk(_topdir):
        # Lots of .py copied to stage/, don't bother with it.
        for subdir in ('.git', 'stage'):
            if subdir in dirs:
                dirs.remove(subdir)
        for f in files:
            if not (f.endswith('.py') or f.endswith('.py.in')):
                continue
            n_files += 1
            fails, excl = check_compliance(os.path.join(dirpath, f))
            if excl:
                n_excl += 1
            elif fails:
                n_fails += 1
                n_p8errs += fails

    prt("Checked", n_files, "files,", n_excl, "excluded,",
        n_fails, "failures,", n_p8errs, "total errors found")

    if n_fails:
        prt(textwrap.dedent("""

        One or more new Python scripts do not follow the PEP-8 style
        guidelines (https://www.python.org/dev/peps/pep-0008/).  To
        see the list of problems, run

            $ flake8 <filename>

        All new Python code must pass the style checker with no
        special options.  Suck it up.

        """)[1:])

    return n_fails


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by prt()

    parser = argparse.ArgumentParser(
        description="Username and password validity test.")
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-e', '--exclude-file', default=None,
                        help="""Specify a file listing non-compliant scripts.
                        These are checked but errors do not fail the test.""")
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')
    parser.add_argument('-v', '--verbose', default=0, action='count', help="""
                        Increase debug logging. 1=info, 2=debug, 3=debug+""")
    parser.add_argument('dirs', nargs='*', help="""Directories to scan""")

    global _args
    _args = parser.parse_args(argv[1:])
    if _args.verbose:
        os.environ['SCIDB_DBG'] = '1'
        dbg(_pgm, "Debug mode enabled")

    # No working flake8 on C6/RH6, but we don't care so long as the
    # test is passing on C7/RH7/Ubuntu.
    distro, version, _ = platform.dist()
    if distro in ('centos', 'redhat') and version.startswith('6.'):
        prt(_pgm, "Test disabled on", distro,
            "6.x because flake8 does not support this platform.")
        return 0

    # No arguments means we're probably running in CDash and the
    # source path envariable is undefined.
    if not _args.dirs:
        prt(_pgm, "Nothing to do")
        return 0

    try:
        load_exclusions(_args.exclude_file)
        fail = 0
        for d in _args.dirs:
            fail += process(d)
        return fail
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
