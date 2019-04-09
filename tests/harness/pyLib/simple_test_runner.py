#!/usr/bin/python
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
The dirt-simple test runner and related useful routines.
"""

import os
import sys

stderr = sys.stderr
verbose = int(os.environ.get("SCIDB_DBG", "0"))

# Test runner looks for methods in derived classes that start with
# this.
TEST_PREFIX = 'test_'


class SkipException(Exception):
    """Raise if test should be skipped."""
    pass


def dbg(*args):
    """Debug logging."""
    if verbose:
        print >>stderr, "DBG:", ' '.join(str(x) for x in args)


def warn(*args):
    """Warnings."""
    print >>stderr, ' '.join(str(x) for x in args)


def prt(*args, **kwargs):
    """Ordinary output.  Pass crlf=False to suppress newlines."""
    try:
        crlf = kwargs['crlf']
    except KeyError:
        crlf = True
    text = ' '.join(str(x) for x in args)
    if crlf:
        print text
    else:
        print text,  # no trailing newline
    sys.stdout.flush()


class SimpleTestRunner(object):

    """
    A dirt-simple test runner.

    The version of unittest in Python 2.6 is so old and crufty that it
    doesn't support setUpClass() and setUpModule() methods.  This
    simple test runner covers most needs.
    """
    _stderr = sys.stderr

    def setUpClass(self):
        """Run once at test startup.  Override this."""
        prt("Setup ...", crlf=False)
        # Do stuff...
        prt("done")

    def tearDownClass(self):
        """Run once at test completion.  Override this."""
        prt("Teardown ...", crlf=False)
        # Do stuff...
        prt("done")

    # Add test methods that begin with the TEST_PREFIX.  Your tests
    # will be run in sorted order, so you may want to number them.
    #
    # For example (using scidblib.iquery_client):
    #
    #   def test_00_verify_good_user(self):
    #       """Login as GOOD_USER works fine"""
    #       _, err = _iquery("list()", auth=(GOOD_USER, GOOD_PASS))
    #       assert _iquery.returncode == 0, "Good user login failed"
    #       assert not err, "Good user login failed but no returncode?!"

    def run(self):
        """Call run() to execute your tests.

        @note DO NOT OVERRIDE!
        @return count of failed tests
        """
        self.setUpClass()
        tests = [x for x in dir(self) if x.startswith(TEST_PREFIX)]
        tests.sort()
        errors = 0
        skipped = 0
        for t in tests:
            print t, "...",
            sys.stdout.flush()
            try:
                getattr(self, t)()
            except SkipException as e:
                print "skipped (%s)" % e
                skipped += 1
            except AssertionError as e:
                print "FAIL (%s)" % e
                errors += 1
            else:
                print "ok"
        self.tearDownClass()
        if skipped:
            print "Skipped", skipped, "of", len(tests), "tests."
        if errors:
            print "Errors in", errors, "of", len(tests) - skipped, "tests run."
        else:
            print "All", len(tests) - skipped, "tests passed."
        return errors


# On import ...
if 'P4_TESTCASES_DIR' in os.environ:
    stderr = sys.stdout
    dbg("Running under the test harness.")
