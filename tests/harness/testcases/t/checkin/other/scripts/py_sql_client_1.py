#!/usr/bin/python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
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

"""
Unit tests for scidblib.psql_client module.
"""

import argparse
import os
import pwd as pw
import re
import sys
import traceback

from scidblib import AppError
from scidblib.iquery_client import IQuery
from scidblib.psql_client import Psql
from simple_test_runner import SimpleTestRunner


_args = None                    # Parsed arguments
_pgm = None                     # Program name
_iquery = None                  # IQuery client used by tests
_psql = None                    # Psql client etc.

TEST_FLAG = 0x8                 # Hopefully unused in ArrayDesc::ArrayFlags


class TheTest(SimpleTestRunner):

    def __init__(self):
        super(TheTest, self).__init__()
        self.victim = "py_sql_victim_%d" % _args.run_id

    def setUpClass(self):
        """Make "guinea pig" array."""
        _, err = _iquery("create array %s<v:int64>[i=0:9]" % self.victim)
        assert _iquery.returncode == 0, "Cannot create victim array: %s" % err
        _iquery("store(build({0}, 10-i), {0})".format(self.victim))
        assert _iquery.returncode == 0, "Cannot load victim array"
        _iquery("store(build({0}, 2*i), {0})".format(self.victim))
        assert _iquery.returncode == 0, "Cannot reload victim array"

    def tearDownClass(self):
        """Undo setup actions."""
        _iquery("remove(%s)" % self.victim)
        assert _iquery.returncode == 0, "Cannot remove victim array"

    def test_00_database_exists(self):
        """Check that DB_NAME exists"""
        x = _psql("select 1 from pg_catalog.pg_database where datname = '%s'" %
                  _args.db_name)
        assert x, "Database '%s' does not exist" % _args.db_name

    def test_01_database_missing(self):
        """Check that a bogus_db_name does not exist"""
        x = _psql(
            "select 1 from pg_catalog.pg_database where datname = 'bogusdb'")
        assert not x, "Database 'bogusdb' exists?!"

    def test_03_array_exists(self):
        """Check that victim array exists"""
        x = _psql("select 1 from \"array\" where name = '%s'" % self.victim)
        assert x, "Array %s does not exist" % self.victim

    def test_04_array_missing(self):
        """Check that improbably named array array does not exist"""
        x = _psql("select 1 from \"array\" where name = 'improbably_named'")
        assert not x, "Array 'improbably_named' exists?!"

    def test_05_invalid_user_needs_password(self):
        """Invalid dbuser would cause password prompting"""
        saved_dbuser = _psql.user
        saved_options = _psql.options
        _psql.user = 'bogusbob'
        _psql.options = ['--no-password']
        try:
            _psql('select * from "array"')
            assert False, "Bogus user did not need password?!"
        except AppError as e:
            # Different Postgres versions give different error
            # messages for this failure; the important thing is that
            # we failed.
            pass
        finally:
            _psql.user = saved_dbuser
            _psql.options = saved_options

    def test_06_select_tuples(self):
        """Read namedtuples from a table"""
        tbl = _psql("select * from \"array\" where name like '{0}%'".format(
                self.victim))
        assert len(tbl) == 3, "Unexpected number of victim versions"
        for entry in tbl:
            assert entry.name.startswith(self.victim), (
                "Unexpected victim array tuple: %s" % entry.name)
            try:
                int(entry.id)
            except ValueError as e:
                assert False, "Non-integer array id?!"
            # Prep for next test: demonstrate TEST_FLAG is not set.
            assert int(entry.flags) & TEST_FLAG == 0, "Test flag already set?!"

    def test_07_update_array_flag(self):
        """Update, verify, and reset the victim array's flag field"""
        # Get original flags value.
        tbl = _psql("select * from \"array\" where name = '%s'" % self.victim)
        assert len(tbl) == 1, "More than one victim?!"
        old_flags = int(tbl[0].flags)
        assert (old_flags & TEST_FLAG) == 0, "Test flag already set?!"
        # Set the test flag.
        _psql("update \"array\" set flags = (flags | {0}) "
              "where name = '{1}'".format(TEST_FLAG, self.victim))
        # Read back all records with TEST_FLAG set, should be just the one.
        tbl = _psql('select * from "array" where (flags & {0})!= 0'.format(
                TEST_FLAG))
        assert len(tbl) == 1, "More than one array with TEST_FLAG set?!"
        assert tbl[0].name == self.victim, "Set TEST_FLAG on wrong record?!"
        aid = int(tbl[0].id)
        # Reset TEST_FLAG, this time based on array id rather than name.
        _psql('update "array" set flags = (flags & ~{0}) where id = {1}'.format(
                TEST_FLAG, aid))
        # Verify old_flags are in place again.
        tbl = _psql("select * from \"array\" where name = '%s'" % self.victim)
        assert len(tbl) == 1, "More than one victim!?"
        assert int(tbl[0].flags) == old_flags, "Unexpected flags change?!"

    def test_08_drop_db_in_use(self):
        """Cannot drop a database that has active connections"""
        # Paranoid: prove SciDB is running so we *know* we won't
        # actually drop any database here!
        _, _ = _iquery("list('queries')")
        assert _iquery.returncode == 0, "SciDB not running!"
        try:
            _psql("drop database %s" % _args.db_name)
        except AppError as e:
            pass
        else:
            assert False, "Successfully dropped active database?!"


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])

    parser = argparse.ArgumentParser(
        description="Unit tests for scidblib.psql_client module.")
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-H', '--db-host', required=True,
                        help='Target host for psql commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-P', '--db-port', default=None,
                        help="Postgres port.")
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')
    parser.add_argument('-u', '--db-user', required=True,
                        help='Postgres role name.')
    parser.add_argument('-d', '--db-name', required=True, 
                        help='Postgres database name.')

    global _args
    _args = parser.parse_args(argv[1:])

    global _iquery
    _iquery = IQuery(afl=True, format='tsv')
    if _args.host:
        _iquery.host = _args.host
    if _args.port:
        _iquery.port = _args.port

    global _psql
    _psql = Psql()
    _psql.user = _args.db_user
    _psql.host = _args.db_host
    if _args.db_port:
        _psql.port = _args.db_port

    try:
        tt = TheTest()
        return tt.run()
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2

if __name__ == '__main__':
    sys.exit(main())
