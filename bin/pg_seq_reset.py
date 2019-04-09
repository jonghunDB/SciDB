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
Reset Postgres sequence generators for SciDB tables.

BEWARE!!!  Non-atomic!  Use only with a quiescent database.
"""

# Ideally we would NOT run this script, but instead backup/restore the
# catalog with currval(seq) and setval(seq, n) (or "ALTER SEQUENCE
# ... RESTART WITH ..."), see SDB-5165.  However, there appears to be
# no way to just inspect the current values of the sequences.  See:
#
# - https://www.postgresql.org/docs/9.3/static/functions-sequence.html
# - https://dba.stackexchange.com/questions/3281/\
#     how-do-i-use-currval-in-postgresql-to-get-the-last-inserted-id
#
# So we're probably stuck with having to perform similar antics during
# metadata restore.

import argparse
import getpass
import os
import sys
import tempfile
import traceback

from scidblib import AppError
from scidblib.pgpass_updater import (PgpassUpdater, PgpassError)
from scidblib.psql_client import Psql
from scidblib.scidb_psf import confirm

_args = None                    # Parsed arguments
_pgm = None                     # Program name
_psql = None                    # Psql client object

# Sequence names, last updated per the SciDB 18.1 meta.sql schema.
#
# Sequences unassociated with the namespaces plugin don't really need
# a reset: restoring array data will be fine with the sequence values
# left by init-syscat (but it does no harm to reset them here).
#
SEQUENCES = (
    "array_id_seq",
    "array_distribution_id_seq",
    "instance_id_seq",
    "libraries_id_seq",
    "user_id_seq",
    "namespaces_id_seq",
    "role_id_seq",
    # "role_permissions_id_seq",  # UNUSED
    "role_namespace_permissions_id_seq",
)


def dbg(*args):
    if _args.verbose:
        print >>sys.stderr, _pgm, ' '.join(str(x) for x in args)


def warn(*args):
    print >>sys.stderr, _pgm, ' '.join(str(x) for x in args)


def sequence_to_table(seq):
    """Map sequence name to table that uses it.

    Not 100% trivial due to SDB-6061.
    """
    assert seq.endswith("_id_seq"), "Bogus sequence name"
    tbl = seq[:-len("_id_seq")]
    if tbl in ("user", "role"):
        tbl = "%ss" % tbl       # append 's'
    return tbl


def get_max_instance_id():
    """Special processing for finding instance_id_seq restart value."""
    # There's no 'id' column, examine the low 32 bits of the
    # instance_id column instead.  See SystemCatalog::_addInstance()
    # and wiki:DEV/Command_Control/Func+spec+15.12 .
    try:
        iids = _psql("select instance_id from instance")
    except AppError as e:
        warn("Error scanning instance_ids:", e)
        return -1
    if not iids:
        warn("Table 'instance' is empty")
        return -1
    ret = max([int(x.instance_id) & 0xFFFFFFFF for x in iids])
    # For single instance config, ret will be zero.
    assert ret > 0 or (ret == 0 and len(iids) == 1), (
        "Unexpected value for max(instance_id & 0xFF...)?!")
    return ret


def process():
    """Restart each sequence based on max id value in corresponding table."""
    for seq in SEQUENCES:

        tbl = sequence_to_table(seq)
        if tbl == 'instance':
            maxid = get_max_instance_id()
            cmd = "select setval('{0}', {1})".format(seq, maxid)
        else:
            # General case: set sequence value so that nextval() returns
            # max(id) plus one.
            cmd = ("""select setval('{0}', (select max(id) from "{1}"))"""
                   .format(seq, tbl))

        if not _args.execute:
            cmd = "%s%s -- Not yet executed" % (cmd, ' ' * (60 - len(cmd)))
        print cmd
        if _args.execute:
            try:
                _psql(cmd)
            except AppError as e:
                warn("Psql error, SQL:", cmd, "\nError:", e)

    return 0


class PgpassContext(object):

    """Set up a scoped PGPASSFILE if one is needed."""

    def __init__(self, psql, verify=True):
        self._pgpass = None
        self._verify = verify
        # All psql attributes should be filled in by now.
        self._user = psql.user
        self._db = psql.database
        self._host = psql.host
        self._port = psql.port
        self._saved_pgpassfile = os.environ.get("PGPASSFILE")
        dbg("PgpassContext:", self.__dict__)

    def _make_pgpass(self):
        # We've decided we need to prompt for password and create our
        # own pgpass file.
        dbpass = getpass.getpass("Postgres %s password: " % self._user)
        self._pgpass = tempfile.NamedTemporaryFile()
        pup = PgpassUpdater(filename=self._pgpass.name)
        pup.update(self._user, dbpass, self._db, self._host, self._port)
        pup.write_file()
        self._pgpass.flush()
        os.environ['PGPASSFILE'] = self._pgpass.name

    def __enter__(self):
        # If no credentials found in the default pgpass file, prompt
        # for a password and create a temporary pgpass file.
        pup = None
        try:
            pgpass = os.environ.get(
                "PGPASSFILE", os.path.join(os.environ['HOME'], '.pgpass'))
            pup = PgpassUpdater(filename=pgpass)
        except PgpassError as e:
            dbg("PgpassUpdater:", e)

        # Make a pgpass file if we don't have one or it doesn't
        # contain the entry we need.
        if not (pup and pup.find(self._user, self._db,
                                 self._host, self._port)):
            self._make_pgpass()

        # Try a sure-to-work psql command, if it fails the given
        # password was probably wrong.
        if self._verify:
            psql = Psql(host=self._host, port=self._port,
                        database=self._db, user=self._user,
                        debug=_args.verbose,
                        options=['--no-password'])
            try:
                psql("select usename from pg_catalog.pg_user limit 1")
            except Exception as e:
                raise AppError("Password verification failed:\n%s" % e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Get rid of temporary pgpass file, if any.
        if self._pgpass:
            self._pgpass.close()
            # Restore PGPASSFILE, in case main() is being called from
            # another script.
            if self._saved_pgpassfile is None:
                del os.environ["PGPASSFILE"]
            else:
                os.environ["PGPASSFILE"] = self._saved_pgpassfile
        return False            # no exceptions handled here


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    # Where possible, options mimic those of psql(1).
    parser = argparse.ArgumentParser(
        description="""Generate SQL to reset SciDB Postgres sequences.""")
    parser.add_argument('-H', '--host', default='127.0.0.1',  # Not localhost,
                        help="Postgres hostname")             # to match run.py
    parser.add_argument('-x', '--execute', action='count',
                        help="""Don't just print SQL commands, execute them.
                        Asks for confirmation, unless you use -xx""")
    parser.add_argument('-p', '--port', default='5432',
                        help="Postgres TCP port")
    parser.add_argument('-U', '--username', default=os.getlogin(),
                        help="Postgres username")
    parser.add_argument('-v', '--verbose', action='count',
                        help='Enable debug logging')
    parser.add_argument('-w', '--no-password', action='store_true',
                        help="""Never issue a password prompt.  If the server
                        requires a password and one cannot be found in the
                        ~/.pgpass file, connection attempts will fail.""")
    parser.add_argument('dbname', help="Postgres database name")

    global _args
    _args = parser.parse_args(argv[1:])

    if _args.execute == 1:
        if not confirm("This will modify the database, are you sure?"):
            print "Goodbye."
            return 0

    try:
        global _psql
        _psql = Psql(host=_args.host, port=_args.port,
                     database=_args.dbname, user=_args.username,
                     debug=(_args.verbose > 1))  # -vv for extra loud
        if _args.no_password:
            _psql.update({"options": ["--no-password"]})
            return process()
        with PgpassContext(_psql):
            return process()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except KeyboardInterrupt:
        print >>sys.stderr, "^C"
        return 2
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
