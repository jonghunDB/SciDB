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
SDB-5850: Binary save failures should not leak file descriptors.
"""

import argparse
import os
import sys
import traceback

from scidblib.iquery_client import IQuery
from scidblib.ssh_runner import SshRunner

_args = None                    # Parsed arguments
_pgm = None                     # Program name
_iquery = None                  # iquery wrapper


def process():
    """Run the test."""

    # Per-run array and file names.
    aname = "SDB_5850_%s" % _args.run_id
    fname = "/tmp/%s.dat" % aname

    # Setup: create an array with some null values.
    _, err = _iquery(
        "store(build(<v:int64>[i=0:7:0:2], iif(i > 5, null, i)), %s)" % aname)
    assert not err and not _iquery.returncode, "store() failed:\n%s" % err

    try:
        # Find the pid of the query coordinator instance.  Netstat needs
        # the 'sudo' to get the pids.
        ssh = SshRunner(host=_iquery.host)
        out, err = ssh.run(
            "sudo netstat --listening --numeric --program --tcp"
            " | grep ':%s '" % _iquery.port)
        assert ssh.ok(), "Cannot run remote netstat: %s" % err
        fields = out.strip().split()
        # In netstat(8) output, fields[3] will be the local socket
        # address and the final fields[-1] will be slash-separated pid
        # and program name.  See the man page.
        assert fields[3].endswith(":%s" % _iquery.port), (
            "Cannot find instance port in netstat output '%s'" % out)
        pid = int(fields[-1].split('/')[0])
        assert pid, "Uh oh, zero pid"

        # List the open files for this pid.
        before, err = ssh.run("lsof -p %s" % pid)
        assert ssh.ok(), "Cannot run remote lsof: %s" % err

        # Run a binary save() query that is guaranteed to fail.  In the
        # binary template language, the default nullability is
        # non-nullable, so the "(int64)" format is going to fail because
        # of the nulls in the array.
        _, err = _iquery(
            "save(%s, '%s', format:'(int64)')" % (aname, fname))
        assert _iquery.returncode, "save() query succeeded?!"
        assert "SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE" in err, (
            "Unexpected error from save() query:\n%s" % err)

        # List the open files again.  The counts should match!
        after, err = ssh.run("lsof -p %s" % pid)
        assert ssh.ok(), "Cannot run remote lsof: %s" % err
        try:
            assert len(before.splitlines()) == len(after.splitlines()), (
                "Leaked a file descriptor: %d files were open, now %d" % (
                    len(before.splitlines()), len(after.splitlines())))
        except AssertionError:
            b = set(before.splitlines())
            a = set(after.splitlines())
            if len(b) < len(a):
                print >>sys.stderr, "Additions:", a - b
            else:
                print >>sys.stderr, "Deletions:", b - a
            raise

    finally:
        # Cleanup.
        _, err = _iquery("remove(%s)" % aname)
        assert not err, "Cannot remove array %s:\n%s" % (aname, err)
        os.system("rm -f %s" % fname)


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by prt()

    parser = argparse.ArgumentParser(
        description="Prove that save() failures don't leak file descriptors.")
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')

    global _args
    _args = parser.parse_args(argv[1:])

    global _iquery
    _iquery = IQuery(afl=True, format='tsv')
    _iquery.setenv(_args)

    try:
        process()
        return 0
    except AssertionError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
