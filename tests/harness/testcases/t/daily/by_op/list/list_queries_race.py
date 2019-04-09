#!/usr/bin/python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2018-2018 SciDB, Inc.
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
Test for SDB-6125.

One process repeatedly issues queries, and another repeatedly runs
list('queries').  Eventually the second process should see entries for
user_id=0, user='nobody'.  That proves that we've tickled the race
condition (Query::_session not set) without causing a segmentation
fault.
"""

import argparse
import errno
import multiprocessing as mp
import os
import sys
import time
import traceback

from scidblib.iquery_client import IQuery

_args = None                    # Parsed arguments
_pgm = None                     # Program name
RUN_FILE = '/tmp/lqr_run_'      # Non-list process quits when this is removed


def query_runner():
    """Run a query repeatedly until the RUN_FILE is removed."""
    iquery = IQuery(afl=True, format='tsv',
                    host=_args.host, port=_args.port)
    afl = """save(filter(IMAGE_CUBE_{0},
                         study_id>=0 and study_id<=0 and
                         x>=0 and x<=127 and
                         y>=0 and y<=127 and
                         z>=2 and z<=2),
                  '/dev/null')""".format(_args.run_id)
    afl = ' '.join(afl.split())  # Prettify for nicer list('queries') output.
    loops = 0
    while True:
        loops += 1
        iquery(afl)
        try:
            os.stat(RUN_FILE)
        except OSError as e:
            if e.errno == errno.ENOENT:
                print "File", RUN_FILE, "gone after", loops, "save() queries"
                return
            else:
                print >>sys.stderr, "Cannot stat {0}: {1}".format(RUN_FILE, e)
                sys.exit(1)


def list_runner():
    """Run list('queries') repeatedly until user 'nobody' (user_id==0) is
       seen, or until our patience runs out.
    """
    iquery = IQuery(afl=True, format='tsv',
                    host=_args.host, port=_args.port)
    try:
        for i in xrange(_args.max_trials):
            out, err = iquery(
                "project(list('queries'), user_id, user, query_string)")
            if err or iquery.returncode:
                # Cannot raise from subprocess, sadly.
                print >>sys.stderr, "List runner query failed:", err
                sys.exit(1)
            if any(x.startswith('0\t') for x in out.splitlines()):
                print "Success, user_id==0 seen after", i+1, "trials!"
                print "Out:\n", out
                return
            time.sleep(0.5)

        # This isn't necessarily a bad thing.  The mere fact that no
        # instances crashed is a win.  Perhaps recent code changes
        # guarantee that Query objects in the Query::_queries maps now
        # *always* have a Session?  Maybe we should not exit here?
        print >>sys.stderr, """
            Exceeded --max-trials={0} .  Does the SDB-6125 race still exist?
            """.strip().format(_args.max_trials)
        sys.exit(1)

    finally:
        try:
            os.unlink(RUN_FILE)
        except:
            pass


def process():
    """Create test array, start list and query runners, wait for done."""
    # Create test array.
    iquery = IQuery(afl=True, no_fetch=True,
                    host=_args.host, port=_args.port)
    _ = iquery("remove(IMAGE_CUBE_%s" % _args.run_id)
    _, err = iquery("""
        store(build(<intensity:float>[study_id=0:30:0:1;
                                      x=0:128:0:64;
                                      y=0:128:0:64;
                                      z=0:50:0:10],
                    x*y),
              IMAGE_CUBE_{0})""".format(_args.run_id))
    assert not err and not iquery.returncode, (
        "Cannot create IMAGE_CUBE_{0}: {1}".format(_args.run_id, err))
    print "Created IMAGE_CUBE_%s" % _args.run_id

    # Spawn subprocesses.
    qproc = mp.Process(target=query_runner, args=())
    lproc = mp.Process(target=list_runner, args=())
    qproc.start()
    time.sleep(0.5)             # Give query_runner a head start.
    lproc.start()
    lproc.join()
    qproc.join()

    # Cleanup and return list runner's exit status.
    iquery("remove(IMAGE_CUBE_%s)" % _args.run_id)
    return lproc.exitcode


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="Test for SDB-6125 list('queries') race")
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')
    parser.add_argument('--max-trials', default=50,
                        help="Quit after this many list('queries') attempts")
    parser.add_argument('-v', '--verbose', default=0, action='count',
                        help='Debug logging level, 1=info, 2=debug, 3=debug+')

    global _args
    _args = parser.parse_args(argv[1:])

    global RUN_FILE
    RUN_FILE = RUN_FILE + str(_args.run_id)
    os.system("touch %s" % RUN_FILE)

    try:
        return process()
    except AssertionError as e:
        print >>sys.stderr, "Test failed: %s" % e
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
