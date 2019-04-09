#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2018 SciDB, Inc.
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

"""Some basic functional tests for autochunking redimension().

(Also, test various ways to use the iquery_client.IQuery wrapper.)
"""

import argparse
import os
import sys
import traceback

from scidblib import AppError
from scidblib.util import make_table
from scidblib.iquery_client import IQuery
from box_of_points import main as boxofpoints
from collections import defaultdict
from simple_test_runner import SimpleTestRunner


_args = None                    # Globally visible parsed arguments
_pgm = None                     # Globally visible program name
_tmpdir = None                  # Dir for generated data files


class TheTest(SimpleTestRunner):

    def setUpClass(self):
        """Create some test data files used by all test methods."""
        print "Setup ...",
        sys.stdout.flush()
        self._iquery = IQuery(afl=True, no_fetch=True)  # -naq
        self._array_cleanups = []
        self._files = {}        # map array name to input file
        # Put all our data files in one temp directory so we can
        # easily remove them all during tearDown.
        if os.system("rm -rf {0} ; mkdir -p {0}".format(_tmpdir)):
            raise AppError("Trouble (re)creating %s" % _tmpdir)

        # Create slightly sparse 3-D input data with no collisions.
        self._files['nocoll_3d'] = os.path.join(_tmpdir, "nocoll_3d.bin")
        if boxofpoints(['boxofpoints',
                        '--lower-corner', '0,0,0',
                        '--upper-corner', '9,69,69',
                        '--cells', '40000',  # sparse: 40000 < 10x70x70 (49000)
                        '--format', 'binary',
                        '--output', self._files['nocoll_3d'],
                        '--seed', '42']):
            raise AppError("box_of_points could not create %s" %
                           self._files['nocoll_3d'])

        # Create dense 2-D input data with 10% collisions.
        self._files['coll_2d'] = os.path.join(_tmpdir, "coll_2d.bin")
        if boxofpoints(['boxofpoints',
                        '--lower-corner', '0,0',
                        '--upper-corner', '49,999',
                        '--cells', '50000',  # dense: 50,000 == 50x1000
                        '--collisions', '0.1',  # 10% collision rate
                        '--format', 'binary',
                        '--output', self._files['coll_2d'],
                        '--seed', '42']):
            raise AppError("box_of_points could not create %s" %
                           self._files['coll_2d'])
        print "done"

    def tearDownClass(self):
        print "Teardown ...",
        sys.stdout.flush()
        if not _args.keep_arrays:
            if os.system("rm -rf {0}".format(_tmpdir)):
                raise AppError("Trouble cleaning up %s" % _tmpdir)
            for a in self._array_cleanups:
                self._iquery("remove(%s)" % a)
        print "done"

    def test_00_load_3d_ac(self):
        """Load 3-D no-collision data using autochunking"""
        dims = "x,y,z"          # Autochunked!
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'], dims, "nocoll_3d_ac")
        _, err = self._iquery(query)
        assert not err, err
        self._array_cleanups.append("nocoll_3d_ac")

    def test_01_load_3d_concrete(self):
        """Load 3-D no-collisions data with specified chunks"""
        # older dim syntax: backward compat
        dims = "x=0:*,10,0,y=0:*,100,0,z=0:*,100,0"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'], dims, "nocoll_3d")
        _, err = self._iquery(query)
        assert not err, err
        self._array_cleanups.append("nocoll_3d")

    def test_02_nocoll_3d_counts_and_sums(self):
        """Compare 3-D array counts and sums"""
        self._iquery.update({'format': 'tsv', 'no_fetch': False})
        out1, err1 = self._iquery('aggregate(nocoll_3d_ac,count(*),sum(v))')
        assert not err1, err1
        out2, err2 = self._iquery('aggregate(nocoll_3d,count(*),sum(v))')
        assert not err2, err2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_03_nocoll_3d_check_values(self):
        """Cell-by-cell value comparison for 3-D arrays"""
        self._iquery.update({'format': 'tsv+', 'no_fetch': False})
        out, err = self._iquery("""filter(join(nocoll_3d,nocoll_3d_ac),
                                          nocoll_3d.v <> nocoll_3d_ac.v)""")
        assert not err, err
        assert out == '', "Cell values differ:\n\t{0}".format(out)

    def test_04_load_2d_ac_w_collisions(self):
        """Load 2-D data containing collisions using autochunking"""
        dims = "x=0:*; y=0:*; synth=0:*"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['coll_2d'], dims, "coll_2d_ac")
        self._iquery.no_fetch = True
        _, err = self._iquery(query)
        assert not err, err
        self._array_cleanups.append("coll_2d_ac")

    def test_05_load_2d_concrete_w_collisions(self):
        """Load 2-D data containing collisions with specified chunks"""
        dims = "x=0:*:0:100; y=0:*:0:100; synth=0:9:0:10"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['coll_2d'], dims, "coll_2d")
        self._iquery.no_fetch = True
        _, err = self._iquery(query)
        assert not err, err
        self._array_cleanups.append("coll_2d")

    def test_06_coll_2d_counts_and_sums(self):
        """Compare 2-D array counts and sums"""
        self._iquery.update((('format', 'tsv'),
                             ('no_fetch', False)))
        out1, err1 = self._iquery('aggregate(coll_2d_ac,count(*),sum(v))')
        assert not err1, err1
        out2, err2 = self._iquery('aggregate(coll_2d,count(*),sum(v))')
        assert not err2, err2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_07_coll_2d_check_values(self):
        """Cell-by-cell value comparison for 2-D arrays

        This test is complicated by the fact that with different chunk
        intervals, redimension() does not produce synthetic dimension
        siblings in any particular order.  So we must process the
        filtered list of differing cells, and only complain if the
        *set* of values along the synthetic dimension at [x,y,*]
        differs for the two arrays.  For example, if the two arrays
        held

            {x,y,synth} v           {x,y,synth} v
            {2,7,0} 20              {2,7,0} 20
            {2,7,1} 73              {2,7,1} 99
            {2,7,2} 99              {2,7,2} 73

        that is perfectly fine.
        """
        tbl = make_table('CellDiffs', """
            filter(join(coll_2d,coll_2d_ac), coll_2d.v <> coll_2d_ac.v)
            """)
        v_xy_sets = defaultdict(set)
        v2_xy_sets = defaultdict(set)
        for celldiff in tbl:
            key = (int(celldiff.x), int(celldiff.y))
            v_xy_sets[key].add(int(celldiff.v))
            v2_xy_sets[key].add(int(celldiff.v_2))
        assert len(v_xy_sets) == len(v2_xy_sets)
        for xy in v_xy_sets:
            assert v_xy_sets[xy] == v2_xy_sets[xy], \
                "Synthetic dimension trouble at {0}".format(xy)

    def test_08_load_3d_ac_w_overlap(self):
        """Load 3-D no-collision data using autochunking and overlaps."""
        dims = "x=0:*:2; y=0:*:3; z=0:*"  # Autochunked!
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'],
                              dims, "nocoll_3d_ac_ol")
        _, err = self._iquery('-naq', query)
        assert not err, err
        self._array_cleanups.append("nocoll_3d_ac_ol")

    def test_09_nocoll_3d_overlap_counts_and_sums(self):
        """Compare 3-D array counts and sums (overlap)"""
        out1, err1 = self._iquery('-otsv', '-aq',
                                  'aggregate(nocoll_3d_ac_ol,count(*),sum(v))')
        assert not err1, err1
        out2, err2 = self._iquery('-otsv', '-aq',
                                  'aggregate(nocoll_3d,count(*),sum(v))')
        assert not err2, err2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_10_nocoll_3d_overlap_check_values(self):
        """Cell-by-cell value comparison for 3-D arrays (overlap)"""
        self._iquery.update({'format': 'tsv+', 'quiet': False})
        out, err = self._iquery("""
            filter(join(nocoll_3d, nocoll_3d_ac_ol),
                        nocoll_3d.v <> nocoll_3d_ac_ol.v)
            """)
        assert not err, err
        assert out == '', "Cell values differ:\n\t{0}".format(out)

    def test_11_empty_input(self):
        """Autochunked redimension of empty array should not fail (SDB-5109)"""
        out, err = self._iquery(
            'create temp array empty<val:double>[k=0:39:4:20]',
            quiet=False)
        assert not err, err
        self._array_cleanups.append("empty")
        out, err = self._iquery('redimension(empty, <val:double>[k=0:39:3])',
                                format='tsv+', no_fetch=False)
        assert not err, err
        assert not out, "Redim of empty array is not empty: '%s'" % out

    def test_12_one_input_cell(self):
        """Autochunked redimension of 1-cell array should not fail"""
        self._iquery.update({'no_fetch': False})
        _, err = self._iquery('create temp array ONE<val:double>[k=0:39:4:20]')
        assert not err, err
        self._array_cleanups.append("ONE")
        # Insert one cell at k == 25.
        iquery = IQuery(afl=True, format='tsv+:l', no_fetch=True)
        _, err = iquery("""
            insert(
              redimension(
                apply(build(<val:double>[i=0:0,1,0], 3.14), k, 25),
                ONE),
            ONE)""")
        assert not err, err
        iquery.update({'format': 'tsv+', 'afl': True, 'no_fetch': False})
        out, err = iquery('redimension(ONE, <val:double>[k=0:39:3])')
        assert not err, err
        try:
            numbers = map(float, out.split())
        except ValueError:
            assert False, "Unexpected non-number in '%s'" % out
        assert len(numbers) == 2
        assert numbers[0] == 25
        assert numbers[1] == 3.14


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    parser = argparse.ArgumentParser(description="The redim_autochunk_2 test.")
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')
    parser.add_argument('-k', '--keep-arrays', action='store_true',
                        help='Do not remove test arrays during cleanup.')
    parser.add_argument('-v', '--verbosity', default=0, action='count',
                        help='Increase debug logs. 1=info, 2=debug, 3=debug+')

    global _args
    _args = parser.parse_args(argv[1:])
    IQuery.setenv(_args)

    global _tmpdir
    _tmpdir = "/tmp/redim_autochunk_2.{0}".format(_args.run_id)

    try:
        tt = TheTest()
        return tt.run()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
