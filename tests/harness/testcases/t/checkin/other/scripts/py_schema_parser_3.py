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
Unit tests for scidblib.scidb_schema module using new dimension syntax.

This is a direct translation of py_schema_parser_2.py into the new
dimension syntax, plus additional subtests.
"""

import argparse
import os
import re
import sys
import traceback

import scidblib.scidb_schema as SS
from simple_test_runner import SimpleTestRunner

_args = None                    # Globally visible parsed arguments
_pgm = None                     # Globally visible program name


def reparse(attrs, dims):
    """Rebuild a schema from attrs and dims... it should match!"""
    # Don't use old-style dimension syntax for this "mirroring".  The
    # old syntax won't "mirror" None values (they must become '*').
    schema = SS.unparse(attrs, dims, compat=False)
    aa, dd = SS.parse(schema)
    for x, y in zip(attrs, aa):
        assert x == y, "Reparse attribute mismatch: '%s' != '%s'" % (x, y)
    for x, y in zip(dims, dd):
        assert x == y, "Reparse dimension mismatch: '%s' != '%s'" % (x, y)


class TheTest(SimpleTestRunner):

    def setUpClass(self):
        """Create some test data files used by all test methods."""
        pass

    def tearDownClass(self):
        """Undo setup actions."""
        pass

    def test_00_missing_attrs(self):
        """Throw on missing attributes"""
        s = "junk < \n\v\r\t  >  [i=0:43:5;  j_ = 1 :  50 : 1 : 10] more junk"
        threw = False
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            threw = True
        assert threw, "No attributes, did not throw: %s" % s

        s = re.sub("<\s*>", "<x:int64>", s)
        threw = ''
        try:
            aa, dd = SS.parse(s)
        except ValueError as e:
            threw = str(e)
        assert not threw, "Got attributes, threw: %s (%s)" % (s, threw)
        assert len(aa) == 1, "Expected one attribute, got %d" % len(aa)
        assert len(dd) == 2, "Expected two dimensions, got %d" % len(dd)
        reparse(aa, dd)

    def test_01_missing_dims(self):
        """Throw on missing dimensions"""
        s = "< a:int64, b:string default foo(bar+1)  > [ \t\v\n\r ]"
        aa = dd = None
        threw = False
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            threw = True
        assert threw, "No dimensions, did not throw: %s" % s

        s = re.sub(r"\[\s*\]", "[i=0:0,1,0]", s)
        threw = ''
        try:
            aa, dd = SS.parse(s)
        except ValueError as e:
            threw = str(e)
        assert not threw, "No dimensions, threw: %s (%s)" % (s, threw)
        assert len(aa) == 2, "Expected two attributes, got %d" % len(aa)
        assert len(dd) == 1, "Expected one dimension, got %d" % len(dd)
        reparse(aa, dd)

    def test_02_missing_everything(self):
        """Throw on missing attributes or dimensions"""
        s = "< \t\v\r\n  > [ \t\v\r\n ]"
        threw = False
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            threw = True
        assert threw, "No variables, did not throw: %s" % s

    def test_03_quotes_in_strings(self):
        """Escaped quote marks OK in string literals"""
        s = r"""<x:string default 'Ain\'t it grand?' compression 'D\'oh!!!'>
                [i=0:0:0:1]"""
        aa, dd = SS.parse(s)
        # The DEFAULT string is preserved, quotes and all.  Parsing
        # the DEFAULT clause is tricky, so we don't actually do
        # it---instead we are just careful not to disturb it.
        assert aa[0].default == r"'Ain\'t it grand?'"
        # The COMPRESSION string is *not* preserved as-is, its
        # enclosing quotes are stripped (and any escaped quote is left
        # escaped, which is wrong... when we have a compression method
        # named 'Mike\'s sick method' we'll fix that).  This is
        # probably the behavior you want.
        assert aa[0].compression == r"D\'oh!!!"

    def test_04_semi_colon_dimension_sep(self):
        """Semi-colons can separate dimension groups"""
        s = "<x:int64>[i=0:0:0:1; j=0:99:2:10]"
        aa, dd = SS.parse(s)
        assert len(dd) == 2, "Expected two dimensions, got %d" % len(dd)
        reparse(aa, dd)

    def test_05_whitespace(self):
        """Newlines and other whitespace"""
        # This schema has whitespace in every legal location.
        s = r"""<
            a : int64 not null default strlen ( 'I\'m whelmed' ) ,
            b : string default 'I\'m the default, baby!' compression 'default',
            c : binary reserve 42 > [ i = - 90 : 90 : 0: 10 ;
                                      j = - 180 : 180: 2 : 20 ]
            """
        aa, dd = SS.parse("\n\t\v\r ".join(s.split()))
        assert len(aa) == 3, "Expected 3 attributes, got %d" % len(aa)
        assert len(dd) == 2, "Expected 2 dimensions, got %d" % len(dd)
        assert aa[1].name == 'b', "b name: %s" % aa[1].name
        assert aa[1].compression == r"default", (
            "b compression: %s" % aa[1].compression)
        assert aa[2].reserve == 42, "c.reserve: %s" % aa[2].reserve

    def test_06_lone_identifier(self):
        """Lone dimension name x means x=0:*:0:*"""
        s = "<x:int64>[i]"
        aa, dd = SS.parse(s)
        assert len(aa) == 1, "Expected one attribute, got %d" % len(aa)
        assert len(dd) == 1, "Expected one dimension, got %d" % len(dd)
        reparse(aa, dd)
        d = dd[0]
        assert d.lo == 0, "Default low bound should be zero, got %s" % dd.lo
        assert d.hi == '*', (
            "Default high bound should be '*', got %s" % dd.hi)
        assert d.overlap == 0, (
            "Default overlap should be zero, got %s" % dd.overlap)
        assert d.chunk == '*', (
            "Default interval should be *, got %s" % dd.chunk)

    def test_07_default_overlap_and_interval(self):
        """Omit interval, or interval and overlap, and get None"""
        s = "<\nx:int64\n>\n[i=-43:+70; j=\v+\t100:+200:5]"
        aa, dd = SS.parse(s)
        assert len(aa) == 1, "Expected one attribute, got %d" % len(aa)
        assert len(dd) == 2, "Expected two dimensions, got %d" % len(dd)
        reparse(aa, dd)
        assert dd[0].name == 'i', "Dim 0 name got borked"
        assert dd[0].overlap is None, (
            "Dim 0 omitted overlap became %s" % dd[0].overlap)
        assert dd[0].chunk is None, (
            "Dim 0 omitted chunk became %s" % dd[0].chunk)
        assert dd[1].name == 'j', "Dim 1 name got borked"
        assert dd[1].overlap == 5, (
            "Dim 1 overlap got borked to %s" % dd[1].overlap)
        assert dd[1].chunk is None, (
            "Dim 1 omitted chunk became %s" % dd[1].chunk)

    def test_08_missing_dim_name(self):
        """Missing dimension name is caught"""
        s = "<x:int64> [=-43:+70]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            pass
        else:
            assert False, "Missing dimension name should have thrown"

    def test_09_missing_low_bound(self):
        """Missing dimension low bound is caught"""
        s = "<x:int64> [i=:+70]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            pass
        else:
            assert False, "Missing dimension low bound should have thrown"

    def test_09_missing_high_bound(self):
        """Missing dimension high bound is caught"""
        s = "<x:int64> [i=+70]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            pass
        else:
            assert False, "Missing dimension high bound should have thrown"

    def test_10_extra_semi(self):
        """Semi-colon is separator not terminator"""
        s = "<x:int64> [i=+70:100;]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            pass
        else:
            assert False, "Extra dimension separator should have thrown"

    def test_11_expression_evaluation(self):
        """Expression evaluation is not supported"""
        # The following is *completely legal* new-style syntax!!!
        s = "<x:int64> [i=0:sizeof(',,,,,,,,,,')]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            # ...but it fails anyway, because SS.parse() can't evalute
            # expressions like sizeof(), it only casts strings to longs.
            pass
        else:
            assert False, "Kudos to whomever implemented sizeof() evaluation!"

    def test_12_id_equals_lo_semi_hi(self):
        """Semi-colon cannot separate low and high bound"""
        s = "<x:int64> [i=0;0:0:1; j=0:99:2:10]"
        try:
            aa, dd = SS.parse(s)
        except ValueError:
            pass
        else:
            assert False, (
                "Semi-colon between hi and lo bound should have thrown")


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])

    parser = argparse.ArgumentParser(
        description="This skeleton program does very little.",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')

    global _args
    _args = parser.parse_args(argv[1:])

    try:
        tt = TheTest()
        return tt.run()
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
