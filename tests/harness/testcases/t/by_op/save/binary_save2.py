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

"""
Ticket 4703.  Testing the binary template's "skip" directive (aka
"dummy", aka "void") which should really have been called "pad", but
oh well.

SUMMARY

We wish to prove

 1. that "skip" correctly ignores binary fields on input,
 2. that the same template correctly inserts padding on output, and
 3. that the resulting padded output can also be loaded with the same template.

Then we know 'skip' is indeed symmetric for save() and load(), and
behaves as expected to skip over the built-ins on input.

DETAILS

First we make a reference array

    <left:int64, right:string>[]

whose fixed-value attributes will bracket the data we choose to skip.
Then for each data type 'T' (and 'T null') in TEST_CASES, we'll create
an array

    <left:int64, center:T, right:string>[]

and save it as T_center.dat.

Then we'll create a template using 'skip' that should ignore the
center attribute on input.  Reloading T_center.dat with this template
should match the reference array.

We'll use the same template to save the reference array into
T_pad.dat, and then reload that.  It too should match the reference
array.
"""

import argparse
import os
import struct
import sys
from subprocess import STDOUT

from t_other_utils import ok
from scidblib.iquery_client import IQuery

_args = None
_files = []
_iquery = IQuery(afl=True, stderr=STDOUT)

TEST_CASES = (
    # type,   len,     example value
    ('int8',    1,     -100),
    ('int16',   2,     65000),
    ('int32',   4,     131072),
    ('int64',   8,     -4),
    ('uint8',   1,     126),
    ('uint16',  2,     65530),
    ('uint32',  4,     0xDEADBEEF),
    ('uint64',  8,     4300000000),
    ('float',   4,     6.02e23),
    ('double',  8,     3.14159265358979),
    ('char',    1,     "strchar('Q')"),
    ('bool',    1,     "true"),
    ('string',  0,     "'Crikey!'"),
    ('binary',  0,     "/\x01\x00embedded NUL horror/")
    )

DIM_LO = 0
DIM_HI = 15
DIM = "[i=%d:%d,4,0]" % (DIM_LO, DIM_HI)
SCHEMA = "<left:int64, right:string>%s" % DIM
REF = "bsave2_ref"
TARGET = "bsave2_target"

def make_ref_array():
    ok(_iquery('create temp array %s %s' % (REF, SCHEMA))[0])
    ok(_iquery('-n', "store(apply(build(<left:int64>%s, i+1), right, 'eor'), %s)" % (DIM, REF))[0])

def check_vs_ref_array(ary):
    """Run queries whose output should always be the same if ary
    matches the reference array.  (Test harness will detect any
    difference.)
    """
    ok(_iquery("filter(%s, left<>i+1 or right <> 'eor')" % ary)[0])
    ok(_iquery("aggregate(%s, count(*))" % ary)[0])

def run_one_test(typ, fixedLen, value):
    """Run a single test."""

    # Create the template to be tested.
    nullable = 'null' in typ
    if fixedLen:
        skip = 'skip(%d)%s' % (fixedLen, ' null' if  nullable else '')
    else:
        skip = 'skip%s' % (' null' if nullable else '')
    template = '(int64,%s,string)' % skip

    # People want to know: which test case is this?
    tc_name = 'tc_%s' % typ.replace(' ', '_')
    print "---- Test case:", tc_name, template

    # Write the T_center.dat file.
    global _files
    _files.append('/tmp/{0}_center_{1}.dat'.format(tc_name, _args.run_id))
    if typ.startswith('binary'):
        # Python runtime hates exec'ing the embedded NUL horror, so we
        # need to craft the .dat file by hand.
        eor = ''.join((struct.pack("<I", len('eor\x00')), 'eor\x00'))
        with open(_files[-1], 'wb') as W:
            for i in xrange(DIM_LO, DIM_HI+1):
                if nullable:
                    W.write(struct.pack("<qbI", i+1, -1, len(value)))
                else:
                    W.write(struct.pack("<qI", i+1, len(value)))
                W.write(value)
                W.write(eor)
    else:
        query = """save(project(apply({0}, center, {1}), left, center, right),
                   '{2}', -2, '(int64,{3},string)')""".format(
            REF, str(value), _files[-1], typ)
        ok(_iquery('-n', query)[0])

    # Recreate target array.
    _iquery('-n',  'remove(%s)' % TARGET) # may fail, no ok() wrapper
    ok(_iquery('create temp array %s %s' % (TARGET, SCHEMA))[0])

    # Load target from T_center.dat and verify == to reference array.
    ok(_iquery('-n', "load({0}, '{1}', -2, '{2}')".format(
                TARGET, _files[-1], template))[0])
    check_vs_ref_array(TARGET)

    # Save using same template.
    _files.append('/tmp/{0}_pad_{1}.dat'.format(tc_name, _args.run_id))
    ok(_iquery('-n', "save({0}, '{1}', -2, '{2}')".format(
                TARGET, _files[-1], template))[0])

    # Recreate target array.
    _iquery('-n', 'remove(%s)' % TARGET)
    ok(_iquery('create temp array %s %s' % (TARGET, SCHEMA))[0])

    # Load target from T_pad.dat and verify == to reference array.
    ok(_iquery('-n', "load({0}, '{1}', -2, '{2}')".format(
                TARGET, _files[-1], template))[0])
    check_vs_ref_array(TARGET)

    return None                 # Done.


def run_tests():
    """Run each test in TEST_CASES."""
    cleanup()                   # after prior run(s)
    make_ref_array()
    for typ, fixedLen, value in TEST_CASES:
        run_one_test(typ, fixedLen, value)
        run_one_test('%s null' % typ, fixedLen, value)
    return 0

def cleanup():
    """Remove files and arrays we created elsewhere."""
    for f in _files:
        try:
            os.unlink(f)
        except:
            pass
    for a in (REF, TARGET):
        _iquery('remove(%s)' % a)

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _args
    parser = argparse.ArgumentParser(
        description='The binary_save2 test script.')
    parser.add_argument('-c', '--host', default='localhost',
                        help="The SciDB host address.")
    parser.add_argument('-p', '--port', type=int, default=1239,
                        help="The TCP port for connecting to SciDB.")
    parser.add_argument('-r', '--run-id', default="", help="""
        Uniquifier (such as $HPID) to use in naming files etc.""")
    _args = parser.parse_args(argv[1:])

    global _iquery
    _iquery.host = _args.host
    _iquery.port = _args.port

    ret = run_tests()
    cleanup()
    return ret

if __name__ == '__main__':
    sys.exit(main())
