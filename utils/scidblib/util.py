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

import datetime
import getpass
import sys
from collections import namedtuple
from scidblib.iquery_client import IQuery


def getVerifiedPassword(prompt=None, verify=None):
    """Read and verify a password from the tty.

    @param prompt the prompt string for initial password entry
    @param verify the prompt string for verification
    """
    if prompt is None:
        prompt = "Password: "
    if verify is None:
        verify = "Re-enter password: "
    while True:
        p1 = getpass.getpass(prompt)
        p2 = getpass.getpass(verify)
        if p1 == p2:
            break
        try:
            with open("/dev/tty", "w") as F:
                print >>F, "Passwords do not match"
        except OSError:
            print >>sys.stderr, "Passwords do not match"
    return p1


def make_table(entry_name, query, namespace=None, host=None, port=None):
    """Build a list of named tuples based on result of the given AFL query.

    @param entry_name name of type to be created by collections.namedtuple
    @param query      AFL query from whose output we will make a table
    @param namespace  use as current namespace when executing query
    @param host       host for iquery connections
    @param port       port for iquery connections

    Because the entire query result is read into memory, best to use
    this only with queries returning smallish results.

    Fields that can be converted to ints, floats, or bools are so converted.

    An example:
    >>> t = make_table('ArrayTable', "list('arrays',true)")
    >>> all_versioned_array_ids = [x.aid for x in t if x.aid != x.uaid]
    """
    # Format tsv+:l gives dimension/attribute names used for tuple attributes.
    iquery = IQuery(afl=True, format='tsv+:l', namespace=namespace,
                    host=host, port=port)
    out, err = iquery(query)
    if err:
        raise RuntimeError(err)
    table_data = out.splitlines()
    # Sometimes SciDB gives the same label to >1 attribute; make them unique.
    attrs = []
    seen = dict()
    for label in table_data[0].split():
        if label in seen:
            seen[label] += 1
            label = '_'.join((label, str(seen[label])))
        else:
            seen[label] = 1
        attrs.append(label)

    # Attempt to convert types to Python equivalents.
    def _convert(x):
        try:
            return int(x)
        except ValueError:
            pass
        try:
            return float(x)
        except ValueError:
            pass
        xx = x.lower().strip()
        if xx == 'true':
            return True
        if xx == 'false':
            return False
        return x

    # Create our data type and fill in the table.
    tuple_type = namedtuple(entry_name, attrs)
    table = []
    for line in table_data[1:]:
        table.append(tuple_type._make(_convert(x) for x in line.split('\t')))
    return table


def timedelta_to_seconds(tdelta):
    """The missing datetime.timedelta.to_seconds() method.

    @return tdelta, in seconds, as a floating point number
    @see http://stackoverflow.com/questions/1083402/\
         missing-datetime-timedelta-to-seconds-float-in-python
    """
    return ((tdelta.days * 3600 * 24) +
            tdelta.seconds +
            (tdelta.microseconds / 1e6))


def seconds_to_timedelta(seconds):
    """Inverse of the missing datetime.timedelta.to_seconds() method.

    @return seconds converted to a datetime.timedelta
    @see http://stackoverflow.com/questions/1083402/\
         missing-datetime-timedelta-to-seconds-float-in-python
    """
    return datetime.timedelta(int(seconds) // (3600 * 24),
                              int(seconds) % (3600 * 24),
                              (seconds - int(seconds)) * 1e6)


class ElapsedTimer(object):
    """Context manager for computing elapsed times of tests."""
    enabled = True

    def __init__(self):
        self.start = None

    def __enter__(self):
        if ElapsedTimer.enabled:
            self.start = datetime.datetime.now()

    def __exit__(self, typ, val, tb):
        if ElapsedTimer.enabled:
            print "Elapsed time:", str(datetime.datetime.now() - self.start)
        return False            # exception (if any) not handled
