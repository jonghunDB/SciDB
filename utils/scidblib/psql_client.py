#!/usr/bin/python

"""
A wrapper for the Postgres psql client.

For now this is a very minimal wrapper, supporting only the
functionality needed during SciDB installation---particularly
"scidb.py init-syscat ...".

Enquiring minds want to know: "Why not use psycopg?"  Just because we
try to avoid adding dependencies on modules that are not part of the
base Python distribution on all supported platforms.

Psql objects have the following attributes:

    database - connect to this database, same as '-d database'
    debug - enable debug logging for this Psql object
    host - host where Postgres cluster resides
    port - port where Postgres cluster resides
    prog - path to psql program binary, default "psql"
    user - database role for connecting etc.
    options - additional psql program options

Examples:

    >>> from scidblib.psql_client import Psql
    >>> psql = Psql(port=5432, user='postgres')
    >>> result = psql("select usename, usesysid from pg_catalog.pg_user")
    >>> result
    [psql_1(usename='postgres', usesysid='10'),
     psql_1(usename='fry', usesysid='23898'),
     psql_1(usename='leela', usesysid='24534'),
     psql_1(usename='nibbler', usesysid='55259'),
     psql_1(usename='calculon', usesysid='198073'),
     psql_1(usename='lrrr', usesysid='211992'),
     psql_1(usename='kif', usesysid='325500'),
     psql_1(usename='bender', usesysid='377586')]
    >>> result[2].usename
    'leela'
    >>> if psql("select 1 from pg_catalog.pg_user where usename = 'fry'"):
    ...     print "Fried."
    ... else:
    ...     print "No pizza today."
    Fried.
"""

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

import re
import subprocess as subp
import sys

from collections import namedtuple
from scidblib import AppError


class Psql(object):

    """Postgres psql wrapper."""

    def _dbg(self, *args):
        """Print debugging information."""
        if self.debug:
            print "DBG:", ' '.join((str(x) for x in args))

    def __init__(self, config=None, **kwargs):
        """Initialize attributes from config dict and keywords."""
        self.database = None
        self.debug = False
        self.host = None
        self.port = None
        self.prog = 'psql'
        self.user = None
        self.options = []
        # Override the defaults if we were given a dict.
        if kwargs or (config is not None):
            self.update(config, **kwargs)

    def update(self, params=None, **kwargs):
        """Update attributes from params dict/iterable.

        @param params dict/iterable with parameter name/value pairs
        @param kwargs keyword name/value pairs
        @returns None

        @description
        You can always set individual attributes directly, but if you
        want to set several all at once, this is the way.  This is
        intended to work just like dict.update().

        @note Overkill, but we already had the code from
              iquery_wrapper, so why not.
        """
        # Uses object.__setattr__() rather than setattr(), otherwise
        # IPython has inspect difficulties for some reason.
        if params is not None:
            try:
                # Maybe it's a dict?
                for k in params:
                    object.__setattr__(self, k, params[k])
            except AttributeError:
                # Not a dict (no keys() attribute), try iterable-of-pairs.
                for k, v in params:
                    object.__setattr__(self, k, v)
        for k in kwargs:
            object.__setattr__(self, k, kwargs[k])
        return None

    @staticmethod
    def _sanitize(label):
        """Transform label into a form not hated by namedtuple.

        For example, a "select 1 from ... where ..." query will
        produce a synthetic column name "?column?", but namedtuple
        hates that.
        """
        # Replace illegal characters
        label = re.sub(r'[^a-zA-Z0-9_]', '_', label)
        # Cannot begin with digits or underscores
        label = re.sub(r'^[\d_]+', '', label)
        assert label, "Utterly bogus column label"
        return label

    @staticmethod
    def _make_labels(labels):
        """Sanitize and uniquify a line of column labels.

        Some queries can yield duplicate label names, which namedtuple
        hates.  Make sure all labels are sanitized *and* unique.
        """
        result = []
        for label in map(Psql._sanitize, labels.split('|')):
            while label in result:
                label += '_'
            result.append(label)
        return result

    def _make_table(self, out, _counter=[]):
        """Construct a list of tuples from SQL output.

        Brashly assumes that whatever SQL statement was executed will
        return some table-like data in --no-align format.
        """
        # Each call generates a unique tuple type, _counter[0] uniquifies it.
        if not _counter:
            _counter.append(0)
        _counter[0] += 1
        # Try to build a list of tuples; throw if that's not possible
        lines = out.splitlines()
        if lines[0] == "List of relations":  # \dt and \ds spew this, skip it
            del lines[0]
        if len(lines) < 2:      # Expect at least header and footer lines
            raise RuntimeError("Not enough lines to make a table")
        labels = Psql._make_labels(lines[0])
        tuple_type = namedtuple("psql_%d" % _counter[0], labels)
        table = []
        for row in lines[1:-1]:
            table.append(tuple_type._make(row.split('|')))
        return table

    def __call__(self, sql_stmt):
        """Invoke psql to run an SQL statement."""
        # Build command line.
        cmd = [self.prog, '--no-align', '--quiet', '--no-psqlrc']
        if self.port:
            cmd.extend(['-p', str(self.port)])
        if self.host:
            cmd.extend(['-h', self.host])
        if self.user:
            cmd.extend(['-U', self.user])
        if self.database:
            cmd.extend(['-d', self.database])
        if self.options:
            cmd.extend(self.options)
        # Run it!
        self._dbg("Cmd:", ' '.join(cmd))
        p = subp.Popen(
            cmd, stdin=subp.PIPE, stdout=subp.PIPE, stderr=subp.PIPE)
        out, err = p.communicate(sql_stmt)
        # Sometimes psql fails but gives exit status 0, mumble....
        if p.returncode or 'ERROR:' in err:
            raise AppError('\n'.join(('Psql: "{0}" failed:'.format(sql_stmt),
                                      "Cmd: {0}".format(' '.join(cmd)),
                                      err)))
        try:
            tbl = self._make_table(out)
        except Exception as e:
            # Couldn't make a table from the output, so just return the output.
            self._dbg("Psql._make_table failed:", e)
            return out
        else:
            return tbl


def main(argv=None):
    if argv is None:
        argv = sys.argv
    # A small test driver.  There's nothing here you can't do in
    # ipython, but it was useful at the time.
    print "Run 'pydoc psql_client.py' for fun and profit."
    psql = Psql(debug=True)
    while True:
        ans = raw_input("Psql> ")
        words = ans.strip().split()
        if not words:
            continue
        if words[0] == 'set':
            if len(words) == 1:
                print psql.__dict__
            elif len(words) != 3:
                print >>sys.stderr, "Bad 'set' command"
                continue
            else:
                setattr(psql, words[1], words[2])
        elif words[0] in ('quit', 'exit'):
            break
        else:
            result = None
            try:
                result = psql(ans)
            except AppError as e:
                print >>sys.stderr, e
            if isinstance(result, basestring):
                print result
            else:
                for x in result:
                    print x
    return 0


if __name__ == '__main__':
    sys.exit(main())
