#!/usr/bin/python
#
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2018 SciDB, Inc.
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
"""This script removes all SciDB arrays satisfying given conditions.

@author Donghui Zhang
@modified Marty Corbett

Assumptions:
  - SciDB is running.
  - 'iquery' is in your path.
"""


import argparse
import sys
import re
import tempfile
import textwrap
import traceback
import scidblib
from scidblib import scidb_afl
from scidblib import scidb_psf
import StringIO

def create_auth_file(username, password):
    """Set up a temporary auth_file based on auth=(user, password)."""
    _auth_tempfile = StringIO.StringIO()
    print >>_auth_tempfile, textwrap.dedent("""
        [security_password]
        user-name={0}
        user-password={1}""".format(username, password).lstrip('\n'))
    return _auth_tempfile

        
def main():
    """The main function gets command-line argument as a pattern, and removes all arrays with that
    pattern.
    
    Note:  Empty namespaces will NOT be removed.
    """
    parser = argparse.ArgumentParser(
                                     description='Remove all SciDB arrays whose names match a given pattern.',
                                     epilog=
                                     'assumptions:\n' +
                                     '  - iquery is in your path.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-f', '--force', action='store_true',
                        help='Forced removal of the arrays without asking for confirmation.')
    parser.add_argument('-c', '--host',
                        help='Host name to be passed to iquery.')
    parser.add_argument('-p', '--port',
                        help='Port number to be passed to iquery.')
    parser.add_argument('-t', '--temp-only', action='store_true',
                        help='Limiting the candidates to temp arrays.')
    parser.add_argument('-A', '--auth-file',
                        help='Authentication file to be passed to iquery.')
    parser.add_argument('-U', '--user-name',
                        help='Deprecated: Use --auth-file instead.  User name to be passed to iquery.')
    parser.add_argument('-P', '--user-password',
                        help='Deprecated: Use --auth-file instead.  User password to be passed to iquery.')

    parser.add_argument('-v', '--verbose', default=True,
                        help='display verbose output.')
    parser.add_argument('regex', metavar='REGEX', type=str, nargs='?', default='.*',
                        help='''Regular expression to match against array names.
                        The utility will remove arrays whose names match the regular expression.
                        Default is '.+', meaning to remove all arrays, because the pattern matches all names.
                        The regular expression must match the full array name.
                        For instance, '.*s' will match array 'dogs' because it ends with 's',
                        but will not match array 'moose' because it does not end with 's'.'''
                        )

    _temp_auth_file = None
    _arrays_removed=0
    args = parser.parse_args()

    try:
        if args.verbose==True:
            print >> sys.stderr, "args={0}".format(args)

        if args.user_name and args.user_password and (args.auth_file==None):
            print >> sys.stderr, '\nWARNING:  --user-name and --user-password are deprecated. Use --auth-file instead.\n'
            _temp_auth_file = create_auth_file(args.user_name, args.user_password)
            args.auth_file      = _temp_auth_file.name
            args.user_name      = None
            args.user_password  = None

        iquery_cmd = scidb_afl.get_iquery_cmd(args)
        namespaces = scidb_afl.get_namespace_names(iquery_cmd)

        if args.verbose==True:
            print >> sys.stderr, "namespaces={0}".format(namespaces)


        _arrays_removed=0
        for namespace in namespaces:
            if args.verbose==True:
                print >> sys.stderr, "\nSearching namespace: ", namespace

            names = scidb_afl.get_array_names(
                iquery_cmd = iquery_cmd,
                temp_only=args.temp_only,
                namespace=namespace)

            if args.verbose==True:
                print >> sys.stderr, "names={0}".format(names)

            names_to_remove = []

            for name in names:
                match_name = re.match('^'+args.regex+'$', name)
                if match_name:
                    if args.verbose==True:
                            print >> sys.stderr, "Schedule {0}.{1} to be removed".format(
                                namespace, name)
                    names_to_remove.append(name)

            if not names_to_remove:
                if args.verbose==True:
                    print "There are no arrays to remove in namespace", namespace
                continue

            if not args.force:
                print 'The following arrays are about to be removed from namespace {0}:'.format(
                    namespace)
                print names_to_remove

                proceed = scidb_psf.confirm(prompt='Are you sure you want to remove?', resp=False)
                if not proceed:
                    return

            for name in names_to_remove:
                scidb_afl.remove_array(name, namespace, iquery_cmd)
                if args.verbose==True:
                    print >> sys.stderr, "array {0}.{1} removed".format(namespace, name)
                _arrays_removed += 1

            if namespace != 'public':
                names = scidb_afl.get_array_names(
                    iquery_cmd=iquery_cmd,
                    temp_only=args.temp_only,
                    namespace=namespace)

                if not names:
                    scidb_afl.afl(
                        iquery_cmd,
                        "drop_namespace('{0}');".format(namespace))

                    if args.verbose==True:
                        print >> sys.stderr, "namespace {0} removed".format(namespace)


        if args.verbose==True:
            print >> sys.stderr, 'Number of arrays removed =', _arrays_removed

        if _temp_auth_file:
            _temp_auth_file.close()
            _temp_auth_file = None

    except Exception, e:
        print >> sys.stderr, '------ Exception -----------------------------'
        print >> sys.stderr, e

        if args.verbose==True:
            print >> sys.stderr, 'Number of arrays removed =', _arrays_removed

        if args.verbose==True:
            print >> sys.stderr, '------ Traceback (for debug purpose) ---------'
            traceback.print_exc()

        if _temp_auth_file:
            _temp_auth_file.close()
            _temp_auth_file = None

        print >> sys.stderr, '----------------------------------------------'
        sys.exit(-1)  # upon an exception, throw -1

    # normal exit path
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
