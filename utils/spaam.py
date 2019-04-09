#!/usr/bin/python

"""
Spaam is the SciDB Password and Account Manager.
"""

# BEGIN_COPYRIGHT
#
# Copyright (C) 2017-2018 SciDB, Inc.
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

import argparse
import base64
import hashlib
import os
import sys
import traceback

from scidblib import AppError
from scidblib.iquery_client import IQuery
from scidblib.util import getVerifiedPassword

_args = None                    # Parsed arguments
_pgm = None                     # Program name
_iquery = None                  # iquery client


def user_exists(u):
    """Return True iff user 'u' already exists in SciDB."""
    out, err = _iquery("project(list('users'), name)")
    if _iquery.returncode:
        raise AppError("Cannot list users:\n%s" % err)
    return u in out.splitlines()


def add_user(u):
    """Add a new SciDB user."""
    if user_exists(u):
        raise AppError("User '{0}' already exists".format(u))
    cleartext = raw_input() if _args.stdin else getVerifiedPassword()
    pwhash = base64.b64encode(hashlib.sha512(cleartext).digest())
    out, err = _iquery("create_user('{0}', '{1}')".format(u, pwhash))
    if _iquery.returncode:
        raise AppError(
            "Cannot create user '{0}':\n{1}".format(u, err))
    print out
    return 0


def modify_user(u):
    """Change SciDB user's password."""
    if not user_exists(u):
        raise AppError("User '{0}' does not exist".format(u))
    prompt = "New %s password: " % u
    cleartext = raw_input() if _args.stdin else getVerifiedPassword(prompt)
    pwhash = base64.b64encode(hashlib.sha512(cleartext).digest())
    out, err = _iquery("change_user('password', '{0}', '{1}')".format(
        u, pwhash))
    if _iquery.returncode:
        raise AppError("Cannot change password for user '{0}':\n{1}".format(
            u, err))
    print out
    return 0


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="SciDB password and account management (SPAAM)")
    parser.add_argument('-A', '--auth-file', default=None,
                        help='iquery authentication file in INI format')
    parser.add_argument('-c', '--host', default=None,
                        help='target host for iquery commands')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands')
    parser.add_argument('--stdin', default=False, action='store_true',
                        help="read password from stdin without prompting")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--add', metavar='USERNAME',
                       help='add account for username')
    group.add_argument('-m', '--modify', metavar='USERNAME',
                       help='modify password for username')

    global _args
    _args = parser.parse_args(argv[1:])
    assert bool(_args.add) ^ bool(_args.modify), (
        "Required mutually exclusive group doesn't work as advertised!")

    global _iquery
    _iquery = IQuery(afl=True, format='tsv',
                     host=_args.host, port=_args.port,
                     auth_file=_args.auth_file)

    try:
        return add_user(_args.add) if _args.add else modify_user(_args.modify)
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupt"
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
