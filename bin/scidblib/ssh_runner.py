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
SSH wrapper for running remote commands and copying remote files.

See scidb_backup.py for usage examples.
"""

import os
import pwd
import subprocess as subp
import sys


class SshRunner(object):

    """SSH wrapper for running successive remote commands."""

    def __init__(self, host=None, wdir=None, port=None):
        """Specify remote hostname, working directory, and SSH port."""
        self.wdir = wdir
        self.host = 'localhost' if host is None else host
        self.user = pwd.getpwuid(os.getuid())[0]
        self.prefix = ['ssh']
        if port is None:
            self.port = None
        else:
            self.prefix.extend(['-p', str(port)])
            self.port = int(port)
        self.prefix.append('@'.join((self.user, self.host)))
        self.prefix.append("PATH=/bin:/usr/bin:/sbin:/usr/sbin")
        self.returncode = 0

    def __repr__(self):
        return "{}(host={}, wdir={})".format(
            type(self).__name__, self.host, self.wdir)

    def _cd(self, cmd):
        """Wrap a remote command with 'cd' in subshell."""
        if self.wdir is None:
            return cmd
        if not isinstance(cmd, basestring):
            cmd = (' '.join(str(x) for x in cmd))
        return "(cd {0} && {1})".format(self.wdir, cmd)

    def run(self, cmd):
        """Run remote command."""
        args = list(self.prefix)
        args.append(self._cd(cmd))
        p = subp.Popen(args, stdout=subp.PIPE, stderr=subp.PIPE)
        out, err = p.communicate()
        self.returncode = p.returncode
        return out, err

    def cd(self, wdir):
        """Change the working directory."""
        out, err = self.run("(cd {0} && echo $(pwd))".format(wdir))
        if self.ok():
            # Use $(pwd) output in case we traversed a relative symlink.
            self.wdir = out.strip()
        return out, err

    def ok(self):
        """Make returncode checking easy: exit status 0 is success."""
        return self.returncode == 0

    def failed(self):
        """Make returncode checking easy: non-zero exit status is failure.

        @note This denotes failure of the command being run, not of SSH itself.
        """
        return self.returncode != 0

    def _scp(self, source, dest, is_get=True):
        """Run scp(1) with these strings as parameters."""

        # Fix relative remote paths to use wdir.
        def make_absolute(path):
            if not path.startswith(os.sep) and self.wdir is not None:
                return os.sep.join((self.wdir, path))
            else:
                return path

        # Tack on correct credentials.
        def adorn(path):
            return ':'.join(('@'.join((self.user, self.host)), path))

        if is_get:
            # source is remote
            source = adorn(make_absolute(source))
        else:
            # dest is remote
            dest = adorn(make_absolute(dest))

        # Run it, baby!
        cmd = ['scp']
        if self.port:
            cmd.extend(['-P', str(self.port)])
        cmd.extend([source, dest])
        p = subp.Popen(cmd, stdout=subp.PIPE, stderr=subp.PIPE)
        out, err = p.communicate()
        self.returncode = p.returncode
        return out, err

    def get_file(self, remote, local):
        """Copy remote file to local file."""
        return self._scp(remote, local, is_get=True)

    def put_file(self, local, remote):
        """Copy local file to remote file."""
        return self._scp(local, remote, is_get=False)


if __name__ == '__main__':
    print >>sys.stderr, "No main program, sorry!"
    sys.exit(1)
