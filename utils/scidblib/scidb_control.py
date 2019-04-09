#!/usr/bin/python

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

import ConfigParser
import json
import subprocess
import sys
import os
import re
import stat
import random
import string
import time

from cStringIO import StringIO
from scidblib.iquery_client import IQuery


def get_scidb_env():
    d = {
        'install_path': os.environ.get('SCIDB_INSTALL_PATH', ''),
        'scidb_host': os.environ.get('IQUERY_HOST', '127.0.0.1'),
        'scidb_port': os.environ.get('IQUERY_PORT', '1239'),
        'cluster_name': os.environ.get('SCIDB_CLUSTER_NAME', ''),
        }
    return d


def stop_scidb():
    scidb_env = get_scidb_env()
    if not scidb_env['install_path']:
        raise RuntimeError("Invalid environment variable 'install_path'")
    scidb_py = os.path.join(scidb_env['install_path'], 'bin', 'scidb.py')
    cmd = [scidb_py, 'stopall', scidb_env['cluster_name']]
    proc = subprocess.Popen(' '.join(cmd), shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_text, err_text = proc.communicate()
    return proc.returncode, out_text, err_text


def start_scidb(config_ini=None, auth_file=None, max_attempts=None):
    scidb_env = get_scidb_env()
    if not scidb_env['install_path']:
        raise RuntimeError("Invalid environment variable 'install_path'")

    scidb_py = os.path.join(scidb_env['install_path'], 'bin', 'scidb.py')
    cmd = [scidb_py, 'startall', scidb_env['cluster_name']]
    if config_ini is not None:
        cmd.append(config_ini)

    if auth_file:
        cmd.extend(['--auth-file', auth_file])

    if max_attempts is not None:
        cmd.extend(['-M', str(max_attempts)])

    print "start_scidb cmd=" + str(cmd)

    proc = subprocess.Popen(
        ' '.join(cmd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    out_text, err_text = proc.communicate()

    return proc.returncode, out_text, err_text


def init_scidb():
    scidb_env = get_scidb_env()
    if scidb_env['install_path'] == '':
        raise RuntimeError("Invalid environment variable 'install_path'")

    scidb_py = os.path.join(scidb_env['install_path'], 'bin', 'scidb.py')
    cmd = [scidb_py, 'init-all', scidb_env['cluster_name']]

    proc = subprocess.Popen(' '.join(cmd),
                            shell=True,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    out_text, err_text = proc.communicate('y\n')

    return proc.returncode, out_text, err_text


def restart_scidb(config_ini=None, auth_file=None, max_attempts=None,
                  want_sync=False):
    # Stop!
    ret, text_out, text_err = stop_scidb()
    assert ret == 0, text_err
    if ret != 0:
        raise RuntimeError(
            "Unable to stop scidb\nerr={0}\nini={1}\nauth={2}".format(
                text_err, config_ini, auth_file))
    # Start!!
    ret, text_out, text_err = start_scidb(
        config_ini,
        auth_file=auth_file,
        max_attempts=max_attempts)
    if ret != 0:
        raise RuntimeError(
            "Unable to start scidb\nerr={0}\nini={1}\nauth={2}".format(
                text_err, config_ini, auth_file))
    # Maybe sync!!!
    if want_sync:
        try:
            sync_scidb(auth_file=auth_file)
        except RuntimeError as e:
            print >>sys.stderr, \
                "Warning: Could not synchronize SciDB after restart:", e


def sync_scidb(auth_file=None, max_attempts=12, noexcept=False):
    """Run AFL sync() operator until it succeeds.

    @param auth authentication filename or (user, password) tuple
    @param max_attempts try once a second for this many seconds
    @param noexcept don't raise on failure, instead return False
    @returns True on success, False on failure (if noexcept)

    @note Since sync() is only available in the P4 'system' library,
          this will fail if the library is not loaded.
    """
    iquery = IQuery(auth_file=auth_file, afl=True)
    err = ''
    for _ in xrange(max_attempts):
        _, err = iquery("sync()")
        if not err:
            return True
        if "SCIDB_LE_LOGICAL_OP_DOESNT_EXIST" in err:
            if noexcept:
                return False
            raise RuntimeError("P4 'system' library is not loaded")
        else:
            time.sleep(1)
    if noexcept:
        return False
    raise RuntimeError("sync_scidb:\n%s", err)


def change_security_mode(mode, config_ini):
    """Slam mode into security=... config.ini entry.

    @note WARNING: This changes the security mode for *all* clusters
          described in the config file!

    @note When reading INI files, prefer to use ConfigParser directly.
          When writing, ConfigParser has issues around preserving
          comments and ordering, so unfortunately -ad hoc- solutions
          like this one are necessary.
    """
    if mode not in ('trust', 'password'):
        print >>sys.stderr, (
            "Warning: Unrecognized security mode '%s' being set!" % mode)
    sio = StringIO()
    found = False
    with open(config_ini) as F:
        for line in F:
            m = re.match(r'\s*security\s*=', line)
            if m:
                print >>sio, "security=%s" % mode
                found = True
            else:
                print >>sio, line.rstrip('\n')
    if not found:
        print >>sio, "security=%s" % mode
    sio.reset()
    with open(config_ini, 'w') as F:
        F.write(sio.read())


def get_ini_value(config_ini, section, key):
    """Retrieve key from [section] of INI file, or empty string."""
    config = ConfigParser.SafeConfigParser()
    config.read(config_ini)
    try:
        return config.get(section, key)
    except Exception as e:
        print >>sys.stderr, "get_ini_value({0}, {1}): {2}".format(
            section, key, e)
        return ""


class ConfigParserWithComments(ConfigParser.ConfigParser):
    # http://stackoverflow.com/questions/8533797/adding-comment-with-configparser    # noqa E501

    def add_comment(self, section, comment):
        """Write a comment to the section specified

        @param section The section to write to, or empty if writing to a
                       non-section area
        @param comment The comment to write to the section
        """
        if len(comment) == 0:
            self.set(section, ';', None)
        else:
            self.set(section, '; %s' % (comment,), None)

    def write(self, fp):
        """Write an .ini-format representation of the configuration state.

        @param fp Pointer to the file to be written to
        """
        if self._defaults:
            fp.write("[%s]\n" % ConfigParser.DEFAULTSECT)
            for (key, value) in self._defaults.items():
                self._write_item(fp, key, value)
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                self._write_item(fp, key, value)
            fp.write("\n")

    def _write_item(self, fp, key, value):
        """Write a key=value pair item to a file

        @param fp Pointer to the file to be written to
        @param key The 'key' portion of the key=value pair
        @param value The 'value' portion of the key=value pair
        """
        if key.startswith(';') and value is None:
            fp.write("%s\n" % (key,))
        else:
            fp.write("%s = %s\n" % (key, str(value).replace('\n', '\n\t')))


def init_auth_file(user_name, user_password, auth_file_name, use_ini=False):
    """Initialize the authentication file with the appropriate information

    @param user_name The user's name
    @param user_password The user's password (in plaintext format)
    @param auth_file_name The name of the file to initialize
    """
    if use_ini:
        config = ConfigParserWithComments()
        config.optionxform = str  # Make the parser preserve case

        config.add_section("security_password")
        config.set("security_password", "user-name",        user_name)
        config.set("security_password", "user-password",    user_password)

        # Write the configuration file to 'auth_file_name'
        with open(auth_file_name, 'wb') as F:
            config.write(F)
    else:
        with open(auth_file_name, 'wb') as F:
            print >>F, json.dumps({
                "user-name": user_name,
                "user-password": user_password})


def create_auth_file(user_name, user_password, auth_file_name=None):
    auth_filename = auth_file_name
    if not auth_filename:
        auth_filename = os.path.join(
            '/', 'tmp',
            'auth_{0}'.format(''.join(random.sample(string.letters, 10))))
    init_auth_file(user_name, user_password, auth_filename)
    os.chmod(auth_filename, stat.S_IRUSR | stat.S_IWUSR)
    return auth_filename
