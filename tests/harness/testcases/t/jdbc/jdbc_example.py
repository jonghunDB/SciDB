#!/usr/bin/python
#
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
#
import subprocess
import time
import sys
import os
import find_java8


def runSubProcess(cmd,  # Command to run (list of string options).
                  si=None,  # Standard in.
                  so=subprocess.PIPE,  # Standard out.
                  se=subprocess.PIPE,  # Standard error.
                  useShell=False):  # Use shell option when starting process.

    localCmd = list(cmd)  # Make private copy (paranoid)
    if useShell:  # If command is for shell, flatten it into a single string
        localCmd = ' '.join(localCmd)

    proc = subprocess.Popen(localCmd,
                            stdin=si,
                            stdout=so,
                            stderr=se,
                            shell=useShell)
    return proc


def main(test_name, expected_result):
    """Given a test in example.jar, run it.

    @param test_name  the name of a Java file in example.jar, e.g. JDBCExample.
    @param expected_result  the name of a file that stores the expected result.
    """
    print 'SCIDB_INSTALL_PATH', os.environ['SCIDB_INSTALL_PATH']

    iquery_host = 'localhost'
    iquery_port = '1239'
    if 'IQUERY_HOST' in os.environ:
        iquery_host = os.environ['IQUERY_HOST']
    if 'IQUERY_PORT' in os.environ:
        iquery_port = os.environ['IQUERY_PORT']

    cmd = [
        find_java8.find(),
        '-classpath',
        ':'.join(('${SCIDB_INSTALL_PATH}/jdbc/example.jar',
                  '${SCIDB_INSTALL_PATH}/jdbc/scidb4j.jar')),
        'org.scidb.%s' % test_name,
        iquery_host,
        iquery_port
        ]

    env_scidb_config_user = os.environ["SCIDB_CONFIG_USER"]
    if len(env_scidb_config_user) > 0:
        cmd.extend([env_scidb_config_user])

    proc = runSubProcess(cmd, useShell=True)
    exitCode = proc.poll()
    while (exitCode is None):
        time.sleep(0.1)
        exitCode = proc.poll()

    sOut = proc.stdout.read().strip()
    sErr = proc.stderr.read().strip()

    if (exitCode != 0):
        print 'Bad exit code!'
        print sErr
        sys.exit(1)

    print sOut

    generated_lines = sOut.split('\n')

    # Verify the result
    expected_lines = open(expected_result).readlines()
    len_expected = len(expected_lines)
    len_generated = len(generated_lines)
    if len_generated != len_expected:
        print "Expected %s lines but received %s lines" % (
            len_expected, len_generated)
        sys.exit(1)
    for i in range(len_expected):
        if expected_lines[i].rstrip() != generated_lines[i]:
            print "Line {0} mismatch!".format(i+1)
            print "Expected:  %s" % expected_lines[i]
            print "Generated: %s" % generated_lines[i]
            sys.exit(1)
    print "Java code produced expected result."


if __name__ == '__main__':
    test_name = sys.argv[1]
    expected_result = sys.argv[2]
    try:
        main(test_name, expected_result)
    except Exception as e:
        print "jdbc_example.py caught exception:"
        print e
