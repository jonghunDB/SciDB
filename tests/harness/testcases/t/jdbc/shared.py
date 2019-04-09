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
"""A function shared by multiple tests in the jdbc suite.

@author Donghui Zhang

Assumptions:
  - It assumes SciDB is running.
  - It assumes $SCIDB_INSTALL_PATH is set correctly.
"""

import subprocess
import sys
import os
import re
import random
import find_java8

def my_call(using_java_iquery, q, ignore_output=False, expected_error=''):
    """Call either the java version of the c++ version of iquery.

    @param using_java_iquery  whether to use the Java version.
    @param q  the query string.
    @param ignore_output  whether to use the '-n' switch.
    @param expected_error  the expected error if any.
    """
    # Print the query string.
    print q

    env_scidb_config_user = os.environ["SCIDB_CONFIG_USER"]

    # Choose between Java or C++ version of iquery.
    cmd_list = [
               os.environ['SCIDB_INSTALL_PATH']+"/bin/iquery"
               ]

    if using_java_iquery:
        cmd_list = [
              find_java8.find(),
              "-ea",
              "-cp",
              os.environ['SCIDB_INSTALL_PATH']+"/jdbc/scidb4j.jar",
              "org.scidb.iquery.Iquery"
              ]

    if len(env_scidb_config_user) > 0:
        cmd_list.extend(["--auth-file", env_scidb_config_user])

    if ignore_output:
        cmd_list.extend(['-n'])

    cmd_list.extend(["-aq", q])

    # Execute the query.
    p = subprocess.Popen(cmd_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate() # collect stdout,stderr, wait

    # Print the result.
    if expected_error=='' and err!='':
        raise Exception(err)
    elif expected_error!='' and err=='':
        raise Exception("Expected error " + expected_error + ", but the query succeeded.")
    elif expected_error!='' and err!='':
        if expected_error in err:
            print 'Expecting error ' + expected_error + ', and the query failed with that error. Good!'
            print
        else:
            raise Exception('The expected error, ' + expected_error + ', did not match the actual error\n' + err)
    else: # expected_error=='' and err==''
        print out
