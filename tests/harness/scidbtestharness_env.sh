#!/bin/bash
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

# Note: Prefer "127.0.0.1" to "localhost" because the former is used
# to generate the default config.ini, and ~/.pgpass Postgres auth
# requires an exact string match.

export DB_HOST=${SCIDB_DB_HOST:=127.0.0.1}
export DB_PORT=${SCIDB_DB_PORT:=5432}
export DB_NAME=${SCIDB_DB_NAME:=mydb}
export DB_USER=${SCIDB_DB_USER:=mydb}
export DB_PASSWD=${SCIDB_DB_PASSWD:=mydb}
export IQUERY_HOST=${SCIDB_HOST:=127.0.0.1}
export IQUERY_PORT=${SCIDB_PORT:=1239}
export SCIDB_CLUSTER_NAME=$DB_NAME
#
# CDash needs to NOT RUN parts of scidbtestharness_env.sh
# The variable CDASH_OVERRIDE is used by CDash to turn off the following.
#
# The USER_ROOT_login that would be created by scidbtestharness_env.sh can not be reached on the CDash coordinator.
# CDash creates this in common/harness.sh function prepare_coordinator().
#
# Since packages are built on the build VM and installed on the test VMs
# there is no SCIDB_BUILD_PATH on the coordinator and
# there is no SCIDB_SOURCE_PATH on the coordinator.
# The following exports and link assume a readable SCIDB_BUILD_PATH and SCIDB_SOURCE_PATH which is not there.
# These settings are NOT RUN if being used by CDash.
#
if ! ${CDASH_OVERRIDE:=false}; then
    export PYTHONPATH="${SCIDB_BUILD_PATH}/tests/harness/pyLib:${SCIDB_BUILD_PATH}/bin"

    if [ "${SCIDB_BUILD_PATH}" != "" -a "${SCIDB_DATA_PATH}" != "" ] ; then
	rm -f ${SCIDB_DATA_PATH}/0/tests
	ln -s ${SCIDB_SOURCE_PATH}/tests ${SCIDB_DATA_PATH}/0/tests

	export DOC_DATA="${SCIDB_SOURCE_PATH}/tests/harness/testcases/data/doc"
	export TESTCASES_DIR="${SCIDB_BUILD_PATH}/tests/harness/testcases/"
	export TEST_DATA_DIR="${SCIDB_DATA_PATH}/0/tests/harness/testcases/data"
	export TEST_UTILS_DIR="${SCIDB_SOURCE_PATH}/tests/utils"
	export TEST_BIN_DIR="${SCIDB_BUILD_PATH}/bin"
    fi

    function create_login_file() {
	local login_file=$1/${USER}_root_login
	echo "[security_password]" > $login_file
	echo "user-name      = root" >> $login_file
	echo "user-password  = Paradigm4" >> $login_file
	chmod 600 $login_file
	echo "$login_file"   # return $login_file
    }

    export SCIDB_CONFIG_USER=$(create_login_file ${SCIDB_BUILD_PATH})
    # echo "SCIDB_CONFIG_USER=$SCIDB_CONFIG_USER"
fi
