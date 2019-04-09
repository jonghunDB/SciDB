#!/bin/bash
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
# Test for loading data into SciDB using loadpipe.
# The test loads a large 2D matrix.
run_cleanup()
{
    echo "Cleaning up on exit..."
    iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} \
	-aq "remove(real_03_A_0)" > /dev/null 2>&1
    echo "Done with cleanup."
}
set -e

trap "run_cleanup" 0

DATA_GEN=${TEST_UTILS_DIR}/mp_data_gen.py
LOADPIPE=${SCIDB_INSTALL_PATH}/bin/loadpipe.py

ATTRS="
a_71:int32 NULL,
a_72:float DEFAULT 0.862792810755,
a_73:char NULL DEFAULT strchar('c'),
a_74:char DEFAULT strchar('s'),
a_75:bool DEFAULT true,
a_76:float NULL,
a_77:string NULL,
a_78:float DEFAULT 0.739194661953,
a_79:float NULL,
a_80:uint32,
a_81:int16 NULL,
a_82:int32 NULL,
a_83:int16 DEFAULT 7248,
a_84:int16 NULL DEFAULT 15313,
a_85:uint32 DEFAULT 26438644,
a_86:string NULL,
a_87:char NULL,
a_88:uint32 DEFAULT 3755072959,
a_89:int16 NULL,
a_90:int8,
a_91:float NULL,
a_92:uint32 DEFAULT 2575462157,
a_93:uint16 NULL DEFAULT 53460,
a_94:float NULL,
a_95:string NULL,
a_96:int64 NULL DEFAULT 0
"
SCHEMA="<
${ATTRS}
>
[
d_10=-25:*,317,1,
d_11=11:*,419,1
]"

FLAT_SCHEMA="<
d_10:int64,
d_11:int64,
${ATTRS}
>[
dummy=0:*,1000000,0
]"

echo "Creating array:"
iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "create array real_03_A_0 ${SCHEMA}"

echo "Loading data into array:"
${DATA_GEN} --dims-sizes 100,100 --seed 617706 --sparsity-rate 0.04 --null-rate 0.09 -n 3 "${SCHEMA}"\
 | ${LOADPIPE} -b 32MiB -s "${FLAT_SCHEMA}" -A real_03_A_0

echo "Ok."

iquery -ocsv:l -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "dimensions(real_03_A_0)"

echo "Done."
