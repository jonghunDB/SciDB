#!/bin/bash

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

CONFIG_INI="$1"

if [ -z $CONFIG_INI ]; then
    echo "Usage: $0 <path-to-config.ini>"
    echo "e.g., $0 /opt/scidb/18.2/etc/config.ini"
    exit 2
fi

# parse config.ini for data paths
data_path=$(cat $CONFIG_INI | grep "base-path" | tail -n 1 | cut -d"=" -f 2)

# Search for at least one occurence of a re-write warning message in one of the scidb.log
# fliles. See BufferMgr::_writeSlotUnlocked LOG4CXX_WARN message.
find ${data_path} -type f -name scidb.log -print0 | xargs -0 -e egrep -n 'Re-writing Compressed BlockBase'

exit $?
