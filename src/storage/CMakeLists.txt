########################################
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
########################################

message(STATUS "****************** BEGIN src/storage/CMakeLists.txt ******************")

set (storage_src
    StorageMgr.cpp
    BufferMgr.cpp
    BufferMgr_debug.cpp
    IndexMgr.cpp
    IndexMap.cpp
    PersistentIndexMap.cpp
)

file(GLOB storage_include "*.h")

add_library(storage_lib STATIC ${storage_src} ${storage_include})
target_link_libraries(storage_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES} rocksdb)
add_dependencies(storage_lib scidb_msg_lib)

message(STATUS "****************** END src/storage/CMakeLists.txt ******************")
