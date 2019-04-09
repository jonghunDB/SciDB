########################################
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
########################################

set (Exercise1_src
        Exersice1.cpp
        LogicalExercise1.cpp
        PhysicalExercise1.cpp
        )

file(GLOB Exercise1_include "*.h")

add_library(Exercise1_lib STATIC ${Exercise1_src} ${Exercise1_include})
target_link_libraries(ops_lib compression_lib network_lib MurmurHash_lib ${libcsv_LIBRARY})





set(ops_lib_src
        BuildInOps.inc
        create_array/LogicalCreateArray.cpp
        create_array/PhysicalCreateArray.cpp

        )

find_package(Libcsv REQUIRED)
include_directories(${libcsv_INCLUDE_DIR})



