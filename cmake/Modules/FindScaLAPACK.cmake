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

# SCALAPACK_FOUND
# SCALAPACK_LIBRARIES

# This is a quick and dirty CMake module, and it would be well suited to be being updated to
# follow the CMake Find Modules guidelines:
#   https://cmake.org/Wiki/CMake:How_To_Find_Libraries#Writing_find_modules

find_library(SCALAPACK_LIBRARIES
        NAMES libscalapack-scidb-mpich2scidb.so.2.0 libscalapack-scidb-mpich2scidb.so libscalapack-scidb-mpich.so libscalapack-scidb-openmpi.so libscalapack-scidb.so  libscalapack-mpich2scidb.so libscalapack-mpich.so libscalapack-openmpi.so libscalapack.so
        PATHS /usr/lib/mpich2scidb/lib/ /usr/lib64/mpich2scidb/lib/ /usr/lib64/mpich/lib/ /usr/lib64/openmpi/lib/)
message(STATUS "SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
if (SCALAPACK_LIBRARIES)
  set(SCALAPACK_FOUND 1)
endif()

if (SCALAPACK_FOUND)
  message(STATUS "SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
endif()


