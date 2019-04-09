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
message(STATUS "****************** BEGIN CMakeBuildFlags.cmake ******************")

# ALL_BASE_FLAGS
set(ALL_BASE_FLAGS "")
# include paths
set(ALL_BASE_FLAGS "${ALL_BASE_FLAGS} -Wno-system-headers -isystem /opt/local/include/ -isystem /usr/local/include/")
# defines
set(ALL_BASE_FLAGS "${ALL_BASE_FLAGS} -D__STDC_FORMAT_MACROS")
set(ALL_BASE_FLAGS "${ALL_BASE_FLAGS} -D__STDC_LIMIT_MACROS")
# code generation
set(ALL_BASE_FLAGS "${ALL_BASE_FLAGS} -fPIC")
set(ALL_BASE_FLAGS "${ALL_BASE_FLAGS} -g -ggdb3 -fno-omit-frame-pointer -fPIC")

# CLIKE_LANGUAGE_WARNINGS  for C/C++ but most don't apply to Fortran
# adjustments to warnings
# [folks who added these should add a comment for each explaining why]
set(CLIKE_LANGUAGE_WARNINGS "")
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -W -Wextra -Wall")
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-unused-local-typedefs")  # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-strict-aliasing")        # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-long-long")              # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-unused-parameter")       # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-variadic-macros")        # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wconversion")                # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Werror")                     # please explain
set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-error=array-bounds")

#
# CMAKE_<LANG>_FLAGS
#
set(CMAKE_CXX_FLAGS     "${ALL_BASE_FLAGS} -std=c++14 -pedantic ${CLIKE_LANGUAGE_WARNINGS}")    # C++ specific flags
 # TODO: eliminate PROJECT_ROOT
 set(CMAKE_CXX_FLAGS     "${CMAKE_CXX_FLAGS} -DPROJECT_ROOT=\\\"${CMAKE_SOURCE_DIR}/\\\"")

set(CMAKE_C_FLAGS       "${ALL_BASE_FLAGS} -pedantic ${CLIKE_LANGUAGE_WARNINGS}")                    # C specific flags
set(CMAKE_Fortran_FLAGS "${ALL_BASE_FLAGS} -fno-f2c -Wline-truncation")           # Fortran specific flags

# CMAKE_<LANG>_RELWITHDEBINFO  -- not asserted, optimized (same as _ASSERT with asserts disabled)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO     "-DNDEBUG -O3 -fno-strict-aliasing") # -fno-strict-aliasing because the alternative is -Wno-error
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO     "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wno-error=unused-result") # Ubuntu warns of unused-result with -O[2-3], CentOS does not
set(CMAKE_C_FLAGS_RELWITHDEBINFO       "-DNDEBUG -O3")
set(CMAKE_Fortran_FLAGS_RELWITHDEBINFO "-DNDEBUG -O3")

# CMAKE_<LANG>_DEBUG           -- asserted, unoptimized
set(CMAKE_CXX_FLAGS_DEBUG     "-O0")
set(CMAKE_C_FLAGS_DEBUG       "-O0")
set(CMAKE_Fortran_FLAGS_DEBUG "-O0")

# CMAKE_<LANG>_PROFILE         -- not asserted, partial optimized, profiled
set(CMAKE_CXX_FLAGS_PROFILE     "-DNDEBUG -O2 -pg")
set(CMAKE_CXX_FLAGS_PROFILE     "${CMAKE_CXX_FLAGS_PROFILE} -fno-strict-aliasing")  # -fno-strict-aliasing because the alternative is -Wno-error
set(CMAKE_CXX_FLAGS_PROFILE     "${CMAKE_CXX_FLAGS_PROFILE} -Wno-error=unused-result") # Ubuntu warns of unused-result with -O[2-3], CentOS does not
set(CMAKE_C_FLAGS_PROFILE       "-DNDEBUG -O2 -pg")
set(CMAKE_Fortran_FLAGS_PROFILE "-DNDEBUG -O2 -pg")

set(CMAKE_EXE_LINKER_FLAGS_PROFILE "-pg")       # why not CMAKE_LDFLAGS as done by _CC ?

# Clean exit is not sufficient for getting code coverage data flushed, so we need an explicit define so the
# function to flush the data can be explicitly called.  We are also calling it at the end of every successful
# query, not just when the applications exits.
# CMAKE_<LANG>_CC              -- asserted, unoptimized, code coverage
set(CMAKE_CXX_FLAGS_CC          "-DCLEAN_EXIT -DCOVERAGE -O0 -fprofile-arcs -ftest-coverage")
set(CMAKE_C_FLAGS_CC            "-DCLEAN_EXIT -DCOVERAGE -O0 -fprofile-arcs -ftest-coverage")
set(CMAKE_Fortran_FLAGS_CC      "-DCLEAN_EXIT -DCOVERAGE -O0 -fprofile-arcs -ftest-coverage")

set(CMAKE_LDFLAGS_CC "-fprofile-arcs -ftest-coverage")

# CMAKE_<LANG>_VALGRIND        -- asserted, unoptimized, interpreted (very very slow)
set(CMAKE_CXX_FLAGS_VALGRIND     "-DCLEAN_EXIT -O0")
set(CMAKE_C_FLAGS_VALGRIND       "-DCLEAN_EXIT -O0")
set(CMAKE_Fortran_FLAGS_VALGRIND "-DCLEAN_EXIT -O0")

# CMAKE_<LANG>_ASSERT          -- asserted, optimized
set(CMAKE_CXX_FLAGS_ASSERT     "-O3 -fno-strict-aliasing")  # -fno-strict-aliasing because the alternative is -Wno-error
set(CMAKE_CXX_FLAGS_ASSERT     "${CMAKE_CXX_FLAGS_ASSERT} -Wno-error=unused-result")  # Ubuntu warns of unused-result with -O[2-3], CentOS does not

set(CMAKE_C_FLAGS_ASSERT       "-O3")
set(CMAKE_Fortran_FLAGS_ASSERT "-O3")

# TODO: according to cmake.org/Wiki/CMake_FAQ#How_can_I_specify_my_own_configurations
#       all the non-built-in configurations should be setting
#       CMAKE_EXE_LINKER_FLAGS_XXX
#       and marking them as advanced below
#       but historically we have only been setting
#       CMAKE_EXE_LINKER_FLAGS_PROFILE and have not been advancing it
#       This needs investigation.
#       See cmake.org/Wiki/CMake_FAQ#How_can_I_specify_my_own_configurations
MARK_AS_ADVANCED(CMAKE_CXX_FLAGS_ASSERT
                 CMAKE_C_FLAGS_ASSERT
                 CMAKE_Fortran_FLAGS_ASSERT)

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
                     "Choose the type  of build, options are: RelWithDebInfo Assert Debug CC Profile Release Valgrind")

message(STATUS "****************** END CMakeBuildFlags.cmake ******************")
