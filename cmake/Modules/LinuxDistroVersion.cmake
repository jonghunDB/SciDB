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

##############################################################
# This module detects variables used in packages:
# ${DISTRO_NAME}
# ${DISTRO_VER}
# ${DISTRO_NAME_VER}
##############################################################

# TODO. It would be preferable to use lsb_release which is actively
# maintained in all Linxu Distributions rather then os_detect.sh (see SDB-5839)
#
# find_program(LSB_RELEASE lsb_release)
# execute_process(COMMAND ${LSB_RELEASE} -is
#   OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT
#   OUTPUT_STRIP_TRAILING_WHITESPACE
#   )


if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/deployment/common/os_detect.sh")
	set(OS "${CMAKE_CURRENT_SOURCE_DIR}/deployment/common/os_detect.sh")
    elseif(EXISTS "${SCIDB_SOURCE_DIR}/deployment/common/os_detect.sh")
	set(OS "${SCIDB_SOURCE_DIR}/deployment/common/os_detect.sh")
    else()
	message(FATAL_ERROR, "Unable to find 'os_detect.sh'")
    endif()
    message("scidb:${CMAKE_CURRENT_SOURCE_DIR}")
    message("os_detect:${OS}")
    execute_process(COMMAND bash "-c" ${OS} OUTPUT_VARIABLE release)
    string(STRIP ${release} OS)
    string(REPLACE " " ";" rel_list ${release})

    list(GET rel_list 0 DISTRO_NAME)
    list(GET rel_list 1 DISTRO_VER)
    if(NOT ${DISTRO_NAME} STREQUAL "")
        set(DISTRO_NAME_VER "${DISTRO_NAME}")
    endif()
    if(NOT ${DISTRO_VER} STREQUAL "")
        set(DISTRO_NAME_VER "${DISTRO_NAME_VER}-${DISTRO_VER}")
    endif()

    set(CPACK_GENERATOR TGZ)
    if(DISTRO_NAME MATCHES "Ubuntu" OR DISTRO_NAME MATCHES "Debian")
        set(CPACK_GENERATOR DEB)
    endif()
    if(DISTRO_NAME MATCHES "SUSE" OR DISTRO_NAME STREQUAL "Fedora" OR DISTRO_NAME STREQUAL "RedHat" OR DISTRO_NAME STREQUAL "CentOS")
        set(CPACK_GENERATOR RPM)
    endif()
endif()

if(NOT ${DISTRO_NAME_VER} STREQUAL "")
    set(DISTRO_NAME_VER "${DISTRO_NAME}-${DISTRO_VER}")
else()
    set(DISTRO_NAME_VER "${CMAKE_SYSTEM_NAME}")
endif()
