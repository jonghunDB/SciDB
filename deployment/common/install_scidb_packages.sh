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
# Requires root privs
if [ $# -lt 1 ]; then
   echo "Usage: install_scidb_packages.sh [<package_file_name>]+"
   exit 1
fi

PACKAGE_FILE_NAME_LIST=$@

case $(./os_detect.sh) in
    "CentOS 6"|"CentOS 7"|"RedHat 6"|"RedHat 7")
	echo "Installing SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
	rpm -i ${PACKAGE_FILE_NAME_LIST}
	;;
    "Ubuntu 14.04")
	echo "Installing SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
	dpkg -i ${PACKAGE_FILE_NAME_LIST}
	;;
    *)
	echo "Not a supported OS"
	exit 1
	;;
esac

rm -rf ${PACKAGE_FILE_NAME_LIST}
