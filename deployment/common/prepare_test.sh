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

set -u

SCIDB_VER="${1}"
OS=$(./os_detect.sh)

function die()
{
    echo 1>&2 "$@"
    exit 1
}

echo "Preparing SciDB test VMs..."

case ${OS} in
    "CentOS 7"|"RedHat 7")
	INSTALL="yum install -y -q"

	yum clean all
	yum makecache fast
	# ...setup epel repo (libcsv is in there)
	rm -f epel-release-latest-7.noarch.rpm
	wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
	rpm -ivh epel-release-latest-7.noarch.rpm
	rm -f epel-release-latest-7.noarch.rpm
	yum makecache fast
	${INSTALL} epel-release
	# ...setup software collections for devtoolset-3
	${INSTALL} "https://downloads.paradigm4.com/devtoolset-3/centos/7/sclo/x86_64/rh/devtoolset-3/scidb-devtoolset-3.noarch.rpm"
	yum makecache fast
	### Compiler's debug
	# gcc/g++/gfort version 4.9
	${INSTALL} devtoolset-3-gdb
	### net-tools needed
	${INSTALL} net-tools
	;;
    "CentOS 6"|"RedHat 6")
	INSTALL="yum install -y -q"

	yum clean all
	yum makecache fast
	# ...setup epel repo (libcsv is in there)
	rm -f epel-release-latest-6.noarch.rpm
	wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm
	rpm -ivh epel-release-latest-6.noarch.rpm
	rm -f epel-release-latest-6.noarch.rpm
	yum makecache fast
	${INSTALL} epel-release
	yum makecache fast
	# ...setup software collections for devtoolset-3
	${INSTALL} "https://downloads.paradigm4.com/devtoolset-3/centos/6/sclo/x86_64/rh/devtoolset-3/scidb-devtoolset-3.noarch.rpm"
	yum makecache fast
	### Compiler's debug
	# gcc/g++/gfort version 4.9
	${INSTALL} devtoolset-3-gdb
	;;
    "Ubuntu 14.04")
	apt-get update > /dev/null
	export DEBIAN_FRONTEND=noninteractive
	INSTALL="apt-get install -y -q"

	echo "Installing Ubuntu repositories..."
	${INSTALL} python-software-properties
	add-apt-repository ppa:ubuntu-toolchain-r/test
	add-apt-repository -y ppa:openjdk-r/ppa

	echo "Updating apt repositories..."
	apt-get update > /dev/null
	;;
    *)  die "Not a supported OS: $OS"
	;;
esac

# All platforms install pip
echo "Installing python-pip"
${INSTALL} python-pip
#
# Then do a 'pip install' of pip-packages.txt
#
if [ -e pip-packages.txt ]
then
    grep -v -E '^#' pip-packages.txt | while read LINE
    do
        if ! pip install $LINE
        then
            die "Could not pip install $LINE"
        fi
    done
fi

echo "...prepared SciDB test VMs"
