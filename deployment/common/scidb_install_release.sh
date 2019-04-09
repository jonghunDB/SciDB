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

function centos6 ()
{
    yum makecache fast
    if [ "1" == "${with_coordinator}" ]; then
	yum install --enablerepo=scidb -y paradigm4-${release}-all-coord paradigm4-${release}-dev-tools
    else
	yum install --enablerepo=scidb -y paradigm4-${release}-all
    fi
}

function ubuntu1404 ()
{
    echo "Updating apt repositories..."
    apt-get update > /dev/null
    if [ "1" == "${with_coordinator}" ]; then
	apt-get install -y paradigm4-${release}-all-coord paradigm4-${release}-dev-tools
    else
	apt-get install -y paradigm4-${release}-all
    fi
}

release=${1}
with_coordinator=${2}

echo "Installing SciDB release..."
case $(./os_detect.sh) in
    "RedHat 6"|"CentOS 6")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	centos6
	;;
    "RedHat 7"|"CentOS 7")
	/sbin/chkconfig firewalld off
	/sbin/service firewalld stop
	centos6
	;;
    "Ubuntu 14.04")
	ubuntu1404
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
echo "...installed SciDB release"
