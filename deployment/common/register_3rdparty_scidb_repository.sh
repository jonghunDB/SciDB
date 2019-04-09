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

function centos6()
{
(cat <<EOF
[scidb3rdparty]
name=SciDB 3rdparty repository
baseurl=https://downloads.paradigm4.com/centos6.3/3rdparty
gpgkey=https://downloads.paradigm4.com/key
gpgcheck=1
enabled=0
EOF
) > /etc/yum.repos.d/scidb3rdparty.repo
    yum clean all > /dev/null
    yum makecache fast
}

function centos7()
{
(cat <<EOF
[scidb3rdparty]
name=SciDB 3rdparty repository
baseurl=https://downloads.paradigm4.com/centos7/3rdparty
gpgkey=https://downloads.paradigm4.com/key
gpgcheck=1
enabled=0
EOF
) > /etc/yum.repos.d/scidb3rdparty.repo
    yum clean all > /dev/null
    yum makecache fast
}

function ubuntu1404()
{
    wget -O- https://downloads.paradigm4.com/key | apt-key add -
    echo "deb https://downloads.paradigm4.com/ ubuntu14.04/3rdparty/" > /etc/apt/sources.list.d/scidb3rdparty.list
    apt-get update > /dev/null
}

case $(./os_detect.sh) in
    "CentOS 6"|"RedHat 6")
	centos6
	;;
    "CentOS 7"|"RedHat 7")
	centos7
	;;
    "Ubuntu 14.04")
	ubuntu1404
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
