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
(echo <<EOF "[scidb]
name=SciDB repository
baseurl=https://downloads.paradigm4.com/centos6.3/${release}
gpgkey=https://downloads.paradigm4.com/key
gpgcheck=1
enabled=0"
EOF
) | tee scidb.repo
    REPO_FILE=/etc/yum.repos.d/scidb.repo
    mv scidb.repo ${REPO_FILE}
    yum clean all
    yum makecache fast
}

function ubuntu1404()
{
    echo "Creating scidb repository..."
    wget -O- https://downloads.paradigm4.com/key | apt-key add -
    echo "deb https://downloads.paradigm4.com/ ubuntu14.04/${release}/" > scidb.list
    echo "deb-src https://downloads.paradigm4.com/ ubuntu14.04/${release}/" >> scidb.list
    cat scidb.list

    REPO_FILE=/etc/apt/sources.list.d/scidb.list
    mv scidb.list ${REPO_FILE}
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
}

release=${1}

case $(./os_detect.sh) in
    "CentOS 6"|"RedHat 6"|"CentOS 7"|"RedHat 7")
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
