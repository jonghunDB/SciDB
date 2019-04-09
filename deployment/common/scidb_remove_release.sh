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
        transaction_all=`yum history list paradigm4-${release}-all-coord | awk '$1 ~ /[0-9]+/ {print $1}' | head -1`
    else
        transaction_all=`yum history list paradigm4-${release}-all | awk '$1 ~ /[0-9]+/ {print $1}' | head -1`
    fi
    if [ "${transaction_all}" != "" ]; then
        yum history undo -y ${transaction_all}
    fi
    transaction_tools=`yum history list paradigm4-${release}-dev-tools | awk '$1 ~ /[0-9]+/ {print $1}' | head -1`
    if [ "${transaction_tools}" != "" ]; then
        yum history undo -y ${transaction_tools}
    fi
}

function ubuntu1404 ()
{
    echo "Updating apt repositories..."
    apt-get update > /dev/null
    echo "Purging paradigm4 repositories..."
    if [ "1" == "${with_coordinator}" ]; then
	apt-get purge -y paradigm4-${release}-all-coord
    else
	apt-get purge -y paradigm4-${release}-all
    fi
    apt-get purge -y paradigm4-${release}-dev-tools
    apt-get autoremove --purge -y
}

release=${1}
with_coordinator=${2}

case $(./os_detect.sh) in
    "CentOS 6"|"RedHat 6")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	centos6
	;;
    "CentOS 7"|"RedHat 7")
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
