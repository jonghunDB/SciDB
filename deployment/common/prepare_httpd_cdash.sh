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

username=${1}

function die()
{
    echo 1>&2 "$@"
    exit 1
}

echo "Preparing httpd..."

case $(./os_detect.sh) in
    "CentOS 6")
	yum makecache fast
	service iptables stop
	chkconfig iptables off
	yum install -y httpd
	chkconfig httpd on
	service httpd start
	CONFIG=/etc/httpd/conf/httpd.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	usermod -G apache -a ${username}
	ln -s /public/cdash_logs /var/www/cdash_logs
	CONFIG=/etc/sysconfig/selinux
	cat ${CONFIG} | sed -e "s/enforcing/disabled/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	setenforce 0 || true
	;;
    "CentOS 7")
	echo "We do not support build SciDB under CentOS 7. Please use CentOS 6 instead"
	exit 1
	;;
    "RedHat 6")
	echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "RedHat 7")
	echo "We do not support build SciDB under RedHat 7. Please use CentOS 6 instead"
	exit 1
	;;
    "Ubuntu 14.04")
	echo "Updating apt repositories..."
	apt-get update &> /dev/null
	echo "Installing apache2..."
	apt-get install -y -q apache2
	usermod -G www-data -a ${username}
	CONFIG=/etc/apache2/sites-available/000-default.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	ln -s /public/cdash_logs /var/www/cdash_logs
	;;
    *)
	echo "Not a supported OS"
	exit 1
	;;
esac

echo "...prepared httpd"
