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

set -eu

username=${1}
password="${2}"
network=${3}

if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: configure_postgresql.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

function postgresql_sudoers ()
{
    POSTGRESQL_SUDOERS=/etc/sudoers.d/postgresql
    echo "Defaults:${username} !requiretty" > ${POSTGRESQL_SUDOERS}
    echo "${username} ALL =(postgres) NOPASSWD: ALL" >> ${POSTGRESQL_SUDOERS}
    chmod 0440 ${POSTGRESQL_SUDOERS}
}

function rhcommon6()
{
    yum install -y postgresql93 postgresql93-server postgresql93-contrib expect || \
    yum update -y postgresql93 postgresql93-server postgresql93-contrib expect
    /sbin/chkconfig postgresql-9.3 on
    su -l postgres -c "/usr/pgsql-9.3/bin/pg_ctl initdb"
    restart="service postgresql-9.3 restart"
    status="service postgresql-9.3 status"
}

function centos6()
{
    # install Postgress 9.3 bootstrap package
    yum makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-2.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-2.noarch.rpm
    rhcommon6
}
function centos7()
{
    # install Postgress 9.3 bootstrap package
    yum makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-centos93-9.3-2.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-centos93-9.3-2.noarch.rpm
    rhcommon6
}
function redhat6()
{
    # install Postgress 9.3 bootstrap package
    yum makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-2.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-2.noarch.rpm
    rhcommon6
}
function redhat7()
{
    # install Postgress 9.3 bootstrap package
    yum makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-redhat93-9.3-2.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-redhat93-9.3-2.noarch.rpm
    rhcommon6
}
function ubuntu1404()
{
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
    echo "Installing postgres packages..."
    apt-get install -y -q python-paramiko python-crypto postgresql-9.3 postgresql-contrib-9.3 expect
    restart="/etc/init.d/postgresql restart"
    status="/etc/init.d/postgresql status"
}

OS=$(./os_detect.sh)
case ${OS} in
    "CentOS 6"|"CentOS 7")
	centos6
	;;
    "CentOS 7")
	centos7
	;;
    "RedHat 6")
	redhat6
	;;
    "RedHat 7")
	redhat7
	;;
    "Ubuntu 14.04")
	ubuntu1404
	;;
    *)
	echo "Not a supported OS";
	exit 1
esac;

postgresql_sudoers
./configure_postgresql.py "${OS}" "${username}" "${password}" "${network}" || echo "WARNING: failed to configure postgres !"
${restart}
${status}
