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

function die()
{
    echo 1>&2 "$@"
    exit 1
}

function chroot_sudoers_mock ()
{
    CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
    echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
    echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
    echo "${username} ALL = NOPASSWD:/usr/sbin/mock, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
    chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}

function centos ()
{
    yum makecache fast
    yum install --enablerepo=scidb3rdparty -y gcc make rpm-build mock python-argparse
    chroot_sudoers_mock
}

function chroot_sudoers_pbuilder ()
{
    CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
    echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
    echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
    echo "${username} ALL = NOPASSWD:/usr/sbin/pbuilder, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
    chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}
#
# Need to add apt-get install apt-transport-https
# before the othermirror (https://downloads.paradigm4.com)
# is loaded.
#
# This creates a pbuilder --create hook that loads the https transport into apt-get.
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
# From the man page:
#   G<digit><digit><whatever-else-you-want> is executed just after
#   debootstrap  finishes, and configuration is loaded, and pbuilder
#   starts mounting /proc and invoking apt-get install in --create target.
#
function pbuilder_apt-transport-https ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y apt-transport-https" >> /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y ca-certificates" >> /var/cache/pbuilder/hook.d/G01https
    chmod 555 /var/cache/pbuilder/hook.d/G01https
}
#
# Need to run apt-get update before trying to satisfy build-dependency
#
# This creates a pbuilder --build hook that runs "apt-get update"
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
# From the man page:
#   D<digit><digit><whatever-else-you-want> is executed before unpacking the source
#   inside  the chroot, after setting up the chroot environment.
#   This is called before build-dependency is satisfied.
#   Also useful for calling apt-get update
#
function pbuilder_apt-get-update ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/D01apt-get-update
    echo "apt-get update" >> /var/cache/pbuilder/hook.d/D01apt-get-update
    chmod 555 /var/cache/pbuilder/hook.d/D01apt-get-update
}

function ubuntu1404 ()
{
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
    echo "Installing packages for chroot-ing..."
    apt-get install -y -q build-essential dpkg-dev pbuilder debhelper m4 cdbs quilt apt-transport-https
    chroot_sudoers_pbuilder
    pbuilder_apt-transport-https
    pbuilder_apt-get-update
}

case $(./os_detect.sh) in
    "CentOS 6" | "CentOS 7")
	centos
	;;
    "RedHat 6")
	echo "We do not support building SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "RedHat 7")
	echo "We do not support building SciDB under RedHat 7. Please use CentOS 6 instead"
	exit 1
	;;
    "Ubuntu 14.04")
	ubuntu1404
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
