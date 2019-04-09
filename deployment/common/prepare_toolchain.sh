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

SCIDB_VER="${1}"

function die()
{
    echo 1>&2 "$@"
    exit 1
}

function installThem()
{
    [ "$INSTALL" != "" ] || die INSTALL not defined
    [ "$CHECK" != "" ] || die CHECK not defined
    for pkg_name in "${@}"
    do
	p=`basename $pkg_name | sed 's/-[^-]*-[^-]*\.[^.]*\.[^.]*$//'`
	echo "    Checking for $p..."
	${CHECK} "$p" 2>"${deploy_err}" 1> /dev/null
	if [ $? -eq 0 ]; then
	    echo "    $p currently installed."
	else
	    echo "    Installing $p..."
	    ${INSTALL} "$pkg_name" 2>"${deploy_err}" 1> /dev/null
	    if [ $? -eq 0 ]; then
		echo "    $p successfully installed."
	    else
		echo -e "    \033[1m\E[31;40mWARNING:\033[0m ${INSTALL} $pkg_name failed:"
		cat "${deploy_err}"
	    fi
	fi
    done
}


function ubuntu1404 ()
{
    apt-get update > /dev/null
    export DEBIAN_FRONTEND=noninteractive

    psp[0]='python-software-properties'
    installThem "${psp[@]}"
    echo "Adding apt repositories..."
    add-apt-repository ppa:ubuntu-toolchain-r/test > /dev/null
    add-apt-repository -y ppa:openjdk-r/ppa > /dev/null
    echo "Updating apt repositories..."
    apt-get update > /dev/null

    echo "Installing Compilers, etc..."
    declare -a gcc=("gcc-4.9" "g++-4.9" "gfortran-4.9" "gfortran")
    installThem "${gcc[@]}"

    # Boostrapping
    echo "Installing the tools to get started..."
    declare -a bstp=("git"
	             "expect"
		     "sudo"
		     "openssh-client"
		     "openssh-server"
		     "unzip")
    installThem "${bstp[@]}"

    # Build dependencies:
    declare -a bdeps=("build-essential"
		      "doxygen"
		      "flex" "bison"
		      "libbz2-dev" "zlib1g-dev"
	              "cmake"
                      "openssl-devel"
		      "libpq5" "libpqxx-4.0" "libpqxx-dev"
		      "libprotobuf-dev" "protobuf-compiler" "libprotobuf-java"
		      "liblog4cxx10" "liblog4cxx10-dev"
		      "libcppunit-dev"
		      "libreadline6-dev" "libreadline6"
		      "python-paramiko" "python-crypto"
		      "python-ply"
		      "xsltproc"
                      "liblapack-dev"
		      "libopenmpi-dev"
		      "swig2.0"
		      "libutfcpp-dev"
		      "debhelper"
		      "ant" "ant-contrib" "ant-optional"
		      "openjdk-8-jdk" "junit"
		      "libpam-dev"
		      "ccache"
		      "bc"
                      "lsof"
                      "librocksdb-dev"
                      "mpich2scidb" "libmpich2scidb-dev", "libscalapack-scidb-mpich2scidb-dev"
		      "scidb-${SCIDB_VER}-ant")
    installThem "${bdeps[@]}"

   # Boost package build requires:
    py[0]="python3"
    installThem "${py[@]}"

    # Scidb 3rd party packages
    echo "Installing tools and libraries built specifically for SciDB..."
    declare -a sdb=("scidb-${SCIDB_VER}-libboost1.54-all-dev"
		    "scidb-${SCIDB_VER}-cityhash"
		    "scidb-${SCIDB_VER}-ant"
		    "scidb-${SCIDB_VER}-libcsv")
    installThem "${sdb[@]}"

    # Testing:
    echo "Installing Postgres 9.3..."
    declare -a pq=("postgresql-client-9.3"
	           "postgresql-9.3"
		   "postgresql-contrib-9.3")
    installThem "${pg[@]}"

    # ScaLAPACK tests:
    echo "Installing time module..."
    t[0]="time"
    installThem "${t[@]}"
}

function centos6 ()
{
    yum clean all
    yum makecache fast

    # ...setup epel repo (libcsv is in there)
    rm -f epel-release-latest-6.noarch.rpm
    wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm
    rpm -ivh epel-release-latest-6.noarch.rpm
    rm -f epel-release-latest-6.noarch.rpm
    yum makecache fast
    echo "Installing epel repo - Extra Packages for Linux..."
    ep[0]="epel-release"
    installThem "${ep[@]}"
    yum makecache fast

    # install Postgress 9.3 bootstrap package
    echo "Installing Postgres 9.3 repo..."
    pgr[0]="https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-2.noarch.rpm"
    installThem "${pgr[@]}"

    declare -a c6=("python-argparse" "python-paramiko" "ant-nodeps")
    installThem "${c6[@]}"

    #...setup software collections for devtoolset-3
    echo "Installing devtoolset-3 repo - software collections devtoolset-3..."
    scl[0]="https://downloads.paradigm4.com/devtoolset-3/centos/6/sclo/x86_64/rh/devtoolset-3/scidb-devtoolset-3.noarch.rpm"
    installThem "${scl[@]}"

    yum makecache fast
}

function centos7 ()
{
    yum clean all
    yum makecache fast

    # ...setup epel repo (libcsv is in there)
    rm -f epel-release-latest-7.noarch.rpm
    wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    rpm -ivh epel-release-latest-7.noarch.rpm
    rm -f epel-release-latest-7.noarch.rpm
    yum makecache fast
    echo "Installing epel repo - Extra Packages for Linux..."
    ep[0]="epel-release"
    installThem "${ep[@]}"
    yum makecache fast

    # install Postgress 9.3 bootstrap package
    echo "Installing Postgres 9.3 repo..."
    pgr[0]="https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-centos93-9.3-2.noarch.rpm"
    installThem "${pgr[@]}"

    declare -a c7=("python2-paramiko")
    installThem "${c7[@]}"

    #...setup software collections for devtoolset-3
    echo "Installing devtoolset-3 repo - software collections devtoolset-3..."
    scl[0]="https://downloads.paradigm4.com/devtoolset-3/centos/7/sclo/x86_64/rh/devtoolset-3/scidb-devtoolset-3.noarch.rpm"
    installThem "${scl[@]}"

    yum makecache fast
}

function centos ()
{
    # Boostrapping
    echo "Installing the tools to get started..."
    # These are probably already installed.
    declare -a bstp=("git"
	             "expect"
		     "sudo"
		     "openssh"
		     "openssh-server")
    installThem "${bstp[@]}"

    ### Compilers
    # gcc/g++/gfort version 4.9
    echo "Installing devtoolset-3 - compilers, etc..."
    declare -a dts3=("devtoolset-3-runtime"
                     "devtoolset-3-toolchain"
                     "devtoolset-3-gdb")
    installThem "${dts3[@]}"

    # Build dependencies:
    echo "Installing tools and libraries for building SciDB..."
    declare -a bdeps=("doxygen"
		      "flex" "flex-devel" "bison"
		      "zlib-devel" "bzip2-devel"
                      "redhat-lsb-core"
		      "cmake" "make" "rpm-build"
		      "postgresql93-devel" "libpqxx-devel"
		      "python-devel"
		      "python-ply"
		      "cppunit-devel"
                      "openssl-devel"
		      "log4cxx-devel"
		      "lapack-devel" "blas-devel"
		      "protobuf-devel" "protobuf-compiler" "protobuf-java"
		      "java-1.8.0-openjdk-devel" "junit"
		      "ant" "ant-contrib" "ant-jdepend"
		      "pam-devel"
		      "swig"
		      "utf8cpp-devel"
		      "ccache"
                      "lsof"
		      "bc"
		      "openssl-devel"
                      "rocksdb-devel"
                      "mpich2scidb-devel"
                      "scalapack-scidb-mpich2scidb-devel"
		      "readline-devel" "libcsv" "libcsv-devel")
    installThem "${bdeps[@]}"

   # Scidb 3rd party packages
    echo "Installing tools and libraries built specifically for SciDB..."
    declare -a sdb=("scidb-${SCIDB_VER}-libboost-devel"
		    "scidb-${SCIDB_VER}-cityhash"
		    "scidb-${SCIDB_VER}-ant")
    installThem "${sdb[@]}"

    # Testing:
    echo "Installing Postgres 9.3..."
    declare -a pg=("postgresql93"
	           "postgresql93-server"
		   "postgresql93-contrib")
    installThem "${pg[@]}"

    # ScaLAPACK tests:
    echo "Installing time module..."
    t[0]="time"
    installThem "${t[@]}"
}

echo "Preparing SciDB build VM..."

deploy_err=$(mktemp /var/tmp/prepare_toolchain.err.XXXXXX)
trap "sudo rm -rf $deploy_err" 0 15 3 2 1


case $(./os_detect.sh) in
    "CentOS 6")
	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"
	CHECK="yum list installed"
	centos6
	centos
	;;
    "CentOS 7")
	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"
	CHECK="yum list installed"
	centos7
	centos
	;;
    "RedHat 6")
	echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "RedHat 7")
	echo "We do not support build SciDB under RedHat 7. Please use CentOS 7 instead"
	exit 1
	;;
    "Ubuntu 14.04")
	INSTALL="aptitude install -y -q"
	CHECK="dpkg -s"
	ubuntu1404
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
sudo rm -rf $deploy_err
echo "...prepared SciDB build VM"
