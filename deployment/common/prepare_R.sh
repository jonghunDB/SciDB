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
#################################################################################################
#
# The following functions for installing SciDB R on the various packages are here for FUTURE use.
#
# We presently do not have any tests that use SciDB R nor R.
#################################################################################################
function ubuntu1404_R ()
{
echo "Installing R and SciDB-R"

INSTALL="apt-get install -y -q"

# Install R
# ...but need most recent (3.01) so add Cran to repo list
echo 'deb http://watson.nci.nih.gov/cran_mirror/bin/linux/ubuntu trusty/' >> /etc/apt/sources.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
echo "Updating apt repositories..."
apt-get update > /dev/null
# ...now install R
echo "Installing R..."
${INSTALL} r-base

# Install SciDB R package
# ...SciDB-R needs curl
${INSTALL} libcurl4-gnutls-dev
# ...now install SciDB R package
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}
function centos_R ()
{
echo "Installing R and SciDB-R"

# Install R
${INSTALL} R

# Install SciDB R package
# ...and install make needed by cran.r
${INSTALL} make
# ...and install curl-devel for Rcurl
${INSTALL} libcurl-devel
# ...now install SciDB R
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}
function redhat_R ()
{
echo "Installing R and SciDB-R"

# The following are needed by R but are not available in RedHat
${INSTALL} /public/software/texinfo-tex-4.13a-8.el6.x86_64.rpm
${INSTALL} /public/software/libjpeg-turbo-1.2.1-1.el6.x86_64.rpm
${INSTALL} /public/software/blas-devel-3.2.1-4.el6.x86_64.rpm
${INSTALL} /public/software/lapack-devel-3.2.1-4.el6.x86_64.rpm
${INSTALL} /public/software/libicu-devel-4.2.1-9.1.el6_2.x86_64.rpm

# Install R
${INSTALL} R

# Install SciDB R package
# ...and install make needed by cran.r
${INSTALL} make
# ...and install curl-devel for Rcurl
${INSTALL} libcurl-devel
# ...now install SciDB R
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}
