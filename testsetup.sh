#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
#
# This file is part of "Meresco Lucene"
#
# "Meresco Lucene" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco Lucene" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco Lucene"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

set -o errexit
rm -rf tmp build
mydir=$(cd $(dirname $0); pwd)
source /usr/share/seecr-test/functions

pyversions="2.6"
if distro_is_debian_wheezy; then
    pyversions="2.6 2.7"
fi

VERSION="x.y.z"

for pyversion in $pyversions; do
    definePythonVars $pyversion
    echo "###### $pyversion, $PYTHON"
    (cd $mydir/src; ./build.sh ${SITEPACKAGES}/meresco/lucene)
    ${PYTHON} setup.py install --root tmp
done
cp -r test tmp/test
removeDoNotDistribute tmp
find tmp -name '*.py' -exec sed -r -e "
    s/\\\$Version:[^\\\$]*\\\$/\\\$Version: ${VERSION}\\\$/;
    sX^usrSharePath.*=.*XusrSharePath = '${mydir}/tmp/usr/share/meresco-lucene'X;
    " -i '{}' \;

runtests "$@"
rm -rf tmp build

