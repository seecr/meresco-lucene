#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2012-2013, 2015-2016, 2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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
source /usr/share/seecr-tools/functions.d/test

VERSION="x.y.z"

definePythonVars
#(
#    cd $mydir
#    mkdir --parents tmp/usr/share/java/meresco-lucene
#    ./build.sh "${VERSION}"
#    mv meresco-lucene-${VERSION}.jar tmp/usr/share/java/meresco-lucene/
#)
${PYTHON} setup.py install --root tmp

cp -r test tmp/test
removeDoNotDistribute tmp

find tmp -name '*.py' -exec sed -r -e "
    s/\\\$Version:[^\\\$]*\\\$/\\\$Version: ${VERSION}\\\$/;
    sX^usrSharePath.*=.*XusrSharePath = '${mydir}/tmp/usr/share/meresco-lucene'X;
    " -i '{}' \;

export SEECRTEST_USR_SHARE="${mydir}/tmp/usr/share"
export SEECRTEST_USR_LOCAL="${mydir}/tmp/usr/local"

if [ -z "$@" ]; then
    runtests "alltests.sh integrationtest.sh"
else
    runtests "$@"
fi

rm -rf tmp build

