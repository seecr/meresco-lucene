#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2017, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

mydir=$(cd $(dirname $0); pwd)
buildDir=$mydir/build
jarsDir=$(dirname $mydir)/jars
libDir=$1
if [ -z "$libDir" ]; then
    libDir=$(dirname $mydir)/lib
fi

PKGNAME=meresco-lucene
_PKGNAME=$(echo ${PKGNAME} | sed 's,-,_,g')

pythonVersion=$(python --version 2>&1 | awk '{print $2}' | cut -d. -f-2)
pythonPackagesDir=/usr/lib64/python${pythonVersion}/site-packages
if [ -f /etc/debian_version ]; then
    pythonPackagesDir=/usr/lib/python${pythonVersion}/dist-packages
fi

JCC_VERSION=3.6
if ! grep -q "VERSION=\"${JCC_VERSION}\"" ${pythonPackagesDir}/jcc/config.py; then
    echo "JCC ${JCC_VERSION} is required."
    exit 1
fi

JAVA_HOME=
test -f /etc/debian_version && JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
test -f /etc/redhat_version && JAVA_HOME=/usr/lib/jvm/java
if [ -z "${JAVA_HOME}" ]; then
    echo "Unable to determine JAVA_HOME"
    exit 0
fi

if [ ! -d "${JAVA_HOME}" ]; then
    echo "${JAVA_HOME} does not exist"
    exit 0
fi

export JAVA_HOME
javac=${JAVA_HOME}/bin/javac

luceneJarDir=${pythonPackagesDir}/lucene

PYLUCENEVERSION=8.1.1

classpath=${luceneJarDir}/lucene-core-${PYLUCENEVERSION}.jar:${luceneJarDir}/lucene-analyzers-common-${PYLUCENEVERSION}.jar

rm -rf ${buildDir} ${libDir}
mkdir --parents ${buildDir} ${libDir}


JARFILE=${buildDir}/${PKGNAME}.jar

${javac} -cp ${classpath} -d ${buildDir} `find . -name "*.java"`
(cd $buildDir; jar -c org > ${JARFILE})

python -m jcc.__main__ \
    --root ${mydir}/root \
    --use_full_names \
    --import lucene \
    --shared \
    --arch x86_64 \
    --jar ${JARFILE} \
    --python ${_PKGNAME} \
    --build \
    --install

rootLibDir=${mydir}/root/usr/lib64/python${pythonVersion}/site-packages/${_PKGNAME}
if [ -f /etc/debian_version ]; then
    rootLibDir=$mydir/root/usr/local/lib/python${pythonVersion}/dist-packages/${_PKGNAME}
fi

mv ${rootLibDir} ${libDir}/

rm -rf ${buildDir} ${mydir}/root ${mydir}/${_PKGNAME}.egg-info
