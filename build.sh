#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2020 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016, 2020 Stichting Kennisnet https://www.kennisnet.nl
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

VERSION=$1
LUCENEVERSION=8.6.0
if [ ! -z "$2" ]; then
    LUCENEVERSION=$2
fi
JAVA_HOME=
if [ ! -z "$3" ]; then
    JAVA_HOME=$3
fi

JARS=$(find jars -type f -name "*.jar")
LUCENE_JARS=$(find /usr/share/java -type f -name "lucene-*${LUCENEVERSION}.jar")

mydir=$(cd $(dirname $0);pwd)
BUILDDIR=${mydir}/build
TARGET=meresco-lucene.jar
if [ "${VERSION}" != "" ]; then
    TARGET=meresco-lucene-${VERSION}.jar
fi

test -d $BUILDDIR && rm -r $BUILDDIR
mkdir $BUILDDIR

CP="$(echo $JARS | tr ' ' ':'):$(echo $LUCENE_JARS | tr ' ' ':')"

if [ -z "${JAVA_HOME}" ]; then
    test -f /etc/debian_version && JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    test -f /etc/redhat-release && JAVA_HOME=/usr/lib/jvm/java
fi
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

${javac} -d $BUILDDIR -cp $CP `find src/org -name "*.java"`
if [ "$?" != "0" ]; then
    echo "Build failed"
    exit 1
fi

jar -cf $TARGET -C $BUILDDIR org

