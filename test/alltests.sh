#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013, 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

export LANG=en_US.UTF-8
export PYTHONPATH=.:"$PYTHONPATH"
pyversions=""
if [ -e /usr/bin/python2.6 ]; then
    pyversions="$pyversions python2.6"
fi
if [ -e /usr/bin/python2.7 ]; then
    pyversions="$pyversions python2.7"
fi
option=$1
if [ "${option:0:10}" == "--python2." ]; then
    shift
    pyversions="${option:2}"
fi
echo Found Python versions: $pyversions
for pycmd in $pyversions; do
    if [ "${pycmd}" == "python2.6" -a -f /etc/debian_version ]; then
        debian_version=$(cat /etc/debian_version)
        if [ "${debian_version:0:1}" == "7" ]; then
            echo "Skipping ${pycmd} for Wheezy"
            continue
        fi
    fi
    echo "================ $t with $pycmd _alltests.py $@ ================"
    $pycmd _alltests.py "$@"
done

if [ $# -ne 0 ] ; then
    exit
fi

RUNJAVATESTS="False"
RUNJAVATESTS="True"   #DO_NOT_DISTRIBUTE
if [ "${RUNJAVATESTS}" == "False" ]; then
    exit 0
fi

BUILDDIR=../build
test -d ${BUILDDIR} && rm -rf ${BUILDDIR}
mkdir ${BUILDDIR}

JUNIT=/usr/share/java/junit4.jar
if [ ! -f ${JUNIT} ]; then
    echo "JUnit is not installed. Please install the junit4 package."
    exit 1
fi

JARS=$(find ../jars -type f -name "*.jar")
LUCENE_JARS=$(find /usr/share/java -type f -name "lucene-*-6.1.*.jar")

CP="$JUNIT:$(echo $JARS | tr ' ' ':'):$(echo $LUCENE_JARS | tr ' ' ':'):../build"

JAVA_VERSION=8
java=/usr/lib/jvm/java-1.${JAVA_VERSION}.0-openjdk.x86_64/bin/java
javac=/usr/lib/jvm/java-1.${JAVA_VERSION}.0-openjdk.x86_64/bin/javac
if [ -f /etc/debian_version ]; then
    java=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64/bin/java
    javac=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64/bin/javac
fi
javaFiles=$(find ../src -name "*.java" | grep -v JoinSort | grep -v py_analysis)
${javac} -d ${BUILDDIR} -cp $CP $javaFiles
if [ "$?" != "0" ]; then
    echo "Build failed"
    exit 1
fi

javaFiles=$(find org -name "*.java")
${javac} -d ${BUILDDIR} -cp $CP $javaFiles
if [ "$?" != "0" ]; then
    echo "Test Build failed"
    exit 1
fi

testClasses=$(find org -name "*Test.java" | sed 's,.java,,g' | tr '/' '.')
echo "Running $testClasses"
${java} -Xmx1024m -classpath ".:$CP" org.junit.runner.JUnitCore $testClasses