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

LUCENEVERSION=8.9.0

function show_usage {
    echo "Usage: $(basename $0)
    --target=<output path>
    --version=<meresco-lucene version>"
}

TEMP=$(getopt \
    --options "" \
    --long target::,version:: \
    -n "$0" -- "$@")

eval set -- "$TEMP"
while true
do
    case "$1" in
        --target)
            case "$2" in
                "") show_usage ; exit 1 ;;
                *) TARGET=$2 ; shift 2 ;;
            esac ;;
        --version)
            case "$2" in
                "") show_usage ; exit 1 ;;
                *) VERSION=$2 ; shift 2 ;;
            esac ;;
        --) shift ; break ;;
        *) echo "Unknown option specified." ; exit 1 ;;
    esac
done

set -o errexit

MYDIR=$(cd $(dirname $0);pwd)

if [ -z "${TARGET}" ]; then
    TARGET=${MYDIR}/lib
fi

JARS=$(find jars -type f -name "*.jar")
LUCENE_JARS=$(find /usr/share/java -type f -name "lucene-*${LUCENEVERSION}.jar")

BUILDDIR=${MYDIR}/build

test -d ${BUILDDIR} && rm -r ${BUILDDIR}
mkdir ${BUILDDIR}

JAR_FILE=meresco-lucene-${VERSION}.jar
if [ -z "${VERSION}" ]; then
    JAR_FILE=meresco-lucene.jar
fi

CP="$(echo ${JARS} | tr ' ' ':'):$(echo ${LUCENE_JARS} | tr ' ' ':')"

test -f /etc/debian_version && JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
test -f /etc/redhat-release && JAVA_HOME=/usr/lib/jvm/java

if [ -z "${JAVA_HOME}" ]; then
    echo "Unable to determine JAVA_HOME"
    exit 0
fi

if [ ! -d "${JAVA_HOME}" ]; then
    echo "${JAVA_HOME} does not exist"
    exit 0
fi

export JAVA_HOME
${JAVA_HOME}/bin/javac -d ${BUILDDIR} -cp ${CP} `find src/org -name "*.java"`
if [ "$?" != "0" ]; then
    echo "Build failed"
    exit 1
fi

jar -cf ${JAR_FILE} -C ${BUILDDIR} org

test -d "${TARGET}" && rm -rf "${TARGET}"
test -d "${TARGET}" || mkdir "${TARGET}"

python3 -m jcc.__main__ \
    --include ${MYDIR}/jars/commons-math3-3.4.1.jar \
    --include ${MYDIR}/jars/javax.json-1.0.4.jar \
    --include ${MYDIR}/jars/commons-cli-1.2.jar \
    --include ${MYDIR}/jars/commons-collections4-4.1.jar \
    --include ${MYDIR}/jars/trove-3.0.2.jar \
    --include ${MYDIR}/jars/javax.servlet-api-3.1.0.jar \
    --include ${MYDIR}/jars/jetty-all-9.2.14.v20151106.jar \
    --shared \
    --use_full_names \
    --import lucene \
    --arch x86_64 \
    --jar ${JAR_FILE} \
    --python meresco_lucene \
    --build \
    --install \
    --root "${TARGET}" 

    #--exclude org.meresco.lucene.http.PrefixSearchHandler \
    #--exclude org.meresco.lucene.http.AbstractMerescoLuceneHandler \
    #--exclude org.meresco.lucene.http.NumerateHandler \
    #--exclude org.meresco.lucene.http.ComposedQueryHandler \
    #--exclude org.meresco.lucene.http.OtherHandler \
    #--exclude org.meresco.lucene.http.QueryParameters \
    #--exclude org.meresco.lucene.http.SettingsHandler \
    #--exclude org.meresco.lucene.http.ExportKeysHandler \
    #--exclude org.meresco.lucene.http.CommitHandler \
    #--exclude org.meresco.lucene.http.LuceneHttpServer \
    #--exclude org.meresco.lucene.http.UpdateHandler \
    #--exclude org.meresco.lucene.http.DeleteHandler \
    #--exclude org.meresco.lucene.http.QueryHandler \
    #--exclude org.meresco.lucene.LuceneShutdown \
    #--exclude org.meresco.lucene.suggestion.SuggestionHandler \
    #--exclude org.meresco.lucene.suggestion.SuggestionShutdown \
    #--exclude org.meresco.lucene.suggestion.SuggestionHttpServer \
