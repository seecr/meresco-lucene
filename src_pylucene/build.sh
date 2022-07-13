#!/bin/bash
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Seecr (Seek You Too B.V.) https://seecr.nl
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

target=

if [ ! -z "$1" ]; then
    target="$1"
fi

if [ ! -f "../meresco-lucene.jar" ]; then
    (cd ..; ./build.sh)
fi


./seecr-build-jcc \
    --path=$(cd $(dirname $0); pwd) \
    --name=meresco-lucene \
    --package=org/meresco/lucene/py_analysis \
    --jcc=3.10 \
    --lucene=8.9.0 \
    --target=${target} \
    --java_home=/usr/lib/jvm/java-17-openjdk-amd64
