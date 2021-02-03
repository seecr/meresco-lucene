#!/bin/bash

target=

if [ ! -z "$1" ]; then
    target="$1"
fi

seecr-build-jcc \
    --path=$(cd $(dirname $0); pwd) \
    --name=meresco-lucene \
    --package=org/meresco/lucene/py_analysis \
    --jcc=3.8 \
    --lucene=8.6.1 \
    --target=${target}
