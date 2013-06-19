#!/bin/bash
export LANG=en_US.UTF-8
export PYTHONPATH=.:"$PYTHONPATH"
pyversions="python2.6"
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
    echo "================ $t with $pycmd _alltests.py $@ ================"
    $pycmd _alltests.py "$@"
done

