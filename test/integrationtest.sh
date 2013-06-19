#!/bin/bash
set -o errexit

export LANG=en_US.UTF-8
export PYTHONPATH=.:$PYTHONPATH
python _integrationtest.py "$@"

