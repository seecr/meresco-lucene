#!/usr/bin/env python

from sys import argv

from seecr.test.testrunner import TestRunner
from _integration.integrationstate import IntegrationState

flags = ['--fast']

if __name__ == '__main__':
    fastMode = '--fast' in argv
    for flag in flags:
        if flag in argv:
            argv.remove(flag)

    runner = TestRunner()
    IntegrationState(
        'default',
        tests=[],
        fastMode=fastMode).addToTestRunner(runner)

    testnames = argv[1:]
    runner.run(testnames)

