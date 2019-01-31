## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os.path import join, abspath, dirname, isdir
from os import system, environ
from traceback import print_exc
from time import time

from seecr.test.integrationtestcase import IntegrationState as SeecrIntegrationState
from seecr.test.portnumbergenerator import PortNumberGenerator
from seecr.test.utils import postRequest, sleepWheel
from meresco.components.json import JsonDict


mydir = dirname(abspath(__file__))
projectDir = dirname(dirname(mydir))

class IntegrationState(SeecrIntegrationState):
    def __init__(self, stateName, tests=None, fastMode=False):
        SeecrIntegrationState.__init__(self, "meresco-lucene-" + stateName, tests=tests, fastMode=fastMode)
        self.stateName = stateName
        if not fastMode:
            system('rm -rf ' + self.integrationTempdir)
            system('mkdir --parents '+ self.integrationTempdir)
        self.suggestionServerPort = PortNumberGenerator.next()
        self.luceneServerPort = PortNumberGenerator.next()
        self.httpPort = PortNumberGenerator.next()
        self.testdataDir = join(dirname(mydir), "data")
        self.JAVA_BIN = "/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/bin"
        if not isdir(self.JAVA_BIN):
            self.JAVA_BIN = "/usr/lib/jvm/java-1.8.0/bin"

    def setUp(self):
        self.startSuggestionServer()
        self.startLuceneServer()
        self.startExampleServer()
        self._createDatabase()

    def binDir(self):
        return join(projectDir, 'bin')

    def _createDatabase(self):
        if self.fastMode:
            print "Reusing database in", self.integrationTempdir
            return
        start = time()
        print "Creating database in", self.integrationTempdir
        try:
            self._runExecutable(join(self.testdataDir, 'upload.py'), processName='IntegrationUpload', cwd=self.testdataDir, port=self.httpPort, redirect=False, timeoutInSeconds=30)
            sleepWheel(5)
            postRequest(self.luceneServerPort, "/default/settings/", data=JsonDict(commitCount=1).dumps(), parse=False)
            print "Finished creating database in %s seconds" % (time() - start)
        except Exception:
            print 'Error received while creating database for', self.stateName
            print_exc()
            exit(1)

    def startSuggestionServer(self):
        self._startServer(
                'suggestion-server',
                self.binPath('start-suggestion-server'),
                'http://localhost:{}/info'.format(self.suggestionServerPort),
                port=self.suggestionServerPort,
                stateDir=join(self.integrationTempdir, 'suggestion'),
                env=environment(JAVA_BIN=self.JAVA_BIN, LANG="en_US.UTF-8")
            )

    def startLuceneServer(self):
        self._startServer(
                'lucene-server',
                self.binPath('start-lucene-server'),
                'http://localhost:{}/info'.format(self.luceneServerPort),
                port=self.luceneServerPort,
                stateDir=join(self.integrationTempdir, 'lucene-server'),
                core=["main", "main2", "empty-core", "default"],
                env=environment(JAVA_BIN=self.JAVA_BIN, LANG="en_US.UTF-8")
            )

    def startExampleServer(self):
        self._startServer(
                'meresco-lucene',
                [self.binPath('python2.7'), 'server.py'],
                'http://localhost:%s/' % self.httpPort,
                cwd=join(mydir, 'helper'),
                port=self.httpPort,
                serverPort=self.luceneServerPort,
                autocompletePort=self.suggestionServerPort,
                stateDir=join(self.integrationTempdir, 'example-state'),
                env=environment(JAVA_BIN=self.JAVA_BIN, LANG="en_US.UTF-8")
            )

    def stopServer(self, serviceName):
        self._stopServer(serviceName)

def environment(**kwargs):
    result = dict(environ)
    result.update(**kwargs)
    return result
