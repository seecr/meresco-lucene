## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os.path import join, abspath, basename, dirname
from os import system, listdir
from traceback import print_exc
from time import time
from lxml.etree import parse, tostring
from StringIO import StringIO

from seecr.test.integrationtestcase import IntegrationState as SeecrIntegrationState
from seecr.test.portnumbergenerator import PortNumberGenerator
from seecr.test.utils import postRequest


mydir = dirname(abspath(__file__))
projectDir = dirname(dirname(mydir))

class IntegrationState(SeecrIntegrationState):
    def __init__(self, stateName, tests=None, fastMode=False):
        SeecrIntegrationState.__init__(self, "meresco-lucene-" + stateName, tests=tests, fastMode=fastMode)
        self.stateName = stateName
        if not fastMode:
            system('rm -rf ' + self.integrationTempdir)
            system('mkdir --parents '+ self.integrationTempdir)
        self.httpPort = PortNumberGenerator.next()
        self.testdataDir = join(dirname(mydir), "data")

    def setUp(self):
        self.startServer()
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
            uploadUpdateRequests(self.testdataDir, '/update', self.httpPort)
            print "Finished creating database in %s seconds" % (time() - start)
        except Exception:
            print 'Error received while creating database for', self.stateName
            print_exc()
            exit(1)

    def startServer(self):
        self._startServer('meresco-lucene', self.binPath('start-server'), 'http://localhost:%s/' % self.httpPort, port=self.httpPort, stateDir=join(self.integrationTempdir, 'state'))

#uploadHellpers
def uploadUpdateRequests(datadir, uploadPath, uploadPort):
    requests = (join(datadir, r) for r in sorted(listdir(datadir)) if r.endswith('.updateRequest'))
    for filename in requests:
        _uploadUpdateRequest(filename, uploadPath, uploadPort)

def _uploadUpdateRequest(filename, uploadPath, uploadPort):
    print 'http://localhost:%s%s' % (uploadPort, uploadPath), '<-', basename(filename)[:-len('.updateRequest')]
    updateRequest = open(filename).read()
    parse(StringIO(updateRequest))
    header, body = postRequest(uploadPort, uploadPath, updateRequest)
    if '200 Ok' not in header:
        print 'No 200 Ok response, but:\n', header
        exit(123)
    if "srw:diagnostics" in tostring(body):
        print tostring(body)
        exit(1234)