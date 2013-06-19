from os.path import join, abspath, dirname
from os import system
from traceback import print_exc
from time import time

from seecr.test.integrationtestcase import IntegrationState as SeecrIntegrationState
from seecr.test.portnumbergenerator import PortNumberGenerator


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

    def setUp(self):
        self._startServer()
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
            print "Finished creating database in %s seconds" % (time() - start)
        except Exception:
            print 'Error received while creating database for', self.stateName
            print_exc()
            exit(1)

    def _startServer(self):
        self._startServer('meresco-lucene', self.binPath('start-server'), 'http://localhost:%s/' % self.httpPort, httpPort=self.httpPort)
