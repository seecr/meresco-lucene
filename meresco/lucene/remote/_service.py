
from meresco.lucene import version
from meresco.core import Observable
from meresco.components.http.utils import Ok, CRLF, ContentTypeHeader, ContentTypePlainText
from meresco.components.http import PathFilter, PathRename, FileServer, StringServer
from simplejson import loads
from cqlparser import parseString
from weightless.core import be
from seecr.html import DynamicHtml

from os.path import join, dirname, abspath
myPath = dirname(abspath(__file__))
usrSharePath = '/usr/share/meresco-lucene'
usrSharePath = join(dirname(dirname(dirname(myPath))), 'usr-share')  #DO_NOT_DISTRIBUTE
staticPath = join(usrSharePath, 'lucene-remote', 'static')
dynamicPath = join(myPath, 'dynamic')

class LuceneRemoteService(Observable):
    def __init__(self, reactor, **kwargs):
        Observable.__init__(self, **kwargs)
        self._dynamicHtml = DynamicHtml([dynamicPath],
                reactor=reactor,
                indexPage='/index.sf',
                additionalGlobals={
                    'VERSION': version,
                    'allCoreInfo': self._allCoreInfo,
                }
            )
        self._internalTree = be((Observable(),
            (PathFilter('/', excluding=['/static', '/version']),
                (self._dynamicHtml,)
            ),
            (PathFilter('/static'),
                (PathRename(lambda path: path[len('/static'):]),
                    (FileServer(staticPath),)
                )
            ),
            (PathFilter('/version'),
                (StringServer("Meresco Lucene version %s" % version, ContentTypePlainText),)
            ),
        ))

    def addObserver(self, *args, **kwargs):
        Observable.addObserver(self, *args, **kwargs)
        self._dynamicHtml.addObserver(*args, **kwargs)

    def addStrand(self, *args, **kwargs):
        Observable.addStrand(self, *args, **kwargs)
        self._dynamicHtml.addStrand(*args, **kwargs)

    def handleRequest(self, path, Method, Body=None, **kwargs):
        if Method == 'POST' and path.endswith('/__lucene_remote__'):
            yield self._handleQuery(Body)
        elif '/info' in path:
            _, _, path = path.partition('/info')
            yield self._internalTree.all.handleRequest(path=path, Method=Method, Body=Body, **kwargs)

    def _handleQuery(self, Body):
        messageDict = loads(Body)
        if messageDict['message'] not in ['executeQuery', 'prefixSearch']:
            raise ValueError('Expected "executeQuery" or "prefixSearch"')
        messageKwargs = messageDict['kwargs']
        if 'cqlQuery' in messageKwargs:
            messageKwargs['cqlAbstractSyntaxTree'] = parseString(messageKwargs.pop('cqlQuery'))
        if 'filterQueries' in messageKwargs and messageKwargs['filterQueries'] is not None:
            messageKwargs['filterQueries'] = [parseString(cqlstring) for cqlstring in messageKwargs['filterQueries']]
        if 'joinQueries' in messageKwargs and messageKwargs['joinQueries'] is not None:
            messageKwargs['joinQueries'] = [dict(joinQuery, query=parseString(joinQuery['query'])) for joinQuery in messageKwargs['joinQueries']]
        response = yield self.any.unknown(message=messageDict['message'], **messageKwargs)
        yield Ok
        yield ContentTypeHeader + 'application/json' + CRLF
        yield CRLF
        yield response.asJson()

    def _allCoreInfo(self):
        infoContainer = []
        self.do.retrieveInfo(infoContainer=infoContainer)
        return infoContainer
