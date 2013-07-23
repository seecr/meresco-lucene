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

from meresco.lucene import version
from meresco.core import Observable
from meresco.components.http.utils import Ok, CRLF, ContentTypeHeader, ContentTypePlainText, serverErrorPlainText
from meresco.components.http import PathFilter, PathRename, FileServer, StringServer
from simplejson import loads
from cqlparser import parseString
from weightless.core import be, compose
from seecr.html import DynamicHtml
from traceback import format_exc

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
                indexPage='index.sf',
                additionalGlobals={
                    'VERSION': version,
                    'allCoreInfo': self._allCoreInfo,
                    'parseCql': parseString,
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
        try:
            messageDict = loads(Body)
            if messageDict['message'] not in _ALLOWED_METHODS:
                raise ValueError('Expected %s' % (' or '.join('"%s"' % m for m in _ALLOWED_METHODS)))
            messageKwargs = messageDict['kwargs']
            if 'cqlQuery' in messageKwargs:
                messageKwargs['cqlAbstractSyntaxTree'] = parseString(messageKwargs.pop('cqlQuery'))
            if 'filterQueries' in messageKwargs and messageKwargs['filterQueries'] is not None:
                messageKwargs['filterQueries'] = [parseString(cqlstring) for cqlstring in messageKwargs['filterQueries']]
            if 'joinQueries' in messageKwargs and messageKwargs['joinQueries'] is not None:
                messageKwargs['joinQueries'] = [dict(joinQuery, query=parseString(joinQuery['query'])) for joinQuery in messageKwargs['joinQueries']]
            response = yield self.any.unknown(message=messageDict['message'], **messageKwargs)
        except:
            yield serverErrorPlainText
            yield format_exc()
            return
        yield Ok
        yield ContentTypeHeader + 'application/json' + CRLF
        yield CRLF
        yield response.asJson()

    def _allCoreInfo(self):
        return list(compose(self.all.coreInfo()))

_ALLOWED_METHODS = ['executeQuery', 'prefixSearch', 'fieldnames']