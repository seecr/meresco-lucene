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

from os.path import dirname, abspath, join, realpath
from sys import stdout

from seecr.html import DynamicHtml

from meresco.components.http import StringServer, ObservableHttpServer, BasicHttpHandler, ApacheLogger, PathFilter, PathRename, FileServer
from meresco.components.http.utils import ContentTypePlainText
from meresco.components.sru import SruRecordUpdate, SruParser, SruHandler
from meresco.components import Xml2Fields, Venturi, StorageComponent, XmlPrintLxml
from meresco.core import Observable, TransactionScope
from meresco.core.processtools import setSignalHandlers

from meresco.lucene import Lucene, Fields2LuceneDoc, CqlToLuceneQuery

from weightless.io import Reactor
from weightless.core import compose, be


myPath = abspath(dirname(__file__))
dynamicPath = join(myPath, 'html', 'dynamic')
staticPath = join(myPath, 'html', 'static')

VERSION = 'dev'

def main(reactor, port, databasePath):
    lucene = Lucene()
    storageComponent = StorageComponent(directory=join(databasePath, 'storage'))
    return \
    (Observable(),
        (ObservableHttpServer(reactor=reactor, port=port),
            (BasicHttpHandler(),
                (ApacheLogger(outputStream=stdout),
                    (PathFilter("/info", excluding=[
                            '/info/version',
                            '/info/name',
                            '/update',
                            '/sru',
                        ]),
                        (DynamicHtml(
                                [dynamicPath],
                                reactor=reactor,
                                indexPage='/info',
                                additionalGlobals={
                                    'VERSION': VERSION,
                                }
                            ),
                        )
                    ),
                    (PathFilter("/info/version"),
                        (StringServer(VERSION, ContentTypePlainText), )
                    ),
                    (PathFilter("/info/name"),
                        (StringServer('Meresco Lucene', ContentTypePlainText),)
                    ),
                    (PathFilter("/static"),
                        (PathRename(lambda path: path[len('/static'):]),
                            (FileServer(staticPath),)
                        )
                    ),
                    (PathFilter("/update"),
                        (SruRecordUpdate(),
                            (TransactionScope('record'),
                                (Venturi(should=[{'partname': 'record', 'xpath': '/*'}]),
                                    (Xml2Fields(),
                                        (Fields2LuceneDoc('record'),
                                            (lucene,)
                                        )
                                    ),
                                    (XmlPrintLxml(fromKwarg='lxmlNode', toKwarg='data'),
                                        (storageComponent,)
                                    )
                                )
                            )
                        )
                    ),
                    (PathFilter('/sru'),
                        (SruParser(defaultRecordSchema='record'),
                            (SruHandler(),
                                (CqlToLuceneQuery([]),
                                    (lucene,)
                                ),
                                (storageComponent,),
                            )
                        )
                    )
                )
            )
        )
    )




def startServer(port, stateDir):
    setSignalHandlers()
    print 'Firing up Meresco Lucene Server.'
    reactor = Reactor()
    databasePath = realpath(abspath(stateDir))

    #main
    dna = main(reactor, port=port, databasePath=databasePath)
    #/main

    server = be(dna)
    list(compose(server.once.observer_init()))

    print "Ready to rumble"
    stdout.flush()
    reactor.loop()
