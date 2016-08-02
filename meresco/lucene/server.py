## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from weightless.http import HttpRequest1_1, SocketPool
from weightless.io import Reactor
from weightless.core import compose, be
from meresco.core import Observable, TransactionScope
from meresco.core.processtools import setSignalHandlers
from meresco.components import Xml2Fields, Venturi, XmlPrintLxml, FilterField, RenameField, FilterMessages, TransformFieldValue
from meresco.components.http import StringServer, ObservableHttpServer, BasicHttpHandler, ApacheLogger, PathFilter, PathRename, FileServer
from meresco.components.http.utils import ContentTypePlainText
from meresco.components.sru import SruRecordUpdate, SruParser, SruHandler, SruDuplicateCount
from meresco.components.drilldown import SRUTermDrilldown
from meresco.components.autocomplete import Autocomplete
from meresco.html import DynamicHtml
from meresco.xml import namespaces
from meresco.sequentialstore import MultiSequentialStorage, StorageComponentAdapter

from meresco.lucene import Lucene, Fields2LuceneDoc, SORTED_PREFIX, UNTOKENIZED_PREFIX, version, MultiLucene, DrilldownField, LuceneSettings
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict
from meresco.lucene.remote import LuceneRemoteService, LuceneRemote
from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene.adaptertolucenequery import AdapterToLuceneQuery
from meresco.lucene.suggestionindexcomponent import SuggestionIndexComponent

from org.meresco.lucene.py_analysis import MerescoDutchStemmingAnalyzer


myPath = abspath(dirname(__file__))
dynamicPath = join(myPath, 'html', 'dynamic')
staticPath = join(myPath, 'html', 'static')

def uploadHelix(lucene, storageComponent, drilldownFields, fieldRegistry):
    indexHelix = (Fields2LuceneDoc('record', fieldRegistry=fieldRegistry),
        (lucene,)
    )

    return \
    (SruRecordUpdate(),
        (TransactionScope('record'),
            (Venturi(should=[{'partname': 'record', 'xpath': '.'}], namespaces={'doc': 'http://meresco.org/namespace/example'}),
                (FilterMessages(allowed=['delete']),
                    (lucene,),
                    (storageComponent,)
                ),
                (FilterMessages(allowed=['add']),
                    (Xml2Fields(),
                        (RenameField(lambda name: name.split('.', 1)[-1]),
                            (FilterField(lambda name: name.startswith('__key__')),
                                (TransformFieldValue(lambda value: int(value)),
                                    indexHelix,
                                ),
                            ),
                            (FilterField(lambda name: 'fieldHier' not in name and not name.startswith('__key__')),
                                indexHelix,
                            ),
                            (FilterField(lambda name: name == 'intfield1'),
                                (RenameField(lambda name: SORTED_PREFIX + name),
                                    indexHelix,
                                )
                            ),
                            (FilterField(lambda name: name in ['field2', 'field4']),
                                (RenameField(lambda name: SORTED_PREFIX + name),
                                    indexHelix,
                                )
                            ),
                            (FilterField(lambda name: name in ['field2', 'field3']),
                                (RenameField(lambda name: UNTOKENIZED_PREFIX + name),
                                    indexHelix,
                                )
                            ),
                            (FilterField(lambda name: name == 'field2'),
                                (RenameField(lambda name: UNTOKENIZED_PREFIX + name + '.copy'),
                                    indexHelix,
                                )
                            ),
                        )
                    ),
                    (FieldHier(),
                        indexHelix,
                    )
                ),
                (XmlPrintLxml(fromKwarg='lxmlNode', toKwarg='data'),
                    (storageComponent,)
                )
            )
        )
    )

def main(reactor, port, serverPort, autocompletePort, databasePath, **kwargs):
    drilldownFields = [
        DrilldownField('untokenized.field2'),
        DrilldownField('untokenized.field2.copy', indexFieldName='copy'),
        DrilldownField('untokenized.fieldHier', hierarchical=True)
    ]

    fieldRegistry = FieldRegistry(drilldownFields)
    luceneSettings = LuceneSettings(
                fieldRegistry=fieldRegistry,
                commitCount=30,
                commitTimeout=1,
                analyzer=MerescoDutchStemmingAnalyzer(["field4", "field5"]),
                _analyzer=dict(type="MerescoDutchStemmingAnalyzer", fields=['field4', 'field5'])
            )

    http11_request = be((HttpRequest1_1(),
        (SocketPool(reactor=reactor, unusedTimeout=5, limits=dict(totalSize=100, destinationSize=10)),)
    ))
    lucene = be((Lucene(host="localhost", port=serverPort, name='main', settings=luceneSettings),
            (http11_request,)
        ))

    lucene2Settings = LuceneSettings(fieldRegistry=fieldRegistry, commitTimeout=0.1)
    lucene2 = be((Lucene(host="localhost", port=serverPort, name='main2', settings=lucene2Settings),
            (http11_request,)
        ))

    emptyLuceneSettings = LuceneSettings(commitTimeout=1)
    multiLuceneHelix = (MultiLucene(host='localhost', port=serverPort, defaultCore='main'),
            (Lucene(host='localhost', port=serverPort, name='empty-core', settings=emptyLuceneSettings),
                (http11_request,)
            ),
            (lucene,),
            (lucene2,),
            (http11_request,)
        )
    storageComponent = be(
        (StorageComponentAdapter(),
            (MultiSequentialStorage(directory=join(databasePath, 'storage')),)
        )
    )

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
                            '/remote',
                            '/via-remote-sru',
                        ]),
                        (DynamicHtml(
                                [dynamicPath],
                                reactor=reactor,
                                indexPage='/info',
                                additionalGlobals={
                                    'VERSION': version,
                                }
                            ),
                        )
                    ),
                    (PathFilter("/info/version"),
                        (StringServer(version, ContentTypePlainText), )
                    ),
                    (PathFilter("/info/name"),
                        (StringServer('Meresco Lucene', ContentTypePlainText),)
                    ),
                    (PathFilter("/static"),
                        (PathRename(lambda path: path[len('/static'):]),
                            (FileServer(staticPath),)
                        )
                    ),
                    (PathFilter("/update_main", excluding=['/update_main2']),
                        uploadHelix(lucene, storageComponent, drilldownFields, fieldRegistry=luceneSettings.fieldRegistry),
                    ),
                    (PathFilter("/update_main2"),
                        uploadHelix(lucene2, storageComponent, drilldownFields, fieldRegistry=lucene2Settings.fieldRegistry),
                    ),
                    (PathFilter('/sru'),
                        (SruParser(defaultRecordSchema='record'),
                            (SruHandler(),
                                (AdapterToLuceneQuery(
                                    defaultCore='main',
                                    coreConverters={
                                        "main": QueryExpressionToLuceneQueryDict([], luceneSettings=luceneSettings),
                                        "main2": QueryExpressionToLuceneQueryDict([], luceneSettings=lucene2Settings),
                                        "empty-core": QueryExpressionToLuceneQueryDict([], luceneSettings=emptyLuceneSettings),
                                    }),
                                    multiLuceneHelix,
                                ),
                                (SRUTermDrilldown(defaultFormat='xml'),),
                                (SruDuplicateCount(),),
                                (storageComponent,),
                            )
                        )
                    ),
                    (PathFilter('/via-remote-sru'),
                        (SruParser(defaultRecordSchema='record'),
                            (SruHandler(),
                                (LuceneRemote(host='localhost', port=port, path='/remote'),),
                                (SRUTermDrilldown(defaultFormat='xml'),),
                                (SruDuplicateCount(),),
                                (storageComponent,),
                            )
                        )
                    ),
                    (PathFilter('/remote'),
                        (LuceneRemoteService(reactor=reactor),
                            (AdapterToLuceneQuery(
                                    defaultCore='main',
                                    coreConverters={
                                        "main": QueryExpressionToLuceneQueryDict([], luceneSettings=luceneSettings),
                                        "main2": QueryExpressionToLuceneQueryDict([], luceneSettings=lucene2Settings),
                                        "empty-core": QueryExpressionToLuceneQueryDict([], luceneSettings=emptyLuceneSettings),
                                    }),
                                multiLuceneHelix,
                            )
                        )
                    ),
                    (PathFilter('/autocomplete'),
                        (Autocomplete(host='localhost', port=port, path='/autocomplete', defaultField='__all__', templateQuery='?', defaultLimit=5, shortname='?', description='?'),
                            (lucene,),
                        )
                    ),
                    (PathFilter('/suggestion'),
                        (SuggestionIndexComponent(host='localhost', port=autocompletePort),
                            (http11_request,),
                        )
                    )
                )
            )
        )
    )


class FieldHier(Observable):
    ns = namespaces.copyUpdate(dict(x='http://meresco.org/namespace/example'))
    def add(self, lxmlNode, **kwargs):
        hierarchicalFields = self.ns.xpath(lxmlNode, '/x:document/x:fieldHier')
        for field in hierarchicalFields:
            values = self.ns.xpath(field, 'x:value/text()')
            self.do.addField(name=UNTOKENIZED_PREFIX+'fieldHier', value=values)
        return
        yield

def startServer(stateDir, **kwargs):
    setSignalHandlers()
    print 'Firing up Meresco Lucene Server.'
    reactor = Reactor()
    databasePath = realpath(abspath(stateDir))

    #main
    dna = main(reactor, databasePath=databasePath, **kwargs)
    #/main

    server = be(dna)
    list(compose(server.once.observer_init()))

    print "Ready to rumble"
    stdout.flush()
    reactor.loop()
