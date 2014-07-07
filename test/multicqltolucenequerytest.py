## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from unittest import TestCase
from seecr.test import CallTrace
from cqlparser import parseString
from meresco.lucene.cqltolucenequery import CqlToLuceneQuery
from meresco.lucene.multicqltolucenequery import MultiCqlToLuceneQuery
from meresco.core import Observable
from weightless.core import be
from seecr.utils.generatorutils import consume
from meresco.lucene.composedquery import ComposedQuery

from org.apache.lucene.search import TermQuery
from org.apache.lucene.index import Term


class MultiCqlToLuceneQueryTest(TestCase):
    def setUp(self):
        coreAConverter = CqlToLuceneQuery([('fieldA', 1.0)])
        coreBConverter = CqlToLuceneQuery([('fieldB', 1.0)])
        self.converter = MultiCqlToLuceneQuery(dict(A=coreAConverter, B=coreBConverter))
        self.observer = CallTrace('Query responder', methods={'executeQuery': executeQueryMock})
        self.dna = be((Observable(),
            (self.converter,
                (self.observer,),
            )
        ))

    def testConvertComposedQuery(self):
        q = ComposedQuery('A')
        q.setCoreQuery(core='A', query=parseString('valueAQ'))
        q.setCoreQuery(core='B', query=parseString('valueBQ'))
        q.addMatch(dict(core='A', uniqueKey='keyA'), dict(core='B', key='keyB'))
        q.addUnite(dict(core='A', query=parseString('fieldUA exact valueUA')), dict(core='B', query=parseString('fieldUB exact valueUB')))
        q.validate()
        consume(self.dna.any.executeComposedQuery(query=q))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        self.assertEquals(repr(TermQuery(Term('fieldA', 'valueaq'))), repr(q.queryFor('A')))
        self.assertEquals(repr(TermQuery(Term('fieldB', 'valuebq'))), repr(q.queryFor('B')))


def executeQueryMock(luceneQuery, *args, **kwargs):
    return
    yield
