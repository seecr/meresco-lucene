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

from seecr.test import SeecrTestCase

from meresco.lucene import Lucene
from org.apache.lucene.search import MatchAllDocsQuery, TermQuery
from org.apache.lucene.document import Document, TextField, StringField, Field
from org.apache.lucene.index import Term
from org.apache.lucene.facet.taxonomy import CategoryPath

from time import sleep

class LuceneTest(SeecrTestCase):
    def setUp(self):
        super(LuceneTest, self).setUp()
        self.lucene = Lucene(self.tempdir)

    def tearDown(self):
        self.lucene.finish()

    def testCreate(self):
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)

    def testAdd1Document(self):
        document = Document()
        document.add(TextField('title', 'The title', Field.Store.NO))
        retval(self.lucene.addDocument(identifier="identifier", document=document))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)
        self.assertEquals(['identifier'], result.hits)
        result = retval(self.lucene.executeQuery(TermQuery(Term("title", 'title'))))
        self.assertEquals(1, result.total)
        result = retval(self.lucene.executeQuery(TermQuery(Term("title", 'the'))))
        self.assertEquals(1, result.total)

    def testAddAndDeleteDocument(self):
        retval(self.lucene.addDocument(identifier="id:0", document=Document()))
        retval(self.lucene.addDocument(identifier="id:1", document=Document()))
        retval(self.lucene.addDocument(identifier="id:2", document=Document()))
        retval(self.lucene.delete(identifier="id:1"))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:2']), set(result.hits))

    def testSorting(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([
                ('field0', 'AA'),
                ('field1', 'ZZ'), 
                ('field2', 'AA'),
            ])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([
                ('field0', 'BB'),
                ('field1', 'AA'), 
                ('field2', 'ZZ'),
            ])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([
                ('field0', 'CC'),
                ('field1', 'ZZ'), 
                ('field2', 'ZZ'),
            ])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:0', 'id:1', 'id:2'], result.hits)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:1', 'id:0'], result.hits)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field1', sortDescending=True), dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:0', 'id:1'], result.hits)

    def testStartStop(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=1, stop=10, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:1', 'id:2'], result.hits)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=0, stop=2, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(['id:0', 'id:1'], result.hits)

    def testFacets(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')]), categories=createCategories([('field2', 'first item0'), ('field3', 'second item')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')]), categories=createCategories([('field2', 'first item1'), ('field3', 'other value')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')]), categories=createCategories([('field2', 'first item2'), ('field3', 'second item')])))
        sleep(0.1)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        self.assertEquals([{
                'fieldname': 'field2',
                'terms': [
                    {'term': 'first item2', 'count': 1},
                    {'term': 'first item1', 'count': 1},
                    {'term': 'first item0', 'count': 1},
                ],
            }],result.drilldownData)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field3')]))
        self.assertEquals([{
                'fieldname': 'field3',
                'terms': [
                    {'term': 'second item', 'count': 2},
                    {'term': 'other value', 'count': 1},
                ],
            }],result.drilldownData)


def createDocument(textfields):
    document = Document()
    for name, value in textfields:
        document.add(TextField(name, value, Field.Store.NO))
    return document

def createCategories(fields):
    result = []
    for name, value in fields:
        result.append(CategoryPath([name, str(value)]))
    return result


def retval(g):
    try:
        g.next()
    except StopIteration, e:
        if len(e.args):
            return e.args[0]
