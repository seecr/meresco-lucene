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
from org.apache.lucene.search import MatchAllDocsQuery
from org.apache.lucene.document import Document, TextField, StringField, Field

from time import sleep

class LuceneTest(SeecrTestCase):
    def setUp(self):
        super(LuceneTest, self).setUp()
        self.lucene = Lucene(self.tempdir)

    def testCreate(self):
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)

    def testAdd1Document(self):
        retval(self.lucene.addDocument(identifier="identifier", document=Document()))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)
        self.assertEquals(['identifier'], result.hits)

    def testAddAndDeleteDocument(self):
        retval(self.lucene.addDocument(identifier="id:0", document=Document()))
        retval(self.lucene.addDocument(identifier="id:1", document=Document()))
        retval(self.lucene.addDocument(identifier="id:2", document=Document()))
        retval(self.lucene.delete(identifier="id:1"))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:2']), set(result.hits))

def retval(g):
    try:
        g.next()
    except StopIteration, e:
        if len(e.args):
            return e.args[0]
