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
from meresco.lucene._lucene import IDFIELD
from org.apache.lucene.search import MatchAllDocsQuery
from org.apache.lucene.document import Document, TextField, StringField, Field

class LuceneTest(SeecrTestCase):
    def testCreate(self):
        lucene = Lucene(self.tempdir)
        result = retval(lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)

    def testAddDocument(self):
        lucene = Lucene(self.tempdir)
        document = Document()
        f = StringField(IDFIELD, "identifier", Field.Store.YES)
        document.add(f)
        lucene.addDocument(document)
        result = retval(lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)
        self.assertEquals(['identifier'], result.hits)

def retval(g):
    try:
        g.next()
    except StopIteration, e:
        return e.args[0]
