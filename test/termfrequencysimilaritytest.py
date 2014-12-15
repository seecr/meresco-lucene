## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import SeecrTestCase, CallTrace
from meresco.lucene import TermFrequencySimilarity, Lucene
from meresco.lucene.fieldregistry import FieldRegistry
from os.path import join
from seecr.utils.generatorutils import returnValueFromGenerator
from org.apache.lucene.document import Document, TextField, Field
from org.apache.lucene.search import TermQuery
from org.apache.lucene.index import Term

class TermFrequencySimilarityTest(SeecrTestCase):

    def testScore(self):
        reactor = CallTrace('reactor')
        lucene = Lucene(
            join(self.tempdir, 'lucene'),
            commitCount=1,
            reactor=reactor,
            similarity=TermFrequencySimilarity(),
            fieldRegistry=FieldRegistry(),
            verbose=False)
        document = Document()
        document.add(TextField('field', 'x '*100, Field.Store.NO))
        returnValueFromGenerator(lucene.addDocument(identifier="identifier", document=document))

        q = TermQuery(Term("field", 'x'))
        result = returnValueFromGenerator(lucene.executeQuery(q))
        self.assertAlmostEqual(0.1, result.hits[0].score)

        q.setBoost(10.0)
        result = returnValueFromGenerator(lucene.executeQuery(q))
        self.assertAlmostEqual(1, result.hits[0].score)
