## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2023 Seecr (Seek You Too B.V.) https://seecr.nl
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

import unittest

from meresco_lucene import JArray

from org.apache.lucene.document import Document
from org.apache.lucene.document import Field
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute
from org.apache.lucene.facet import FacetField

from org.meresco.lucene.analysis import DocumentUtil


class DocumentUtilTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.doc = Document()
        self.fieldnames = JArray('string')(('base', 'middle', 'top'))
        self.analyzer = StandardAnalyzer()

    def testAddStringField(self):
        DocumentUtil.add_StringFields(self.doc, self.fieldnames, 2, '.tag', 'analyse value', Field.Store.NO, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'middle.tag', 'top.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse value'], tokens(fields[0]))
        self.assertEqual(['analyse value'], tokens(fields[1]))

    def testAddStringFieldPartly(self):
        DocumentUtil.add_StringFields(self.doc, self.fieldnames, 1, '.tag', 'analyse value', Field.Store.NO, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'middle.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse value'], tokens(fields[0]))
        self.assertEqual(['analyse value'], tokens(fields[1]))
        self.assertFalse(fields[0].fieldType().stored())

    def testAddStringFieldStore(self):
        DocumentUtil.add_StringFields(self.doc, self.fieldnames, 0, '.tag', 'analyse value', Field.Store.YES, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse value'], tokens(fields[0]))
        self.assertTrue(fields[0].fieldType().stored())

    def testAddStringFieldFacets(self):
        DocumentUtil.add_StringFields(self.doc, self.fieldnames, 0, '.tag', 'analyse value', Field.Store.NO, True)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'dummy'], [f.name() for f in fields])
        self.assertEqual(['analyse value'], tokens(fields[0]))
        facet = FacetField.cast_(fields[1])
        self.assertEqual('base.tag.facet', facet.dim)
        self.assertEqual(['analyse value'], list(facet.path))

    def testAddStringFieldFacetsTopFieldOnly(self):
        DocumentUtil.add_StringFields(self.doc, self.fieldnames, 0, '.tag', 'analyse value', Field.Store.NO, True)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'dummy'], [f.name() for f in fields])
        self.assertEqual(['analyse value'], tokens(fields[0]))
        facet = FacetField.cast_(fields[1])
        self.assertEqual('base.tag.facet', facet.dim)
        self.assertEqual(['analyse value'], list(facet.path))

    def testAddTextField(self):
        DocumentUtil.add_TextFields(self.doc, self.fieldnames, 2, '.tag', 'analyse value', Field.Store.NO, 10, self.analyzer, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'middle.tag', 'top.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse', 'value'], tokens(fields[0]))
        self.assertEqual(['analyse', 'value'], tokens(fields[1]))

    def testAddTextFieldPartly(self):
        DocumentUtil.add_TextFields(self.doc, self.fieldnames, 1, '.tag', 'analyse value', Field.Store.NO, 10, self.analyzer, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'middle.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse', 'value'], tokens(fields[0]))
        self.assertEqual(['analyse', 'value'], tokens(fields[1]))
        self.assertFalse(fields[0].fieldType().stored())

    def testAddTextFieldStore(self):
        DocumentUtil.add_TextFields(self.doc, self.fieldnames, 0, '.tag', 'analyse value', Field.Store.YES, 10, self.analyzer, False)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag'], [f.name() for f in fields])
        self.assertEqual(['analyse', 'value'], tokens(fields[0]))
        self.assertTrue(fields[0].fieldType().stored())

    def testAddTextFieldFacets(self):
        DocumentUtil.add_TextFields(self.doc, self.fieldnames, 0, '.tag', 'analyse value', Field.Store.NO, 10, self.analyzer, True)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'dummy'], [f.name() for f in fields])
        self.assertEqual(['analyse', 'value'], tokens(fields[0]))
        facet = FacetField.cast_(fields[1])
        self.assertEqual('base.tag.facet', facet.dim)
        self.assertEqual(['analyse value'], list(facet.path))

    def testAddTextFieldFacetsTopFieldOnly(self):
        DocumentUtil.add_TextFields(self.doc, self.fieldnames, 2, '.tag', 'analyse value', Field.Store.NO, 10, self.analyzer, True)
        fields = [f for f in self.doc.getFields()]
        self.assertEqual(['base.tag', 'middle.tag', 'top.tag', 'dummy'], [f.name() for f in fields])
        self.assertEqual(['analyse', 'value'], tokens(fields[0]))
        facet = FacetField.cast_(fields[-1])
        self.assertEqual('top.tag.facet', facet.dim)
        self.assertEqual(['analyse value'], list(facet.path))


def tokens(field):
    analyzer = StandardAnalyzer()
    token_stream = field.tokenStream(analyzer, None)
    term_att = token_stream.addAttribute(CharTermAttribute.class_)
    token_stream.reset()
    r = []
    while token_stream.incrementToken():
        r.append(term_att.toString())
    token_stream.end()
    token_stream.close()
    return r
