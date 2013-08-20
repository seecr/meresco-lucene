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

from seecr.test import IntegrationTestCase
from meresco.lucene import Fields2LuceneDoc
from meresco.lucene._lucene import IDFIELD

class Fields2LuceneDocTest(IntegrationTestCase):
    def testCreateDocument(self):
        fields = {
            'field1': ['value1'],
            'field2': ['value2', 'value2.1'],
            'sorted.field3': ['value3'],
            'untokenized.field4': ['value4'],
        }
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFieldnames=[])
        document = fields2LuceneDoc._createDocument(fields)
        self.assertEquals(set(['field1', 'field2', 'sorted.field3', 'untokenized.field4']), set([f.name() for f in document.getFields()]))

        field1 = document.getField("field1")
        self.assertEquals('value1', field1.stringValue())
        self.assertTrue(field1.fieldType().indexed())
        self.assertFalse(field1.fieldType().stored())
        self.assertTrue(field1.fieldType().tokenized())

        self.assertEquals(['value2', 'value2.1'], document.getValues('field2'))

        field3 = document.getField("sorted.field3")
        self.assertEquals('value3', field3.stringValue())
        self.assertTrue(field3.fieldType().indexed())
        self.assertFalse(field3.fieldType().stored())
        self.assertFalse(field3.fieldType().tokenized())

        field4 = document.getField("untokenized.field4")
        self.assertEquals('value4', field4.stringValue())
        self.assertTrue(field4.fieldType().indexed())
        self.assertFalse(field4.fieldType().stored())
        self.assertFalse(field4.fieldType().tokenized())

    def testCreateFacet(self):
        fields = {
            'field1': ['value1'],
            'sorted.field3': ['value3'],
            'untokenized.field4': ['value4'],
            'untokenized.field5': ['value5', 'value6'],
            'untokenized.field6': ['value5/value6'],
            'untokenized.field7': ['valuex']
        }
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFieldnames=['untokenized.field4', 'untokenized.field5', 'untokenized.field6'])
        categories = fields2LuceneDoc._createFacetCategories(fields)
        self.assertEquals(4, len(categories))
        self.assertEquals(set([
                ('untokenized.field5', 'value5'),
                ('untokenized.field5', 'value6'),
                ('untokenized.field4', 'value4'),
                ('untokenized.field6', 'value5/value6'),
            ]), set(tuple(c.components) for c in categories))

    def testAddTimeStamp(self):
        fields = {'field1': ['value1']}
        fields2LuceneDoc = Fields2LuceneDoc('tsname', addTimestamp=True, drilldownFieldnames=[])
        fields2LuceneDoc._time = lambda: 123456789
        document = fields2LuceneDoc._createDocument(fields)
        self.assertEquals(set(['field1', '__timestamp__']), set([f.name() for f in document.getFields()]))
        timestampField = document.getField("__timestamp__")
        self.assertEquals(123456789, timestampField.numericValue().intValue())
        self.assertTrue(timestampField.fieldType().indexed())
        self.assertFalse(timestampField.fieldType().stored())
        self.assertTrue(timestampField.fieldType().tokenized())

# TODO
#
# - copy isSingleValuedField from meresco.solr
# - add more prefixes for special fields
# - add more tests for transaction like stuff
