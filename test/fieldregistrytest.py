## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2016, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018 Stichting Kennisnet https://www.kennisnet.nl
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
from meresco.lucene.fieldregistry import FieldRegistry, STRINGFIELD_STORED, NO_TERMS_FREQUENCY_FIELD, STRINGFIELD, TEXTFIELD, LONGFIELD, INTFIELD, NUMERICFIELD, JAVA_MAX_INT, JAVA_MIN_INT, JAVA_MAX_LONG, JAVA_MIN_LONG
from meresco.lucene import DrilldownField
import warnings


class FieldRegistryTest(SeecrTestCase):
    def testDefault(self):
        registry = FieldRegistry()
        field = registry.createField('__id__', 'id:1')
        self.assertEquals({
                "type": "StringField",
                "name": "__id__",
                "value": "id:1",
                "stored": True
            }, field)

    def testSpecificField(self):
        registry = FieldRegistry()
        field = registry.createField('fieldname', 'value')
        self.assertEquals({
                "type": "TextField",
                "name": "fieldname",
                "value": "value",
            }, field)
        registry.register('fieldname', STRINGFIELD_STORED)
        field = registry.createField('fieldname', 'value')
        self.assertEquals({
                "type": "StringField",
                "name": "fieldname",
                "value": "value",
                "stored": True
            }, field)

    def testNoTermsFreqField(self):
        registry = FieldRegistry()
        registry.register('fieldname', NO_TERMS_FREQUENCY_FIELD)
        field = registry.createField('fieldname', 'value')
        self.assertEquals({
                "type": "NoTermsFrequencyField",
                "name": "fieldname",
                "value": "value",
            }, field)

    def testNumericField(self):
        registry = FieldRegistry()
        registry.register('fieldname', NUMERICFIELD)
        field = registry.createField('fieldname', 2010)
        self.assertEquals({
                "type": "NumericField",
                "name": "fieldname",
                "value": 2010,
            }, field)

    def testPhraseQueryPossible(self):
        registry = FieldRegistry()
        registry.register('fieldname', NO_TERMS_FREQUENCY_FIELD)
        self.assertFalse(registry.phraseQueryPossible('fieldname'))
        self.assertTrue(registry.phraseQueryPossible('other.fieldname'))

    def testIsUntokenized(self):
        registry = FieldRegistry(drilldownFields=[DrilldownField('aDrilldownField')])
        self.assertTrue(registry.isUntokenized('aDrilldownField'))
        self.assertTrue(registry.isUntokenized('untokenized.some.field'))
        self.assertFalse(registry.isUntokenized('other.field'))
        registry.register('fieldname', STRINGFIELD)
        self.assertTrue(registry.isUntokenized('fieldname'))
        registry.register('fieldname', TEXTFIELD)
        self.assertFalse(registry.isUntokenized('fieldname'))

    def testDrilldownFields(self):
        drilldownFields = [DrilldownField(name='aap'), DrilldownField(name='noot', hierarchical=True)]
        registry = FieldRegistry(drilldownFields=drilldownFields)
        registry.registerDrilldownField(fieldname='mies', multiValued=False)
        self.assertTrue(registry.isDrilldownField('aap'))
        self.assertTrue(registry.isDrilldownField('noot'))
        self.assertTrue(registry.isDrilldownField('mies'))
        self.assertFalse(registry.isDrilldownField('vuur'))
        self.assertFalse(registry.isHierarchicalDrilldown('aap'))
        self.assertTrue(registry.isHierarchicalDrilldown('noot'))
        self.assertTrue(registry.isMultivaluedDrilldown('aap'))
        self.assertTrue(registry.isMultivaluedDrilldown('noot'))
        self.assertFalse(registry.isMultivaluedDrilldown('mies'))
        self.assertTrue(registry.isUntokenized('mies'))

        field = registry.createFacetField("name", ["value"])
        self.assertEqual({
                "type": "FacetField",
                "name": "name",
                "path": ["value"]
            }, field)

    def testGenericDrilldownFields(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            registry = FieldRegistry(isDrilldownFieldFunction=lambda name: name.startswith('drilldown'))
            self.assertTrue(registry.isDrilldownField('drilldown.aap'))
            self.assertTrue(registry.isDrilldownField('drilldown.noot'))
            self.assertFalse(registry.isDrilldownField('noot'))

    def testDefaultDefinition(self):
        registry = FieldRegistry()
        field = registry.createField('aField', 'id:1')
        self.assertEquals({
                "type": "TextField",
                "name": "aField",
                "value": "id:1",
            }, field)
        self.assertFalse(registry.isUntokenized('aField'))

        registry = FieldRegistry(defaultDefinition=STRINGFIELD)
        field = registry.createField('aField', 'id:1')
        self.assertEquals({
                "type": "StringField",
                "name": "aField",
                "value": "id:1",
            }, field)
        self.assertTrue(registry.isUntokenized('aField'))

    def testTermVectorsForField(self):
        registry = FieldRegistry(termVectorFields=['field1', 'field2'])
        self.assertTrue(registry.isTermVectorField('field1'))
        self.assertTrue(registry.isTermVectorField('field2'))
        self.assertFalse(registry.isTermVectorField('field3'))
        field = registry.createField('field1', 'id:1')
        self.assertEquals({
                "type": "TextField",
                "name": "field1",
                "value": "id:1",
                "termVectors": True,
            }, field)
        field = registry.createField('field2', 'id:1')
        self.assertEquals({
                "type": "TextField",
                "name": "field2",
                "value": "id:1",
                "termVectors": True,
            }, field)
        field = registry.createField('field3', 'id:1')
        self.assertEquals({
                "type": "TextField",
                "name": "field3",
                "value": "id:1",
            }, field)

    def testIsIndexField(self):
        registry = FieldRegistry(drilldownFields=[DrilldownField(f) for f in ['field2', 'field3']], termVectorFields=['field1', 'field2'])
        self.assertTrue(registry.isIndexField('field1'))
        self.assertTrue(registry.isIndexField('field2'))
        self.assertFalse(registry.isIndexField('field3'))
        self.assertTrue(registry.isIndexField('field4'))

    def testIsNumeric(self):
        registry = FieldRegistry()
        registry.register("longfield", fieldDefinition=LONGFIELD)
        registry.register("intfield", fieldDefinition=INTFIELD)
        self.assertFalse(registry.isNumeric('field1'))
        self.assertTrue(registry.isNumeric('longfield'))
        self.assertTrue(registry.isNumeric('intfield'))
        self.assertTrue(registry.isNumeric('range.double.afield'))
        self.assertFalse(registry.isNumeric('__key__.field1'))

    def testRangeQueryAndType(self):
        registry = FieldRegistry()
        registry.register("longfield", fieldDefinition=LONGFIELD)
        registry.register("intfield", fieldDefinition=INTFIELD)
        q, t = registry.rangeQueryAndType('longfield')
        self.assertEqual("Long", q)
        self.assertEqual(long, t)
        q, t = registry.rangeQueryAndType('intfield')
        self.assertEqual("Int", q)
        self.assertEqual(int, t)
        q, t = registry.rangeQueryAndType('range.double.field')
        self.assertEqual("Double", q)
        self.assertEqual(float, t)

        q, t = registry.rangeQueryAndType('anyfield')
        self.assertEqual("String", q)
        self.assertEqual(str, t)

    def testSortField(self):
        registry = FieldRegistry()
        registry.register("sorted.longfield", fieldDefinition=LONGFIELD)
        registry.register("sorted.intfield", fieldDefinition=INTFIELD)
        registry.register("sorted.stringfield", fieldDefinition=STRINGFIELD)

        self.assertEqual("Long", registry.sortFieldType("sorted.longfield"))
        self.assertEqual(JAVA_MIN_LONG, registry.defaultMissingValueForSort("sorted.longfield", True))
        self.assertEqual(JAVA_MAX_LONG, registry.defaultMissingValueForSort("sorted.longfield", False))

        self.assertEqual("Int", registry.sortFieldType("sorted.intfield"))
        self.assertEqual(JAVA_MIN_INT, registry.defaultMissingValueForSort("sorted.intfield", True))
        self.assertEqual(JAVA_MAX_INT, registry.defaultMissingValueForSort("sorted.intfield", False))

        self.assertEqual("String", registry.sortFieldType("sorted.stringfield"))
        self.assertEqual("STRING_FIRST", registry.defaultMissingValueForSort("sorted.stringfield", True))
        self.assertEqual("STRING_LAST", registry.defaultMissingValueForSort("sorted.stringfield", False))

        self.assertEqual(None, registry.defaultMissingValueForSort("score", False))

        field = registry.createField('sorted.longfield', '1')
        self.assertEqual({'name': 'sorted.longfield', 'type': 'LongField', 'value': 1, 'sort': True}, field)
