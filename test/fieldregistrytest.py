## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import SeecrTestCase
from meresco.lucene.fieldregistry import FieldRegistry, NO_TERMS_FREQUENCY_FIELDTYPE, STRINGFIELD
from org.apache.lucene.index import FieldInfo
from org.apache.lucene.document import StringField, TextField
from meresco.lucene import DrilldownField


class FieldRegistryTest(SeecrTestCase):
    def testDefault(self):
        registry = FieldRegistry()
        field = registry.createField('__id__', 'id:1')
        self.assertFalse(field.fieldType().tokenized())
        self.assertTrue(field.fieldType().stored())
        self.assertTrue(field.fieldType().indexed())
        self.assertTrue(registry.isUntokenized('__id__'))

    def testSpecificField(self):
        registry = FieldRegistry()
        field = registry.createField('fieldname', 'value')
        self.assertFalse(field.fieldType().stored())
        registry.register('fieldname', StringField.TYPE_STORED)
        field = registry.createField('fieldname', 'value')
        self.assertTrue(field.fieldType().stored())

    def testNoTermsFreqField(self):
        registry = FieldRegistry()
        registry.register('fieldname', NO_TERMS_FREQUENCY_FIELDTYPE)
        field = registry.createField('fieldname', 'value')
        self.assertEquals(FieldInfo.IndexOptions.DOCS_ONLY, field.fieldType().indexOptions())

    def testPhraseQueryPossible(self):
        registry = FieldRegistry()
        registry.register('fieldname', NO_TERMS_FREQUENCY_FIELDTYPE)
        self.assertFalse(registry.phraseQueryPossible('fieldname'))
        self.assertTrue(registry.phraseQueryPossible('other.fieldname'))

    def testIsUntokenized(self):
        registry = FieldRegistry()
        self.assertTrue(registry.isUntokenized('untokenized.some.field'))
        registry.register('fieldname', StringField.TYPE_NOT_STORED)
        self.assertTrue(registry.isUntokenized('fieldname'))
        registry.register('fieldname', TextField.TYPE_NOT_STORED)
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

        facetsConfig = registry.facetsConfig
        dimConfigs = facetsConfig.getDimConfigs()
        self.assertEquals(set(['aap', 'noot', 'mies']), set(dimConfigs.keySet()))
        self.assertFalse(dimConfigs.get('aap').hierarchical)
        self.assertTrue(dimConfigs.get('noot').hierarchical)
        self.assertTrue(dimConfigs.get('noot').multiValued)
        self.assertFalse(dimConfigs.get('mies').multiValued)

    def testReuseCreatedField(self):
        registry = FieldRegistry()
        field = registry.createField('fieldname', 'value')
        self.assertEquals("value", field.stringValue())
        newField = registry.createField('fieldname', 'newvalue', mayReUse=True)
        self.assertEquals("newvalue", newField.stringValue())
        self.assertEquals(field, newField)
        newField2 = registry.createField('fieldname', 'newvalue', mayReUse=False)
        self.assertEquals("newvalue", newField2.stringValue())
        self.assertNotEqual(newField, newField2)

    def testDefaultDefinition(self):
        registry = FieldRegistry()
        field = registry.createField('aField', 'id:1')
        self.assertTrue(field.fieldType().tokenized())
        self.assertFalse(field.fieldType().stored())
        self.assertTrue(field.fieldType().indexed())
        self.assertFalse(registry.isUntokenized('aField'))

        registry = FieldRegistry(defaultDefinition=STRINGFIELD)
        field = registry.createField('aField', 'id:1')
        self.assertFalse(field.fieldType().tokenized())
        self.assertFalse(field.fieldType().stored())
        self.assertTrue(field.fieldType().indexed())
        self.assertTrue(registry.isUntokenized('aField'))

    def testTermVectorsForField(self):
        registry = FieldRegistry(termVectorFields=['field1', 'field2'])
        field = registry.createField('field1', 'id:1')
        self.assertTrue(field.fieldType().storeTermVectors())
        field = registry.createField('field2', 'id:1')
        self.assertTrue(field.fieldType().storeTermVectors())
        field = registry.createField('field3', 'id:1')
        self.assertFalse(field.fieldType().storeTermVectors())
