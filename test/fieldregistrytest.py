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

from seecr.test import SeecrTestCase
from meresco.lucene.fieldregistry import FieldRegistry, NO_TERMS_FREQUENCY_FIELDTYPE
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
        self.assertTrue(field.fieldType().tokenized())
        registry.register('fieldname', StringField.TYPE_NOT_STORED, create=lambda fieldname, value: 'NEW FIELD')
        self.assertEquals('NEW FIELD', registry.createField('fieldname', 'value'))

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
        self.assertEquals(drilldownFields, registry.drilldownFields)
        self.assertTrue(registry.isDrilldownField('aap'))
        self.assertTrue(registry.isDrilldownField('noot'))




