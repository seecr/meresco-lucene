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
from meresco.lucene.fieldfactory import FieldFactory, NO_TERMS_FREQUENCY_FIELDTYPE
from org.apache.lucene.index import FieldInfo
from org.apache.lucene.document import StringField, TextField
from meresco.lucene.fieldfactory import DEFAULT_FACTORY


class FieldFactoryTest(SeecrTestCase):

    def testDefault(self):
        factory = FieldFactory()
        field = factory.createField('__id__', 'id:1')
        self.assertFalse(field.fieldType().tokenized())
        self.assertTrue(field.fieldType().stored())
        self.assertTrue(field.fieldType().indexed())
        self.assertTrue(factory.isUntokenized('__id__'))

    def testSpecificField(self):
        factory = FieldFactory()
        field = factory.createField('fieldname', 'value')
        self.assertTrue(field.fieldType().tokenized())
        factory.register('fieldname', StringField.TYPE_NOT_STORED, build=lambda fieldname, value: 'NEW FIELD')
        self.assertEquals('NEW FIELD', factory.createField('fieldname', 'value'))

    def testNoTermsFreqField(self):
        factory = FieldFactory()
        factory.register('fieldname', NO_TERMS_FREQUENCY_FIELDTYPE)
        field = factory.createField('fieldname', 'value')
        self.assertEquals(FieldInfo.IndexOptions.DOCS_ONLY, field.fieldType().indexOptions())

    def testPhraseQueryPossible(self):
        factory = FieldFactory()
        factory.register('fieldname', NO_TERMS_FREQUENCY_FIELDTYPE)
        self.assertFalse(factory.phraseQueryPossible('fieldname'))
        self.assertTrue(factory.phraseQueryPossible('other.fieldname'))

    def testIsUntokenized(self):
        factory = FieldFactory()
        self.assertTrue(factory.isUntokenized('untokenized.some.field'))
        factory.register('fieldname', StringField.TYPE_NOT_STORED)
        self.assertTrue(factory.isUntokenized('fieldname'))
        factory.register('fieldname', TextField.TYPE_NOT_STORED)
        self.assertFalse(factory.isUntokenized('fieldname'))

    def testFreeze(self):
        factory = FieldFactory()
        factory.register('fieldname', StringField.TYPE_NOT_STORED)
        factory.freeze()
        self.assertRaises(ValueError, factory.register, 'fieldname2', StringField.TYPE_NOT_STORED)

    def testDefaultFactoryFrozen(self):
        self.assertRaises(ValueError, DEFAULT_FACTORY.register, 'fieldname2', StringField.TYPE_NOT_STORED)
