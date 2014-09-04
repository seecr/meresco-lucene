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

from org.apache.lucene.document import TextField, StringField, NumericDocValuesField, Field, FieldType
from org.apache.lucene.index import FieldInfo

IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
KEY_PREFIX = "__key__."
NUMERIC_PREFIX = "__numeric__."

class FieldFactory(object):
    def __init__(self):
        self._buildField = {
                IDFIELD: (lambda fieldname, value: StringField(fieldname, value, Field.Store.YES)),
            }

    def _oldBuild(self, fieldname, value):
        if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
            return createStringField(fieldname, value)
        if fieldname.startswith(KEY_PREFIX):
            return createNumericDocValuesField(fieldname, value)
        if fieldname.startswith(NUMERIC_PREFIX):
            return createNumericDocValuesField(fieldname, value)
        return createTextField(fieldname, value)

    def createField(self, fieldname, value):
        buildField = self._buildField.get(fieldname)
        if buildField is not None:
            return buildField(fieldname, value)
        return self._oldBuild(fieldname, value)

    def createIdField(self, value):
        return self.createField(IDFIELD, value)

    def register(self, fieldname, buildField):
        self._buildField[fieldname] = buildField

DEFAULT_FACTORY = FieldFactory()

def createStringField(fieldname, value):
    return StringField(fieldname, value, Field.Store.NO)

def createNumericDocValuesField(fieldname, value):
    return NumericDocValuesField(fieldname, long(value))

def createTextField(fieldname, value):
    return TextField(fieldname, value, Field.Store.NO)

def createNoTermsFrequencyField(fieldname, value):
    return Field(fieldname, value, NO_TERMS_FREQUENCY_FIELD)

def _createNoTermsFrequencyFieldType():
    f = FieldType()
    f.setIndexed(True)
    f.setTokenized(True)
    f.setOmitNorms(True)
    f.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
    f.freeze()
    return f
NO_TERMS_FREQUENCY_FIELD = _createNoTermsFrequencyFieldType()

