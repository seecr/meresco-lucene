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
STRINGFIELD, NUMERICFIELD, TEXTFIELD = range(3)

class FieldRegistry(object):
    def __init__(self):
        self._buildFields = {
                IDFIELD: FieldRegistry.FieldDefinition(type=StringField.TYPE_STORED),
            }
        self._prefixBuildFields = {
                STRINGFIELD: FieldRegistry.FieldDefinition(type=StringField.TYPE_NOT_STORED),
                NUMERICFIELD: FieldRegistry.FieldDefinition(
                    type=NumericDocValuesField.TYPE,
                    create=lambda fieldname, value: NumericDocValuesField(fieldname, long(value)),
                ),
                TEXTFIELD: FieldRegistry.FieldDefinition(type=TextField.TYPE_NOT_STORED),
            }

    def createField(self, fieldname, value):
        return self._getFieldDefinition(fieldname).create(fieldname, value)

    def createIdField(self, value):
        return self.createField(IDFIELD, value)

    def register(self, fieldname, fieldType, create=None):
        self._buildFields[fieldname] = FieldRegistry.FieldDefinition(type=fieldType, create=create)

    def phraseQueryPossible(self, fieldname):
        return self._getFieldDefinition(fieldname).phraseQueryPossible

    def isUntokenized(self, fieldname):
        return self._getFieldDefinition(fieldname).isUntokenized

    def _getFieldDefinition(self, fieldname):
        buildField = self._buildFields.get(fieldname)
        if buildField is not None:
            return buildField
        if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
            return self._prefixBuildFields[STRINGFIELD]
        if fieldname.startswith(KEY_PREFIX) or fieldname.startswith(NUMERIC_PREFIX):
            return self._prefixBuildFields[NUMERICFIELD]
        return self._prefixBuildFields[TEXTFIELD]

    class FieldDefinition(object):
        def __init__(self, type, create=None):
            self.type = type
            self.create = create
            if self.create is None:
                self.create = lambda fieldname, value: Field(fieldname, value, self.type)
            positionsStored = self.type.indexOptions() in [FieldInfo.IndexOptions.DOCS_ONLY, FieldInfo.IndexOptions.DOCS_AND_FREQS]
            self.phraseQueryPossible = not positionsStored
            self.isUntokenized = not self.type.tokenized()

def _createNoTermsFrequencyFieldType():
    f = FieldType()
    f.setIndexed(True)
    f.setTokenized(True)
    f.setOmitNorms(True)
    f.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
    f.freeze()
    return f
NO_TERMS_FREQUENCY_FIELDTYPE = _createNoTermsFrequencyFieldType()

