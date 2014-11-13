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
from org.apache.lucene.facet import FacetsConfig, DrillDownQuery


IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
KEY_PREFIX = "__key__."
NUMERIC_PREFIX = "__numeric__."

class FieldRegistry(object):
    def __init__(self, drilldownFields=None):
        self._fieldDefinitions = {
            IDFIELD: _FieldDefinition(type=StringField.TYPE_STORED),
        }
        self._drilldownFieldNames = set()
        self._hierarchicalDrilldownFieldNames = set()
        self.facetsConfig = FacetsConfig()
        for field in (drilldownFields or []):
            self.registerDrilldownField(field.name, hierarchical=field.hierarchical, multiValued=field.multiValued)

    def createField(self, fieldname, value):
        return self._getFieldDefinition(fieldname).create(fieldname, value)

    def createIdField(self, value):
        return self.createField(IDFIELD, value)

    def register(self, fieldname, fieldType, create=None):
        self._fieldDefinitions[fieldname] = _FieldDefinition(type=fieldType, create=create)

    def phraseQueryPossible(self, fieldname):
        return self._getFieldDefinition(fieldname).phraseQueryPossible

    def isUntokenized(self, fieldname):
        return self._getFieldDefinition(fieldname).isUntokenized

    def registerDrilldownField(self, fieldname, hierarchical=False, multiValued=True):
        self._drilldownFieldNames.add(fieldname)
        if hierarchical:
            self._hierarchicalDrilldownFieldNames.add(fieldname)
        self.facetsConfig.setMultiValued(fieldname, multiValued)
        self.facetsConfig.setHierarchical(fieldname, hierarchical)

    def isDrilldownField(self, fieldname):
        return fieldname in self._drilldownFieldNames

    def isHierarchicalDrilldown(self, fieldname):
        return fieldname in self._hierarchicalDrilldownFieldNames

    def makeDrilldownTerm(self, fieldname, path):
        indexFieldName = self.facetsConfig.getDimConfig(fieldname).indexFieldName;
        return DrillDownQuery.term(indexFieldName, fieldname, path)

    def _getFieldDefinition(self, fieldname):
        fieldDefinition = self._fieldDefinitions.get(fieldname)
        if fieldDefinition is not None:
            return fieldDefinition
        if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
            return STRINGFIELD
        if fieldname.startswith(KEY_PREFIX) or fieldname.startswith(NUMERIC_PREFIX):
            return NUMERICFIELD
        return TEXTFIELD


class _FieldDefinition(object):
    def __init__(self, type, create=None):
        self.type = type
        self.create = create
        if self.create is None:
            self.create = lambda fieldname, value: Field(fieldname, value, self.type)
        positionsStored = self.type.indexOptions() in [FieldInfo.IndexOptions.DOCS_ONLY, FieldInfo.IndexOptions.DOCS_AND_FREQS]
        self.phraseQueryPossible = not positionsStored
        self.isUntokenized = not self.type.tokenized()

STRINGFIELD = _FieldDefinition(type=StringField.TYPE_NOT_STORED)
NUMERICFIELD = _FieldDefinition(
    type=NumericDocValuesField.TYPE,
    create=lambda fieldname, value: NumericDocValuesField(fieldname, long(value))
)
TEXTFIELD = _FieldDefinition(type=TextField.TYPE_NOT_STORED)


def _createNoTermsFrequencyFieldType():
    f = FieldType()
    f.setIndexed(True)
    f.setTokenized(True)
    f.setOmitNorms(True)
    f.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
    f.freeze()
    return f

NO_TERMS_FREQUENCY_FIELDTYPE = _createNoTermsFrequencyFieldType()

