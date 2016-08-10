## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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


from warnings import warn


IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
KEY_PREFIX = "__key__."
NUMERIC_PREFIX = "__numeric__."
RANGE_DOUBLE_PREFIX = 'range.double.'
RANGE_INT_PREFIX = 'range.int.'

class FieldRegistry(object):
    def __init__(self, drilldownFields=None, defaultDefinition=None, termVectorFields=None, isDrilldownFieldFunction=None):
        self._fieldDefinitions = {
            IDFIELD: STRINGFIELD_STORED,
        }
        self._indexFieldNames = {}
        self._defaultDefinition = defaultDefinition or TEXTFIELD
        self.drilldownFieldNames = dict()
        self._termVectorFieldNames = set(termVectorFields or [])
        for field in (drilldownFields or []):
            self.registerDrilldownField(field.name, hierarchical=field.hierarchical, multiValued=field.multiValued, indexFieldName=field.indexFieldName)
        self._isDrilldownFieldFunction = lambda name: False
        if isDrilldownFieldFunction is not None:
            self._isDrilldownFieldFunction = isDrilldownFieldFunction
            warn("isDrilldownFieldFunction can have side effects.")

    def createField(self, fieldname, value):
        return self._getFieldDefinition(fieldname).createField(fieldname, value, fieldname in self._termVectorFieldNames)

    def createFacetField(self, field, path):
        return dict(type="FacetField", name=field, path=path)

    def createIdField(self, value):
        return self.createField(IDFIELD, value)

    def register(self, fieldname, fieldDefinition):
        self._fieldDefinitions[fieldname] = fieldDefinition

    def phraseQueryPossible(self, fieldname):
        return self._getFieldDefinition(fieldname).phraseQueryPossible

    def isUntokenized(self, fieldname):
        return self.isDrilldownField(fieldname) or self._getFieldDefinition(fieldname).isUntokenized

    def isNumeric(self, fieldname):
        fieldType = self._getFieldDefinition(fieldname).pythonType
        return fieldType in [long, int, float]

    def registerDrilldownField(self, fieldname, hierarchical=False, multiValued=True, indexFieldName=None):
        self.drilldownFieldNames[fieldname] = dict(
                hierarchical=hierarchical,
                multiValued=multiValued,
                indexFieldName=indexFieldName
            )

    def isDrilldownField(self, fieldname):
        if self._isDrilldownFieldFunction(fieldname):
            # Side effect will only happen when using the _isDrilldownFieldFunction
            if fieldname not in self.drilldownFieldNames:
                self.registerDrilldownField(fieldname, multiValued=True)
            return True
        return fieldname in self.drilldownFieldNames

    def isHierarchicalDrilldown(self, fieldname):
        return self.drilldownFieldNames.get(fieldname, {}).get("hierarchical")

    def isMultivaluedDrilldown(self, fieldname):
        return self.drilldownFieldNames.get(fieldname, {}).get("multiValued")

    def isTermVectorField(self, fieldname):
        return fieldname in self._termVectorFieldNames

    def isIndexField(self, fieldname):
        return not self.isDrilldownField(fieldname) or self.isTermVectorField(fieldname)

    def rangeQueryAndType(self, fieldname):
        if not self.isNumeric(fieldname):
            return "String", str
        definition = self._getFieldDefinition(fieldname)
        query = "Long"
        pythonType = definition.pythonType
        if pythonType == int:
            query = "Int"
        elif pythonType == float:
            query = "Double"
        elif pythonType == long:
            query = "Long"
        return query, definition.pythonType

    def sortFieldType(self, fieldname):
        if not self.isNumeric(fieldname):
            return "String"
        definition = self._getFieldDefinition(fieldname)
        pythonType = definition.pythonType
        if pythonType == int:
            return "Int"
        elif pythonType == float:
            return "Double"
        elif pythonType == long:
            return "Long"

    def defaultMissingValueForSort(self, fieldname, sortDescending):
        if not self.isNumeric(fieldname) and fieldname != "score":
            return "STRING_FIRST" if sortDescending else "STRING_LAST"
        return None

    def _getFieldDefinition(self, fieldname):
        fieldDefinition = self._fieldDefinitions.get(fieldname)
        if not fieldDefinition is None:
            return fieldDefinition
        fieldDefinition = self._defaultDefinition
        if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
            fieldDefinition = STRINGFIELD
        elif fieldname.startswith(KEY_PREFIX):
            fieldDefinition = KEYFIELD
        elif fieldname.startswith(NUMERIC_PREFIX):
            fieldDefinition = NUMERICFIELD
        elif fieldname.startswith(RANGE_DOUBLE_PREFIX):
            fieldDefinition = DOUBLEFIELD
        elif fieldname.startswith(RANGE_INT_PREFIX):
            fieldDefinition = INTFIELD
        self._fieldDefinitions[fieldname] = fieldDefinition
        return fieldDefinition

# These names should be the same in the meresco-lucene-server code.

class _FieldDefinition(object):
    def __init__(self, type, pythonType, isUntokenized, phraseQueryPossible, stored=False):
        self.type = type
        self.pythonType = pythonType
        self.phraseQueryPossible = phraseQueryPossible
        self.isUntokenized = isUntokenized
        self.stored = stored

    def createField(self, name, value, termVectors=False):
        field = dict(
            type=self.type,
            name=name,
            value=value,
        )
        if termVectors:
            field['termVectors'] = True
        if name.startswith(SORTED_PREFIX):
            field["sort"] = True
        if self.stored:
            field["stored"] = True
        return field

STRINGFIELD_STORED = _FieldDefinition("StringField",
    pythonType=str,
    isUntokenized=True,
    phraseQueryPossible=True,
    stored=True)
STRINGFIELD = _FieldDefinition("StringField",
    pythonType=str,
    isUntokenized=True,
    phraseQueryPossible=True)
TEXTFIELD = _FieldDefinition("TextField",
    pythonType=str,
    isUntokenized=False,
    phraseQueryPossible=True)
NO_TERMS_FREQUENCY_FIELD = _FieldDefinition("NoTermsFrequencyField",
    pythonType=str,
    isUntokenized=False,
    phraseQueryPossible=False)
INTFIELD = _FieldDefinition("IntField",
    pythonType=int,
    isUntokenized=False,
    phraseQueryPossible=False)
INTFIELD_STORED = _FieldDefinition("IntField",
    pythonType=int,
    isUntokenized=False,
    phraseQueryPossible=False,
    stored=True)
LONGFIELD = _FieldDefinition("LongField",
    pythonType=long,
    isUntokenized=False,
    phraseQueryPossible=False)
LONGFIELD_STORED = _FieldDefinition("LongField",
    pythonType=long,
    isUntokenized=False,
    phraseQueryPossible=False,
    stored=True)
DOUBLEFIELD = _FieldDefinition("DoubleField",
    pythonType=float,
    isUntokenized=False,
    phraseQueryPossible=False)
DOUBLEFIELD_STORED = _FieldDefinition("DoubleField",
    pythonType=float,
    isUntokenized=False,
    phraseQueryPossible=False,
    stored=True)
NUMERICFIELD = _FieldDefinition("NumericField",
    pythonType=long,
    isUntokenized=False,
    phraseQueryPossible=False)
KEYFIELD = _FieldDefinition("KeyField",
    pythonType=long,
    isUntokenized=True,
    phraseQueryPossible=False)
