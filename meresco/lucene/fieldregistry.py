## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2014-2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018, 2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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
        self.isDrilldownField = lambda name: name in self.drilldownFieldNames
        if isDrilldownFieldFunction is not None:
            self.isDrilldownField = isDrilldownFieldFunction

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
        return fieldType in [int, int, float]

    def registerDrilldownField(self, fieldname, hierarchical=False, multiValued=True, indexFieldName=None):
        self.drilldownFieldNames[fieldname] = dict(
                hierarchical=hierarchical,
                multiValued=multiValued,
                indexFieldName=indexFieldName
            )

    def isHierarchicalDrilldown(self, fieldname):
        return self.drilldownFieldNames.get(fieldname, {}).get("hierarchical")

    def isMultivaluedDrilldown(self, fieldname):
        return self.drilldownFieldNames.get(fieldname, {}).get("multiValued")

    def isTermVectorField(self, fieldname):
        return fieldname in self._termVectorFieldNames

    def isIndexField(self, fieldname):
        return not self.isDrilldownField(fieldname) or self.isTermVectorField(fieldname)

    def rangeQueryAndType(self, fieldname):
        definition = self._getFieldDefinition(fieldname)
        return definition.queryType, definition.pythonType

    def sortFieldType(self, fieldname):
        definition = self._getFieldDefinition(fieldname)
        return definition.queryType

    getQueryType = sortFieldType

    def defaultMissingValueForSort(self, fieldname, sortDescending):
        if fieldname == 'score':
            return None
        return self._getFieldDefinition(fieldname).missingValuesForSort[1 if sortDescending else 0]

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
    def __init__(self, type, pythonType, isUntokenized, phraseQueryPossible, queryType="String", stored=False, missingValuesForSort=(None, None)):
        self.type = type
        self.pythonType = pythonType
        self.queryType = queryType
        self._transformValue = (lambda x:x) if pythonType is None else (lambda x:pythonType(x))
        self.phraseQueryPossible = phraseQueryPossible
        self.isUntokenized = isUntokenized
        self.stored = stored
        self.missingValuesForSort=missingValuesForSort

    def createField(self, name, value, termVectors=False):
        field = dict(
            type=self.type,
            name=name,
            value=self._transformValue(value),
        )
        if termVectors:
            field['termVectors'] = True
        if name.startswith(SORTED_PREFIX):
            field["sort"] = True
        if self.stored:
            field["stored"] = True
        return field

    def clone(self, **kwargs):
        newArgs = {k:v for k,v in self.__dict__.items() if k[0] != '_'}
        newArgs.update(**kwargs)
        return _FieldDefinition(**newArgs)

JAVA_MAX_INT, JAVA_MIN_INT = 2**31-1, -2**31
JAVA_MAX_LONG, JAVA_MIN_LONG = 2**63-1, -2**63

STRINGFIELD = _FieldDefinition("StringField",
    pythonType=str,
    isUntokenized=True,
    phraseQueryPossible=True,
    missingValuesForSort=('STRING_LAST', 'STRING_FIRST'))
STRINGFIELD_STORED = STRINGFIELD.clone(stored=True)
TEXTFIELD = _FieldDefinition("TextField",
    pythonType=str,
    isUntokenized=False,
    phraseQueryPossible=True,
    missingValuesForSort=('STRING_LAST', 'STRING_FIRST'))
NO_TERMS_FREQUENCY_FIELD = _FieldDefinition("NoTermsFrequencyField",
    pythonType=str,
    isUntokenized=False,
    phraseQueryPossible=False,
    missingValuesForSort=('STRING_LAST', 'STRING_FIRST'))
INTFIELD = _FieldDefinition("IntField",
    pythonType=int,
    queryType="Int",
    isUntokenized=False,
    phraseQueryPossible=False,
    missingValuesForSort=(JAVA_MAX_INT, JAVA_MIN_INT))
INTFIELD_STORED = INTFIELD.clone(stored=True)
INTPOINT = _FieldDefinition("IntPoint",
    pythonType=int,
    queryType="Int",
    isUntokenized=False,
    phraseQueryPossible=False,
    missingValuesForSort=(JAVA_MAX_INT, JAVA_MIN_INT))
LONGFIELD = _FieldDefinition("LongField",
    pythonType=int,
    queryType="Long",
    isUntokenized=False,
    phraseQueryPossible=False,
    missingValuesForSort=(JAVA_MAX_LONG, JAVA_MIN_LONG))
LONGFIELD_STORED = LONGFIELD.clone(stored=True)
LONGPOINT = _FieldDefinition("LongPoint",
    pythonType=int,
    queryType="Long",
    isUntokenized=False,
    phraseQueryPossible=False,
    missingValuesForSort=(JAVA_MAX_LONG, JAVA_MIN_LONG))
DOUBLEFIELD = _FieldDefinition("DoubleField",
    pythonType=float,
    queryType="Double",
    isUntokenized=False,
    phraseQueryPossible=False)
DOUBLEFIELD_STORED = DOUBLEFIELD.clone(stored=True)
DOUBLEPOINT = _FieldDefinition("DoublePoint",
    pythonType=float,
    queryType="Double",
    isUntokenized=False,
    phraseQueryPossible=False)
NUMERICFIELD = _FieldDefinition("NumericField",
    pythonType=int,
    isUntokenized=False,
    phraseQueryPossible=False)
KEYFIELD = _FieldDefinition("KeyField",
    pythonType=None,
    isUntokenized=True,
    phraseQueryPossible=False)
LATLONFIELD = _FieldDefinition("LatLonField",
    pythonType=list,
    queryType='Distance',
    isUntokenized=True,
    phraseQueryPossible=False)
