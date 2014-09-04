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
from itertools import chain

IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
KEY_PREFIX = "__key__."
NUMERIC_PREFIX = "__numeric__."
PHRASE_QUERY_POSSIBLE, IS_UNTOKENIZED, BUILD, STRINGFIELD, NUMERICFIELD, TEXTFIELD, TYPE = range(7)

class FieldFactory(object):
    def __init__(self):
        self._buildFields = {
                IDFIELD: {
                    TYPE: StringField.TYPE_STORED,
                },
            }
        self._prefixBuildFields = {
                STRINGFIELD: {
                    TYPE: StringField.TYPE_NOT_STORED,
                },
                NUMERICFIELD: {
                    BUILD: lambda fieldname, value: NumericDocValuesField(fieldname, long(value)),
                    TYPE: NumericDocValuesField.TYPE,
                },
                TEXTFIELD: {
                    TYPE: TextField.TYPE_NOT_STORED,
                },
            }
        for fieldDict in chain(self._buildFields.values(), self._prefixBuildFields.values()):
            self._initializeFieldDict(fieldDict, build=fieldDict.get(BUILD))

    def createField(self, fieldname, value):
        return self._getBuildField(fieldname)[BUILD](fieldname, value)

    def createIdField(self, value):
        return self.createField(IDFIELD, value)

    def register(self, fieldname, fieldType, build=None):
        fieldDict = {TYPE: fieldType}
        self._initializeFieldDict(fieldDict, build=build)
        self._buildFields[fieldname] = fieldDict

    def freeze(self):
        def register(*args, **kwargs):
            raise ValueError('This factory can not be changed.')
        self.register = register
        return self

    def phraseQueryPossible(self, fieldname):
        return self._getBuildField(fieldname)[PHRASE_QUERY_POSSIBLE]

    def isUntokenized(self, fieldname):
        return self._getBuildField(fieldname)[IS_UNTOKENIZED]

    def _initializeFieldDict(self, fieldDict, build):
        fieldDict[IS_UNTOKENIZED] = not fieldDict[TYPE].tokenized()
        positionsStored = fieldDict[TYPE].indexOptions() in [FieldInfo.IndexOptions.DOCS_ONLY, FieldInfo.IndexOptions.DOCS_AND_FREQS]
        fieldDict[PHRASE_QUERY_POSSIBLE] = not positionsStored
        if build is None:
            build = lambda fieldname, value: Field(fieldname, value, fieldDict[TYPE])
        fieldDict[BUILD] = build

    def _getBuildField(self, fieldname):
        buildField = self._buildFields.get(fieldname)
        if buildField is not None:
            return buildField
        if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
            return self._prefixBuildFields[STRINGFIELD]
        if fieldname.startswith(KEY_PREFIX) or fieldname.startswith(NUMERIC_PREFIX):
            return self._prefixBuildFields[NUMERICFIELD]
        return self._prefixBuildFields[TEXTFIELD]

def _createNoTermsFrequencyFieldType():
    f = FieldType()
    f.setIndexed(True)
    f.setTokenized(True)
    f.setOmitNorms(True)
    f.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
    f.freeze()
    return f
NO_TERMS_FREQUENCY_FIELDTYPE = _createNoTermsFrequencyFieldType()

DEFAULT_FACTORY = FieldFactory().freeze()
