## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from org.apache.lucene.document import TextField, StringField, Field, LongField, NumericDocValuesField
from org.apache.lucene.search import SortField

TIMESTAMPFIELD = '__timestamp__'
IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
KEY_PREFIX = "__key__."
NUMERIC_PREFIX = "__numeric__."

LONGTYPE = 'long'
TEXTTYPE = 'text'
STRINGTYPE = 'string'
NUMERICTYPE = 'numeric'

typeToField = {
    LONGTYPE: lambda fieldname, value, store: LongField(fieldname, long(value), store),
    TEXTTYPE: TextField,
    STRINGTYPE: StringField,
    NUMERICTYPE: lambda fieldname, value, store: NumericDocValuesField(fieldname, long(value)),
}
typeToSortFieldTypeAndMissingValue = {
    LONGTYPE: (SortField.Type.LONG, None),
    TEXTTYPE: (SortField.Type.STRING, (SortField.STRING_LAST, SortField.STRING_FIRST)),
    STRINGTYPE: (SortField.Type.STRING, (SortField.STRING_LAST, SortField.STRING_FIRST)),
}

def fieldType(fieldname):
    if fieldname == IDFIELD:
        return STRINGTYPE
    if fieldname == TIMESTAMPFIELD:
        return LONGTYPE
    if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
        return STRINGTYPE
    if fieldname.startswith(KEY_PREFIX):
        return NUMERICTYPE
    if fieldname.startswith(NUMERIC_PREFIX):
        return NUMERICTYPE
    return TEXTTYPE

def createField(fieldname, value):
    store = Field.Store.YES if fieldname == IDFIELD else Field.Store.NO
    fieldFactory = typeToField[fieldType(fieldname)]
    return fieldFactory(fieldname, value, store)

def sortField(fieldname, sortDescending):
    sortType, missingValues = typeToSortFieldTypeAndMissingValue[fieldType(fieldname)]
    result = SortField(fieldname, sortType, sortDescending)
    if missingValues is not None:
        valueAsc, valueDesc = missingValues
        result.setMissingValue(valueDesc if sortDescending else valueAsc)
    return result

def createIdField(value):
    return createField(IDFIELD, value)

def createTimestampField(value):
    return createField(TIMESTAMPFIELD, value)

def rankToTermFreq(value, rank):
    rank = max(0, min(1, rank))
    return '{0} '.format(value) * int(rank * 1000)

