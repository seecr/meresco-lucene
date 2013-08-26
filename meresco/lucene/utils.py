## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from org.apache.lucene.document import TextField, StringField, Field, LongField, FieldType
from org.apache.lucene.search import SortField
from java.lang import Integer

TIMESTAMPFIELD = '__timestamp__'
IDFIELD = '__id__'
SORTED_PREFIX = "sorted."
UNTOKENIZED_PREFIX = "untokenized."
JOINHASH_PREFIX = "joinhash."

LONGTYPE = 'long'
TEXTTYPE = 'text'
STRINGTYPE = 'string'

typeToField = {
    LONGTYPE: lambda fieldname, value, store: LongField(fieldname, long(value), fieldTypeForLong(store, precisionStep(fieldname))),
    TEXTTYPE: TextField,
    STRINGTYPE: StringField
}
typeToSortFieldType = {
    LONGTYPE: SortField.Type.LONG,
    TEXTTYPE: SortField.Type.STRING,
    STRINGTYPE: SortField.Type.STRING,
}

def fieldType(fieldname):
    if fieldname == IDFIELD:
        return STRINGTYPE
    if fieldname == TIMESTAMPFIELD or fieldname.startswith(JOINHASH_PREFIX):
        return LONGTYPE
    if fieldname.startswith(SORTED_PREFIX) or fieldname.startswith(UNTOKENIZED_PREFIX):
        return STRINGTYPE
    return TEXTTYPE

def fieldTypeForLong(store, precisionStep):
    fieldType = FieldType()
    # For some strange reason FieldType(FieldType ref) doesn't work, this does the same
    ref = LongField.TYPE_STORED if store == Field.Store.YES else LongField.TYPE_NOT_STORED
    fieldType.setIndexed(ref.indexed());
    fieldType.setStored(ref.stored());
    fieldType.setTokenized(ref.tokenized());
    fieldType.setStoreTermVectors(ref.storeTermVectors());
    fieldType.setStoreTermVectorOffsets(ref.storeTermVectorOffsets());
    fieldType.setStoreTermVectorPositions(ref.storeTermVectorPositions());
    fieldType.setStoreTermVectorPayloads(ref.storeTermVectorPayloads());
    fieldType.setOmitNorms(ref.omitNorms());
    fieldType.setIndexOptions(ref.indexOptions());
    fieldType.setDocValueType(ref.docValueType());
    fieldType.setNumericType(ref.numericType());

    if precisionStep:
        fieldType.setNumericPrecisionStep(precisionStep)
    return fieldType

def precisionStep(fieldname):
    if fieldname.startswith(JOINHASH_PREFIX):
        return Integer.MAX_VALUE

def createField(fieldname, value):
    store = Field.Store.YES if fieldname == IDFIELD else Field.Store.NO
    fieldFactory = typeToField[fieldType(fieldname)]
    return fieldFactory(fieldname, value, store)

def sortField(fieldname, sortDescending):
    return SortField(fieldname, typeToSortFieldType[fieldType(fieldname)], sortDescending)

def createIdField(value):
    return createField(IDFIELD, value)

def createTimestampField(value):
    return createField(TIMESTAMPFIELD, value)
