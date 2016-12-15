## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from weightless.core import compose
from meresco.core import Observable


class FieldsListToLuceneDocument(Observable):
    def __init__(self, fieldRegistry, untokenizedFieldnames, indexFieldFactory, rewriteIdentifier=None, **kwargs):
        Observable.__init__(self, **kwargs)
        self._fieldRegistry = fieldRegistry
        self._untokenizedFieldnames = untokenizedFieldnames
        self._indexFieldFactory = indexFieldFactory
        self._rewriteIdentifier = rewriteIdentifier or (lambda i: i)

    def add(self, identifier, fieldslist, **kwargs):
        indexFieldFactory = self._indexFieldFactory(self, self._untokenizedFieldnames)
        fields = []
        for fieldname, value in fieldslist:
            for fieldname, value in (yield compose(indexFieldFactory.fieldsFor(fieldname, value))):
                self._addFieldToLuceneDocument(fields=fields, fieldname=fieldname, value=value)
        yield self.all.addDocument(identifier=self._rewriteIdentifier(identifier), fields=fields)

    def _addFieldToLuceneDocument(self, fieldname, value, fields):
        if self._fieldRegistry.isDrilldownField(fieldname):
            lvalue = value
            if isinstance(lvalue, basestring):
                lvalue = [unicode(lvalue)]
            lvalue = [v[:MAX_STRING_LENGTH] for v in lvalue]
            fields.append(self._fieldRegistry.createFacetField(fieldname, lvalue))
        if self._fieldRegistry.isIndexField(fieldname):
            field = self._fieldRegistry.createField(fieldname, value)
            fields.append(field)

MAX_STRING_LENGTH = 256
