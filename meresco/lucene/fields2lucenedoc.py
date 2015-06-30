## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Observable
from org.apache.lucene.document import Document

from fieldregistry import IDFIELD, KEY_PREFIX


class Fields2LuceneDoc(Observable):
    def __init__(self, transactionName, fieldRegistry, identifierRewrite=None, rewriteFields=None):
        Observable.__init__(self)
        self._transactionName = transactionName
        self._identifierRewrite = (lambda identifier: identifier) if identifierRewrite is None else identifierRewrite
        self._rewriteFields = (lambda fields: fields) if rewriteFields is None else rewriteFields
        self._fieldRegistry = fieldRegistry

    def begin(self, name):
        if name != self._transactionName:
            return
        tx = self.ctx.tx
        tx.join(self)

    def addField(self, name, value):
        tx = self.ctx.tx
        valueList = tx.objectScope(self).setdefault('fields', {}).setdefault(name, [])
        valueList.append(value)

    def addFacetField(self, name, value):
        tx = self.ctx.tx
        valueList = tx.objectScope(self).setdefault('facet_fields', {}).setdefault(name, [])
        valueList.append(value)

    def commit(self, id):
        tx = self.ctx.tx
        fields = tx.objectScope(self).get('fields', {})
        facet_fields = tx.objectScope(self).get('facet_fields', {})
        if not (fields or facet_fields):
            return
        identifier = self._identifierRewrite(tx.locals['id'])
        fields = self._rewriteFields(fields)
        yield self.all.addDocument(
                identifier=identifier,
                document=self._createDocument(fields, facet_fields),
            )

    def _createDocument(self, fields, facet_fields=None):
        facet_fields = facet_fields or {}
        doc = Document()
        for field, values in (fields.items() + facet_fields.items()):
            if self._fieldRegistry.isDrilldownField(field):
                for value in values:
                    if hasattr(value, 'extend'):
                        path = [str(category) for category in value]
                    else:
                        path = [str(value)]
                    doc.add(self._fieldRegistry.createFacetField(field, path))
            else:
                for value in values:
                    if field == IDFIELD:
                        raise ValueError("Field '%s' is protected and created by Meresco Lucene" % IDFIELD)
                    if field.startswith(KEY_PREFIX):
                        value = self.call.numerateTerm(value)
                    doc.add(self._fieldRegistry.createField(field, value))
        return doc
