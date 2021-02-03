## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from meresco.core import Observable
from meresco.lucene import SORTED_PREFIX
from itertools import chain

from .fieldregistry import IDFIELD


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
        if name.startswith(SORTED_PREFIX) and valueList:
            return
        valueList.append(value)

    def addFacetField(self, name, value):
        tx = self.ctx.tx
        valueList = tx.objectScope(self).setdefault('facet_fields', {}).setdefault(name, [])
        valueList.append(value)

    def commit(self, id):
        tx = self.ctx.tx
        fieldValues = tx.objectScope(self).get('fields', {})
        facet_fields = tx.objectScope(self).get('facet_fields', {})
        if not (fieldValues or facet_fields):
            return
        identifier = self._identifierRewrite(tx.locals['id'])
        fieldValues = self._rewriteFields(fieldValues)
        yield self.all.addDocument(
                identifier=identifier,
                fields=self._createFields(fieldValues, facet_fields),
            )

    def _createFields(self, fieldValues, facet_fields=None):
        facet_fields = facet_fields or {}
        fields = []
        for field, values in chain(fieldValues.items(), facet_fields.items()):
            if self._fieldRegistry.isDrilldownField(field):
                for value in values:
                    if hasattr(value, 'extend'):
                        path = [str(category) for category in value]
                    else:
                        path = [str(value)]
                    fields.append(self._fieldRegistry.createFacetField(field, path))
            else:
                for value in values:
                    if field == IDFIELD:
                        raise ValueError("Field '%s' is protected and created by Meresco Lucene" % IDFIELD)
                    fields.append(self._fieldRegistry.createField(field, value))
        return fields
