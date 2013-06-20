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

from meresco.core import Observable
from org.apache.lucene.document import Document, Field, IntField
from _lucene import IDFIELD

class Fields2LuceneDoc(Observable):
    def __init__(self, transactionName):
        Observable.__init__(self)
        self._transactionName = transactionName

    def begin(self, name):
        if name != self._transactionName:
            return
        tx = self.ctx.tx
        tx.join(self)

    def addField(self, name, value):
        tx = self.ctx.tx
        valueList = tx.objectScope(self).setdefault(name.split('.', 1)[-1], [])
        valueList.append(value)

    def commit(self, id):
        tx = self.ctx.tx
        fields = tx.objectScope(self)
        if not fields:
            return
        recordIdentifier = tx.locals["id"]
        specialFields = {
            IDFIELD: [recordIdentifier],
        }
        fields.update(specialFields)
        document = self._createDocument(fields)
        yield self.all.addDocument(document=document)

    def _createDocument(self, fields):
        doc = Document()
        for field, values in fields.items():
            for value in values:
                if type(value) == int:
                    f = IntField(field, value, Field.Store.NO)
                else:
                    f = Field(field, value, Field.Store.YES, Field.Index.ANALYZED)
                doc.add(f)
        return doc