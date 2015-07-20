## begin license ##
#
# "NBC+" also known as "ZP (ZoekPlatform)" is
#  a project of the Koninklijke Bibliotheek
#  and provides a search service for all public
#  libraries in the Netherlands.
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
#
# This file is part of "NBC+ (Zoekplatform BNL)"
#
# "NBC+ (Zoekplatform BNL)" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "NBC+ (Zoekplatform BNL)" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "NBC+ (Zoekplatform BNL)"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##


from weightless.core import consume

from seecr.test import SeecrTestCase, CallTrace

from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene import FieldsListToLuceneDocument, DrilldownField

from org.apache.lucene.facet import FacetField


class FieldsListToLuceneDocumentTest(SeecrTestCase):
    def testAdd(self):
        class Factory():
            def __init__(self, observable, untokenizedFieldnames):
                self.observable = observable
                self.untokenizedFieldnames = untokenizedFieldnames

            def fieldsFor(self, fieldname, value):
                raise StopIteration([(fieldname, value)])
                yield
        fieldFactory = Factory

        fieldRegistry = FieldRegistry(drilldownFields=[DrilldownField('drilldown.field')])
        index = FieldsListToLuceneDocument(fieldRegistry, untokenizedFieldnames=[], indexFieldFactory=fieldFactory)
        observer = CallTrace(emptyGeneratorMethods=['addDocument'])
        index.addObserver(observer)
        fields = [
            ("field1", "value1"),
            ("field2", "value2"),
            ("drilldown.field", "a drilldown value"),
        ]
        consume(index.add(identifier="", fieldslist=fields))
        self.assertEquals(['addDocument'], observer.calledMethodNames())
        document = observer.calledMethods[0].kwargs['document']
        searchFields, facetsFields = fieldsFromDocument(document)
        self.assertEquals(set([
                'field1',
                'field2',
            ]), set([f.name() for f in searchFields]))
        self.assertEquals([
                ('drilldown.field', ['a drilldown value']),
            ], [(f.dim, list(f.path)) for f in facetsFields])

def fieldsFromDocument(document):
    searchFields = [f for f in document.getFields() if not FacetField.instance_(f)]
    facetsFields = [FacetField.cast_(f) for f in document.getFields() if FacetField.instance_(f)]
    return searchFields, facetsFields
