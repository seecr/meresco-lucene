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


from weightless.core import consume

from seecr.test import SeecrTestCase, CallTrace

from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene import FieldsListToLuceneDocument, DrilldownField


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
            ("__key__.field", "a key value"),
            ("__key__.field1", 2),
        ]
        consume(index.add(identifier="", fieldslist=fields))
        self.assertEquals(['addDocument'], observer.calledMethodNames())
        fields = observer.calledMethods[0].kwargs['fields']
        self.assertEqual([
                {'name': 'field1', 'type': 'TextField', 'value': 'value1'},
                {'name': 'field2', 'type': 'TextField', 'value': 'value2'},
                {'name': 'drilldown.field', 'type': 'FacetField', 'path': ['a drilldown value']},
                {'name': '__key__.field', 'type': 'KeyField', 'value': 'a key value'},
                {'name': '__key__.field1', 'type': 'KeyField', 'value': 2},
            ], fields)