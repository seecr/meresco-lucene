## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2014-2016, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
        longSpecialCharacterValue = '\u041c\u0438\u043d\u0438\u0441\u0442\u0435\u0440\u0441\u0442\u0432\u043e \u0420\u044b\u0431\u043d\u043e\u0439 \u041f\u0440\u043e\u043c\u044b\u0448\u043b\u0435\u043d\u043d\u043e\u0441\u0438 \u0421\u043e\u044e\u0437\u0430 \u0421\u0421\u0420, \u0422\u0438\u0445\u043e\u043e\u043a\u0435\u0430\u043d\u0438\u0441\u043a\u0438\u0439 \u041d\u0430\u0443\u0447\u043d\u043e-\u0418\u0441\u0441\u043b\u0435\u0434\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u0441\u043a\u0438\u0439 \u0418\u043d\u0441\u0442\u0438\u0442\u0443\u0442 \u0420\u044b\u0431\u043d\u043e\u0433\u043e \u0425\u043e\u0437\u044f\u0439\u0441\u0442\u0432\u0430 \u0438 \u041e\u043a\u0435\u0430\u043d\u043e\u0433\u0440\u0430\u0444\u0438\u0438, \u0412\u043b\u0430\u0434\u0438\u0432\u043e\u0441\u0442\u043e\u043a'
        fields = [
            ("field1", "value1"),
            ("field2", "value2"),
            ("drilldown.field", "a drilldown value"),
            ("drilldown.field", longSpecialCharacterValue),
            ("drilldown.field", ['a', 'b']),
            ("drilldown.field", []),
            ("__key__.field", "a key value"),
            ("__key__.field1", 2),
        ]
        consume(index.add(identifier="", fieldslist=fields))
        self.assertEqual(['addDocument'], observer.calledMethodNames())
        fields = observer.calledMethods[0].kwargs['fields']
        self.assertEqual([
                {'name': 'field1', 'type': 'TextField', 'value': 'value1'},
                {'name': 'field2', 'type': 'TextField', 'value': 'value2'},
                {'name': 'drilldown.field', 'type': 'FacetField', 'path': ['a drilldown value']},
                {'name': 'drilldown.field', 'type': 'FacetField', 'path': [longSpecialCharacterValue]},
                {'name': 'drilldown.field', 'type': 'FacetField', 'path': ['a', 'b']},
                {'name': '__key__.field', 'type': 'KeyField', 'value': 'a key value'},
                {'name': '__key__.field1', 'type': 'KeyField', 'value': 2},
            ], fields)