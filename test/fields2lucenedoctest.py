## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
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

from seecr.test import IntegrationTestCase, CallTrace
from meresco.lucene import Fields2LuceneDoc, DrilldownField
from meresco.core import Transaction
from weightless.core import consume
from meresco.lucene.fieldregistry import FieldRegistry

class Fields2LuceneDocTest(IntegrationTestCase):
    maxDiff = None

    def testCreateDocument(self):
        fields = {
            'field1': ['value1'],
            'field2': ['value2', 'value2.1'],
            'sorted.field3': ['value3'],
            'untokenized.field4': ['value4'],
            '__key__.field5': [12345],
            '__numeric__.field6': [12345],
        }
        fields2LuceneDoc = Fields2LuceneDoc('tsname', fieldRegistry=FieldRegistry())
        fields = fields2LuceneDoc._createFields(fields)
        self.assertEqual([
                {
                    "name": "__key__.field5",
                    "type": "KeyField",
                    "value": 12345
                },
                {
                    "name": "__numeric__.field6",
                    "type": "NumericField",
                    "value": 12345
                },
                {
                    "name": "field1",
                    "type": "TextField",
                    "value": "value1"
                },
                {
                    "name": "field2",
                    "type": "TextField",
                    "value": "value2"
                },
                {
                    "name": "field2",
                    "type": "TextField",
                    "value": "value2.1"
                },
                {
                    "name": "sorted.field3",
                    "type": "StringField",
                    "value": "value3",
                    "sort": True,
                },
                {
                    "name": "untokenized.field4",
                    "type": "StringField",
                    "value": "value4"
                },
            ], sorted(fields, key=lambda d:(d['name'],d['value'])))

    def testCreateFacet(self):
        fields = {
            'field1': ['value1'],
            'sorted.field3': ['value3'],
            'untokenized.field4': ['value4'],
            'untokenized.field5': ['value5', 'value6'],
            'untokenized.field6': ['value5/value6'],
            'untokenized.field7': ['valuex'],
            'untokenized.field8': [['grandparent', 'parent', 'child'], ['parent2', 'child']]
        }
        fields2LuceneDoc = Fields2LuceneDoc('tsname',
            fieldRegistry=FieldRegistry(drilldownFields=[
                DrilldownField('untokenized.field4'),
                DrilldownField('untokenized.field5'),
                DrilldownField('untokenized.field6'),
                DrilldownField('untokenized.field8', hierarchical=True),
            ])
        )
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        for field, values in fields.items():
            for value in values:
                fields2LuceneDoc.addField(field, value)

        consume(fields2LuceneDoc.commit('unused'))

        fields = observer.calledMethods[0].kwargs['fields']

        searchFields = [f for f in fields if not "path" in f]
        self.assertEqual(['field1', 'sorted.field3', 'untokenized.field7'], [f['name'] for f in searchFields])

        facetsFields = [f for f in fields if "path" in f]
        self.assertEqual(6, len(facetsFields))
        self.assertEqual([
                ('untokenized.field4', ['value4']),
                ('untokenized.field5', ['value5']),
                ('untokenized.field5', ['value6']),
                ('untokenized.field6', ['value5/value6']),
                ('untokenized.field8', ['grandparent', 'parent', 'child']),
                ('untokenized.field8', ['parent2', 'child']),
            ], [(f['name'], f['path']) for f in facetsFields])

    def testAddFacetField(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname',
            fieldRegistry=FieldRegistry(drilldownFields=[
                DrilldownField('untokenized.field'),
            ])
        )
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field', 'value')
        fields2LuceneDoc.addFacetField('untokenized.field', 'untokenized value')
        consume(fields2LuceneDoc.commit('unused'))
        fields = observer.calledMethods[0].kwargs['fields']
        facetsFields = [f for f in fields if "path" in f]
        self.assertEqual(1, len(facetsFields))

    def testOnlyOneSortValueAllowed(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname',
            fieldRegistry=FieldRegistry()
        )
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('sorted.field', 'value1')
        fields2LuceneDoc.addField('sorted.field', 'value2')
        consume(fields2LuceneDoc.commit('unused'))
        fields = observer.calledMethods[0].kwargs['fields']
        self.assertEqual(1, len(fields))
        self.assertEqual({'sort': True, 'type': 'StringField', 'name': 'sorted.field', 'value': 'value1'}, fields[0])

    def testAddDocument(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname', fieldRegistry=FieldRegistry())
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field', 'value')
        consume(fields2LuceneDoc.commit('unused'))

        self.assertEqual(['addDocument'], observer.calledMethodNames())
        self.assertEqual('identifier', observer.calledMethods[0].kwargs['identifier'])

    def testRewriteIdentifier(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname',
            fieldRegistry=FieldRegistry(),
            identifierRewrite=lambda identifier: "test:" + identifier)
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field', 'value')
        consume(fields2LuceneDoc.commit('unused'))

        self.assertEqual(['addDocument'], observer.calledMethodNames())
        self.assertEqual('test:identifier', observer.calledMethods[0].kwargs['identifier'])

    def testRewriteFields(self):
        def rewriteFields(fields):
            fields['keys'] = list(sorted(fields.keys()))
            return fields
        fields2LuceneDoc = Fields2LuceneDoc('tsname', rewriteFields=rewriteFields, fieldRegistry=FieldRegistry())
        observer = callTraceObserver()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field1', 'value1')
        fields2LuceneDoc.addField('field2', 'value2')
        consume(fields2LuceneDoc.commit('unused'))
        self.assertEqual(['addDocument'], observer.calledMethodNames())
        fields = observer.calledMethods[0].kwargs['fields']
        self.assertEqual(set(['field1', 'field2', 'keys']), set([f['name'] for f in fields]))
        self.assertEqual(['field1', 'field2'], [f['value'] for f in fields if f['name'] == 'keys'])

callTraceObserver = lambda: CallTrace(emptyGeneratorMethods=['addDocument'])
