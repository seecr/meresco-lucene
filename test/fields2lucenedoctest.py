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

from seecr.test import IntegrationTestCase, CallTrace
from meresco.lucene import Fields2LuceneDoc, DrilldownField
from meresco.core import Transaction
from weightless.core import consume
from org.apache.lucene.facet import FacetField


class Fields2LuceneDocTest(IntegrationTestCase):
    def testCreateDocument(self):
        fields = {
            'field1': ['value1'],
            'field2': ['value2', 'value2.1'],
            'sorted.field3': ['value3'],
            'untokenized.field4': ['value4'],
            '__key__.field5': ["12345"],
            '__numeric__.field6': ["12345"],
        }
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFields=[])
        observer = CallTrace(returnValues={'numerateTerm': 1})
        fields2LuceneDoc.addObserver(observer)
        document = fields2LuceneDoc._createDocument(fields)
        self.assertEquals(set(['field1', 'field2', 'sorted.field3', 'untokenized.field4', '__key__.field5', '__numeric__.field6']), set([f.name() for f in document.getFields()]))

        field1 = document.getField("field1")
        self.assertEquals('value1', field1.stringValue())
        self.assertTrue(field1.fieldType().indexed())
        self.assertFalse(field1.fieldType().stored())
        self.assertTrue(field1.fieldType().tokenized())

        self.assertEquals(['value2', 'value2.1'], document.getValues('field2'))

        field3 = document.getField("sorted.field3")
        self.assertEquals('value3', field3.stringValue())
        self.assertTrue(field3.fieldType().indexed())
        self.assertFalse(field3.fieldType().stored())
        self.assertFalse(field3.fieldType().tokenized())

        field4 = document.getField("untokenized.field4")
        self.assertEquals('value4', field4.stringValue())
        self.assertTrue(field4.fieldType().indexed())
        self.assertFalse(field4.fieldType().stored())
        self.assertFalse(field4.fieldType().tokenized())

        field5 = document.getField("__key__.field5")
        self.assertEquals(1, field5.numericValue().longValue())
        self.assertFalse(field5.fieldType().indexed())
        self.assertFalse(field5.fieldType().stored())
        self.assertTrue(field5.fieldType().tokenized())

        field6 = document.getField("__numeric__.field6")
        self.assertEquals(12345, field6.numericValue().longValue())
        self.assertFalse(field6.fieldType().indexed())
        self.assertFalse(field6.fieldType().stored())
        self.assertTrue(field6.fieldType().tokenized())

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
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFields=[
                DrilldownField('untokenized.field4'),
                DrilldownField('untokenized.field5'),
                DrilldownField('untokenized.field6'),
                DrilldownField('untokenized.field8', hierarchical=True),
            ])
        observer = CallTrace()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        for field, values in fields.items():
            for value in values:
                fields2LuceneDoc.addField(field, value)

        consume(fields2LuceneDoc.commit('unused'))

        document = observer.calledMethods[0].kwargs['document']
        facetsfields = [FacetField.cast_(f) for f in document.getFields() if FacetField.instance_(f)]
        self.assertEquals(6, len(facetsfields))
        self.assertEquals([
                ('untokenized.field8', ['grandparent', 'parent', 'child']),
                ('untokenized.field8', ['parent2', 'child']),
                ('untokenized.field6', ['value5/value6']),
                ('untokenized.field4', ['value4']),
                ('untokenized.field5', ['value5']),
                ('untokenized.field5', ['value6']),
            ], [(f.dim, list(f.path)) for f in facetsfields])
        self.assertEquals(['grandparent', 'grandparent/parent', 'grandparent/parent/child', 'parent2', 'parent2/child'], [f.stringValue() for f in document.getFields('untokenized.field8')])

    def testAddTimeStamp(self):
        fields = {'field1': ['value1']}
        fields2LuceneDoc = Fields2LuceneDoc('tsname', addTimestamp=True, drilldownFields=[])
        fields2LuceneDoc._time = lambda: 123456789
        document = fields2LuceneDoc._createDocument(fields)
        self.assertEquals(set(['field1', '__timestamp__']), set([f.name() for f in document.getFields()]))
        timestampField = document.getField("__timestamp__")
        self.assertEquals(123456789, timestampField.numericValue().intValue())
        self.assertTrue(timestampField.fieldType().indexed())
        self.assertFalse(timestampField.fieldType().stored())
        self.assertTrue(timestampField.fieldType().tokenized())

    def testAddDocument(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFields=[])
        observer = CallTrace()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field', 'value')
        consume(fields2LuceneDoc.commit('unused'))

        self.assertEquals(['addDocument'], observer.calledMethodNames())
        self.assertEquals('identifier', observer.calledMethods[0].kwargs['identifier'])

    def testRewriteIdentifier(self):
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFields=[], identifierRewrite=lambda identifier: "test:" + identifier)
        observer = CallTrace()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field', 'value')
        consume(fields2LuceneDoc.commit('unused'))

        self.assertEquals(['addDocument'], observer.calledMethodNames())
        self.assertEquals('test:identifier', observer.calledMethods[0].kwargs['identifier'])

    def testRewriteFields(self):
        def rewriteFields(fields):
            fields['keys'] = list(sorted(fields.keys()))
            return fields
        fields2LuceneDoc = Fields2LuceneDoc('tsname', drilldownFields=[], rewriteFields=rewriteFields)
        observer = CallTrace()
        fields2LuceneDoc.addObserver(observer)
        fields2LuceneDoc.ctx.tx = Transaction('tsname')
        fields2LuceneDoc.ctx.tx.locals['id'] = 'identifier'
        fields2LuceneDoc.addField('field1', 'value1')
        fields2LuceneDoc.addField('field2', 'value2')
        consume(fields2LuceneDoc.commit('unused'))
        self.assertEquals(['addDocument'], observer.calledMethodNames())
        doc = observer.calledMethods[0].kwargs['document']
        self.assertEquals(set(['field1', 'field2', 'keys']), set([f.name() for f in doc.getFields()]))
        self.assertEquals(['field1', 'field2'], [f.stringValue() for f in doc.getFields('keys')])

# TODO
#
# - copy isSingleValuedField from meresco.solr
# - add more prefixes for special fields
# - add more tests for transaction like stuff
