# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase
from cqlparser.cqltoexpression import QueryExpression, cqlToExpression
from cqlparser import parseString as parseCql, UnsupportedCQL
from meresco.lucene import LuceneSettings, DrilldownField
from meresco.lucene.fieldregistry import FieldRegistry, INTFIELD, LONGFIELD, NO_TERMS_FREQUENCY_FIELDTYPE
from meresco.lucene.queryexpressiontolucenequery import QueryExpressionToLuceneQuery
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.document import StringField
from org.apache.lucene.facet import DrillDownQuery
from org.apache.lucene.index import Term
from org.apache.lucene.search import TermQuery, BooleanQuery, BooleanClause, MatchAllDocsQuery, PhraseQuery, PrefixQuery, TermRangeQuery, NumericRangeQuery, WildcardQuery
from org.meresco.lucene.analysis import MerescoDutchStemmingAnalyzer

class QueryExpressionToLuceneQueryTest(SeecrTestCase):

    def testTermQuery(self):
        self.assertConversion(TermQuery(Term("field", "value")), QueryExpression.searchterm("field", "=", "value"))

    def testMatchAllQuery(self):
        self.assertConversion(MatchAllDocsQuery(), QueryExpression.searchterm(term="*"))

    def testUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0)]
        self.assertConversion(TermQuery(Term("aField", "value")), QueryExpression.searchterm(term="value"))

    def testMultipleUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0), ('oField', 2.0)]
        expected = BooleanQuery()
        q1 = TermQuery(Term("aField", "value"))
        q1.setBoost(1.0)
        expected.add(q1, BooleanClause.Occur.SHOULD)
        q2 = TermQuery(Term("oField", "value"))
        q2.setBoost(2.0)
        expected.add(q2, BooleanClause.Occur.SHOULD)
        self.assertConversion(expected, QueryExpression.searchterm(term="value"))

    def testBooleanAndQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        expected = BooleanQuery()
        expected.add(TermQuery(Term("field1", "value1")), BooleanClause.Occur.MUST)
        expected.add(TermQuery(Term("field2", "value2")), BooleanClause.Occur.MUST)
        self.assertConversion(expected, expr)

    def testBooleanOrQuery(self):
        expr = QueryExpression.nested(operator='OR')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        expected = BooleanQuery()
        expected.add(TermQuery(Term("field1", "value1")), BooleanClause.Occur.SHOULD)
        expected.add(TermQuery(Term("field2", "value2")), BooleanClause.Occur.SHOULD)
        self.assertConversion(expected, expr)

    def testBooleanNotQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        expr.operands[1].must_not = True
        expected = BooleanQuery()
        expected.add(TermQuery(Term("field1", "value1")), BooleanClause.Occur.MUST)
        expected.add(TermQuery(Term("field2", "value2")), BooleanClause.Occur.MUST_NOT)
        self.assertConversion(expected, expr)

    def testBooleanNotQueryNexted(self):
        expr = QueryExpression.nested(operator='AND')
        nestedNotExpr = QueryExpression.nested(operator='AND')
        nestedNotExpr.must_not = True
        nestedNotExpr.operands = [
            QueryExpression.searchterm("field2", "=", "value2"),
            QueryExpression.searchterm("field3", "=", "value3")
        ]
        expr.operands = [QueryExpression.searchterm("field1", "=", "value1"), nestedNotExpr]
        expected = BooleanQuery()
        expected.add(TermQuery(Term("field1", "value1")), BooleanClause.Occur.MUST)
        nested = BooleanQuery()
        nested.add(TermQuery(Term("field2", "value2")), BooleanClause.Occur.MUST)
        nested.add(TermQuery(Term("field3", "value3")), BooleanClause.Occur.MUST)
        expected.add(nested, BooleanClause.Occur.MUST_NOT)
        self.assertConversion(expected, expr)

    def testPhraseOutput(self):
        query = PhraseQuery()
        query.add(Term("unqualified", "cats"))
        query.add(Term("unqualified", "dogs"))
        self.assertConversion(query, QueryExpression.searchterm(term='"cats dogs"'))

    def testWhitespaceAnalyzer(self):
        self._analyzer = WhitespaceAnalyzer()
        query = PhraseQuery()
        query.add(Term("unqualified", "kat"))
        query.add(Term("unqualified", "hond"))
        self.assertConversion(query, cql='"kat hond"')

    def testPhraseOutputDoesNoDutchStemming(self):
        self._analyzer = MerescoDutchStemmingAnalyzer()
        query = PhraseQuery()
        query.add(Term("unqualified", "katten"))
        query.add(Term("unqualified", "honden"))
        self.assertConversion(query, cql='"katten honden"')

    def testDutchStemming(self):
        self._analyzer = MerescoDutchStemmingAnalyzer()
        query = BooleanQuery()
        query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
        query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
        self.assertConversion(query, cql='honden')

    def testDutchStemmingOnlyForGivenFields(self):
        self._analyzer = MerescoDutchStemmingAnalyzer(['unqualified'])
        query = BooleanQuery()
        query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
        query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
        self.assertConversion(query, cql='honden')

        query = TermQuery(Term("field", "honden"))
        self.assertConversion(query, cql='field=honden')

    def testIgnoreStemming(self):
        self._ignoredStemmingForWords = ['kate', 'wageningen']
        self._analyzer = MerescoDutchStemmingAnalyzer()
        query = TermQuery(Term("unqualified", "kate"))
        self.assertConversion(query, cql='kate')
        query = BooleanQuery()
        query.add(TermQuery(Term("unqualified", "katten")), BooleanClause.Occur.SHOULD)
        query.add(TermQuery(Term("unqualified", "kat")), BooleanClause.Occur.SHOULD)
        self.assertConversion(query, cql='katten')

    def testPhraseQueryIsStandardAnalyzed(self):
        expected = PhraseQuery()
        for term in ["vol.118", "2008", "nr.3", "march", "p.435-444"]:
            expected.add(Term("unqualified", term))
        input = '"vol.118 (2008) nr.3 (March) p.435-444"'
        self.assertConversion(expected, cql=input)

    def testOneTermPhraseQueryUsesStandardAnalyzed(self):
        expected = PhraseQuery()
        expected.add(Term('unqualified', 'aap'))
        expected.add(Term('unqualified', 'noot'))
        self.assertConversion(expected, cql='aap:noot')

    def testCreatesEmptyPhraseQueryIfNoValidCharsFound(self):
        expected = PhraseQuery()
        self.assertConversion(expected, cql=':')

    def testStandardAnalyserWithoutStopWords(self):
        expected = PhraseQuery()
        for term in ["no", "is", "the", "only", "option"]:
            expected.add(Term("unqualified", term))
        self.assertConversion(expected, cql='"no is the only option"')

    def testDiacritics(self):
        self.assertConversion(TermQuery(Term('title', 'moree')), cql='title=Moree')
        self.assertConversion(TermQuery(Term('title', 'moree')), cql='title=Morée')
        self.assertConversion(TermQuery(Term('title', 'moree')), cql='title=Morèe')

        self._analyzer = MerescoDutchStemmingAnalyzer()
        query = PhraseQuery()
        query.add(Term("title", "waar"))
        query.add(Term("title", "is"))
        query.add(Term("title", "moree"))
        query.add(Term("title", "vandaag"))
        self.assertConversion(query, cql='title="Waar is Morée vandaag"')

    def testDiacriticsShouldBeNormalizedNFC(self):
        pq = PhraseQuery()
        pq.add(Term("title", "more"))
        pq.add(Term("title", "e"))
        self.assertConversion(pq, cql='title=More\xcc\x81e') # Combined
        from unicodedata import normalize
        self.assertConversion(TermQuery(Term('title', 'moree')), cql=normalize('NFC', unicode('title=More\xcc\x81e')))

    def testIndexRelationTermOutput(self):
        self.assertConversion(TermQuery(Term("animal", "cats")), cql='animal=cats')
        query = PhraseQuery()
        query.add(Term("animal", "cats"))
        query.add(Term("animal", "dogs"))
        self.assertConversion(query, cql='animal="cats dogs"')
        self.assertConversion(query, cql='animal="catS Dogs"')

    def testIndexRelationExactTermOutput(self):
        self.assertConversion(TermQuery(Term("animal", "hairy cats")), cql='animal exact "hairy cats"')
        self.assertConversion(TermQuery(Term("animal", "Capital Cats")), cql='animal exact "Capital Cats"')

    def testBoost(self):
        query = TermQuery(Term("title", "cats"))
        query.setBoost(2.0)
        self.assertConversion(query, cql="title =/boost=2.0 cats")

    def testWildcards(self):
        query = PrefixQuery(Term('unqualified', 'prefix'))
        self.assertConversion(query, cql='prefix*')
        self.assertConversion(query, cql='PREfix*')
        query = PrefixQuery(Term('field', 'prefix'))
        self.assertConversion(query, cql='field="PREfix*"')
        self.assertConversion(query, cql='field=prefix*')
        query = PrefixQuery(Term('field', 'oc-0123'))
        self.assertConversion(query, cql='field="oc-0123*"')
        query = TermQuery(Term('field', 'p'))
        self.assertConversion(query, cql='field="P*"')
        #only prefix queries for now
        query = TermQuery(Term('field', 'post'))
        self.assertConversion(query, cql='field="*post"')

        query = TermQuery(Term('field', 'prefix'))
        self.assertConversion(query, cql='field=prefix**')

        self.unqualifiedFields = [("field0", 0.2), ("field1", 2.0)]

        query = BooleanQuery()
        left = PrefixQuery(Term("field0", "prefix"))
        left.setBoost(0.2)
        query.add(left, BooleanClause.Occur.SHOULD)

        right = PrefixQuery(Term("field1", "prefix"))
        right.setBoost(2.0)
        query.add(right, BooleanClause.Occur.SHOULD)
        self.assertConversion(query, cql="prefix*")

    def testMagicExact(self):
        exactResult = self.convert(cql='animal exact "cats dogs"')
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('animal', StringField.TYPE_NOT_STORED)
        self.assertConversion(exactResult, cql='animal = "cats dogs"')

    def testTextRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        self.assertConversion(TermRangeQuery.newStringRange('field', 'value', None, False, False), cql='field > value')
        self.assertConversion(TermRangeQuery.newStringRange('field', 'value', None, True, False), cql='field >= value')
        self.assertConversion(TermRangeQuery.newStringRange('field', None, 'value', False, False), cql='field < value')
        self.assertConversion(TermRangeQuery.newStringRange('field', None, 'value', False, True), cql='field <= value')

    def testIntRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        self.assertConversion(NumericRangeQuery.newIntRange('intField', 1, None, False, False), cql='intField > 1')
        self.assertConversion(NumericRangeQuery.newIntRange('intField', 1, None, True, False), cql='intField >= 1')
        self.assertConversion(NumericRangeQuery.newIntRange('intField', None, 3, False, False), cql='intField < 3')
        self.assertConversion(NumericRangeQuery.newIntRange('intField', None, 3, False, True), cql='intField <= 3')

    def testLongRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        self.assertConversion(NumericRangeQuery.newLongRange('longField', 1, None, False, False), cql='longField > 1')
        self.assertConversion(NumericRangeQuery.newLongRange('longField', 1, None, True, False), cql='longField >= 1')
        self.assertConversion(NumericRangeQuery.newLongRange('longField', None, 3, False, False), cql='longField < 3')
        self.assertConversion(NumericRangeQuery.newLongRange('longField', None, 3, False, True), cql='longField <= 3')

    def testDrilldownFieldQuery(self):
        self.fieldRegistry = FieldRegistry([DrilldownField('field')])
        self.assertConversion(TermQuery(DrillDownQuery.term("$facets", "field", "value")), cql="field = value")

    def testExcludeUnqualifiedFieldForWhichNoPhraseQueryIsPossibleInCaseOfPhraseQuery(self):
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('noTermFreqField', NO_TERMS_FREQUENCY_FIELDTYPE)
        self.unqualifiedFields = [("unqualified", 1.0), ('noTermFreqField', 2.0)]
        expected = PhraseQuery()
        expected.add(Term("unqualified", "phrase query"))
        self.assertConversion(expected, cql='"phrase query"')

    def testQueryForIntField(self):
        expected = NumericRangeQuery.newIntRange("intField", 5, 5, True, True)
        self.assertConversion(expected, cql="intField=5")

        expected = NumericRangeQuery.newIntRange("intField", 5, 5, True, True)
        self.assertConversion(expected, cql="intField exact 5")

    def testQueryForLongField(self):
        expected = NumericRangeQuery.newLongRange("longField", long(5), long(5), True, True)
        self.assertConversion(expected, cql="longField=5")

    def testQueryForDoubleField(self):
        expected = NumericRangeQuery.newDoubleRange("range.double.field", float(5), float(5), True, True)
        self.assertConversion(expected, cql="range.double.field=5")

    def testCreateDrilldownQuery(self):
        self.fieldRegistry = FieldRegistry(drilldownFields=[DrilldownField('dd-field')])
        expected = TermQuery(self.fieldRegistry.makeDrilldownTerm("dd-field", "VALUE"))
        self.assertConversion(expected, cql='dd-field exact VALUE')
        self.assertConversion(expected, cql='dd-field=VALUE')

    def testWildcardQuery(self):
        self.fieldRegistry = FieldRegistry()
        expected = WildcardQuery(Term("field", "???*"))
        self.assertConversion(expected, cql='field=???*')

    def testUnsupportedCQL(self):
        for relation in ['<>']:
            try:
                self.convert(cql='index %(relation)s term' % locals())
                self.fail()
            except UnsupportedCQL:
                pass

    def convert(self, expression=None, cql=None):
        if expression is None:
            expression = cqlToExpression(parseCql(cql))
        unqualifiedFields = getattr(self, 'unqualifiedFields', [("unqualified", 1.0)])
        settings = LuceneSettings()
        if hasattr(self, '_analyzer'):
            settings.analyzer = self._analyzer
        if hasattr(self, 'fieldRegistry'):
            settings.fieldRegistry = self.fieldRegistry
        else:
            settings.fieldRegistry = FieldRegistry()
            settings.fieldRegistry.register("intField", fieldDefinition=INTFIELD)
            settings.fieldRegistry.register("longField", fieldDefinition=LONGFIELD)
        converter = QueryExpressionToLuceneQuery(
                unqualifiedTermFields=unqualifiedFields,
                luceneSettings=settings,
                ignoreStemmingForWords=getattr(self, '_ignoredStemmingForWords', None)
            )
        return converter.convert(expression)

    def assertConversion(self, expected, expression=None, cql=None):
        result = self.convert(expression=expression, cql=cql)
        self.assertEquals(type(expected), type(result), "expected %s, but got %s" % (repr(expected), repr(result)))
        self.assertEquals(repr(expected), repr(result))
