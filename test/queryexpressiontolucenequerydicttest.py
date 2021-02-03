# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016, 2018, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016, 2018, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from unittest import skip
from seecr.test import SeecrTestCase

from cqlparser import cqlToExpression, parseString as parseCql, UnsupportedCQL
from cqlparser.cqltoexpression import QueryExpression

from meresco.lucene import LuceneSettings, DrilldownField
from meresco.lucene.composedquery import ComposedQuery
from meresco.lucene.fieldregistry import NO_TERMS_FREQUENCY_FIELD, FieldRegistry, LONGFIELD, INTFIELD, STRINGFIELD
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict


class QueryExpressionToLuceneQueryDictTest(SeecrTestCase):
    def testTermQuery(self):
        self.assertEqual(
            {
                "type": "TermQuery",
                "term": {
                    "field":"field",
                    "value": "value",
                }
            }, self._convert(QueryExpression.searchterm("field", "=", "value")))
        self.assertEqual(
            {"term": {"field": "field", "value": "value"}, "type": "TermQuery"}, self._convert(QueryExpression.searchterm("field", "=", "value")))

    def testRightHandSideIsLowercase(self):
        self.assertEqual(
            {'boost': 1.0, 'term': {'field': 'unqualified', 'value': 'cat'}, 'type': 'TermQuery'},
            self._convert(QueryExpression.searchterm(term="CaT")))

    def testOneTermOutputWithANumber(self):
        self.assertEqual(
            {'boost': 1.0, 'term': {'field': 'unqualified', 'value': '2005'}, 'type': 'TermQuery'},
            self._convert(QueryExpression.searchterm(term="2005")))

    def testMatchAllQuery(self):
        self.assertEqual(
            {"type": "MatchAllDocsQuery"}, self._convert(QueryExpression.searchterm(term="*")))

    def testUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0)]
        self.assertEqual(
            {"type": "TermQuery", "term": {"field": "aField", "value": "value"}, 'boost': 1.0},
            self._convert(QueryExpression.searchterm(term="value")))

    def testUnqualifiedTermFieldsWithNestedExpression(self):
        self.unqualifiedFields = [('aField', 1.0)]
        expr = QueryExpression.nested(operator='AND')
        expr.operands = [
            QueryExpression.searchterm(term="value1"),
            QueryExpression.searchterm(term="value2")
        ]
        self.assertEqual({
                'type': 'BooleanQuery',
                'clauses': [
                    {'type': 'TermQuery', 'occur': 'MUST', 'term': {'field': 'aField', 'value': 'value1'}, 'boost': 1.0},
                    {'type': 'TermQuery', 'occur': 'MUST', 'term': {'field': 'aField', 'value': 'value2'}, 'boost': 1.0}
                ],
            },
            self._convert(expr))

    def testMultipleUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0), ('oField', 2.0)]
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "TermQuery",
                        "term": {"field": "aField", "value": "value"},
                        "boost": 1.0,
                        "occur": "SHOULD"
                    }, {
                        "type": "TermQuery",
                        "term": {"field": "oField", "value": "value"},
                        "boost": 2.0,
                        "occur": "SHOULD"
                    }
                ]
            }, self._convert(QueryExpression.searchterm(term="value")))

    def testBooleanAndQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands = [
            QueryExpression.searchterm("field1", "=", "value1"),
            QueryExpression.searchterm("field2", "=", "value2")
        ]
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "TermQuery",
                        "term": {"field": "field1", "value": "value1"},
                        "occur": "MUST"
                    }, {
                        "type": "TermQuery",
                        "term": {"field": "field2", "value": "value2"},
                        "occur": "MUST"
                    }
                ]
            }, self._convert(expr))

    def testBooleanOrQuery(self):
        expr = QueryExpression.nested(operator='OR')
        expr.operands=[
            QueryExpression.searchterm("field1", "=", "value1"),
            QueryExpression.searchterm("field2", "=", "value2")
        ]
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "TermQuery",
                        "term": {"field": "field1", "value": "value1"},
                        "occur": "SHOULD"
                    }, {
                        "type": "TermQuery",
                        "term": {"field": "field2", "value": "value2"},
                        "occur": "SHOULD"
                    }
                ]
            }, self._convert(expr))

    def testBooleanNotQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands=[
            QueryExpression.searchterm("field1", "=", "value1"),
            QueryExpression.searchterm("field2", "=", "value2")
        ]
        expr.operands[1].must_not = True
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "TermQuery",
                        "term": {"field": "field1", "value": "value1"},
                        "occur": "MUST"
                    }, {
                        "type": "TermQuery",
                        "term": {"field": "field2", "value": "value2"},
                        "occur": "MUST_NOT"
                    }
                ]
            }, self._convert(expr))

    def testBooleanNotQueryNested(self):
        expr = QueryExpression.nested(operator='AND')
        nestedNotExpr = QueryExpression.nested(operator='AND')
        nestedNotExpr.must_not = True
        nestedNotExpr.operands = [
            QueryExpression.searchterm("field2", "=", "value2"),
            QueryExpression.searchterm("field3", "=", "value3")
        ]
        expr.operands = [QueryExpression.searchterm("field1", "=", "value1"), nestedNotExpr]
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "TermQuery",
                        "term": {"field": "field1", "value": "value1"},
                        "occur": "MUST"
                    }, {
                        "type": "BooleanQuery",
                        "occur": "MUST_NOT",
                        "clauses": [
                            {
                                "type": "TermQuery",
                                "term": {"field": "field2", "value": "value2"},
                                "occur": "MUST"
                            },
                            {
                                "type": "TermQuery",
                                "term": {"field": "field3", "value": "value3"},
                                "occur": "MUST"
                            }
                        ]
                    }
                ]
            }, self._convert(expr))

    def testNotExpression(self):
        expr = QueryExpression.searchterm("field", "=", "value")
        expr.must_not = True
        self.assertEqual(
            {
                "type": "BooleanQuery",
                "clauses": [
                    {
                        "type": "MatchAllDocsQuery",
                        "occur": "MUST"
                    }, {
                        "type": "TermQuery",
                        "term": {"field": "field", "value": "value"},
                        "occur": "MUST_NOT"
                    }
                ]
            }, self._convert(expr))

    def testPhraseOutput(self):
        self.assertEqual(
            {
                "type": "PhraseQuery",
                "boost": 1.0,
                "terms": [
                    {"field": "unqualified", "value": "cats"},
                    {"field": "unqualified", "value": "dogs"}
                ]
            }, self._convert(QueryExpression.searchterm(term='"cats dogs"')))

    # def testWhitespaceAnalyzer(self):
    #     self._analyzer = WhitespaceAnalyzer()
    #     query = PhraseQuery()
    #     query.add(Term("unqualified", "kat"))
    #     query.add(Term("unqualified", "hond"))
    #     self.assertEquals(query, self._convert('"kat hond"'))

    # def testPhraseOutputDoesNoDutchStemming(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = PhraseQuery()
    #     query.add(Term("unqualified", "katten"))
    #     query.add(Term("unqualified", "honden"))
    #     self.assertEquals(query, self._convert('"katten honden"'))

    # def testDutchStemming(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
    #     self.assertEquals(query, self._convert('honden'))

    # def testDutchStemmingOnlyForGivenFields(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer(['unqualified'])
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
    #     self.assertEquals(query, self._convert('honden'))

    #     query = TermQuery(Term("field", "honden"))
    #     self.assertEquals(query, self._convert('field=honden'))

    # def testIgnoreStemming(self):
    #     self._ignoredStemmingForWords = ['kate', 'wageningen']
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = TermQuery(Term("unqualified", "kate"))
    #     self.assertEquals(query, 'kate')
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "katten")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "kat")), BooleanClause.Occur.SHOULD)
    #     self.assertEquals(query, self._convert('katten'))

    def testPhraseQueryIsStandardAnalyzed(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        for term in ["vol.118", "2008", "nr.3", "march", "p.435-444"]:
            expected["terms"].append(dict(field="unqualified", value=term))
        self.assertEqual(expected, self._convert('"vol.118 (2008) nr.3 (March) p.435-444"'))

    def testOneTermPhraseQueryUsesStandardAnalyzed(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        expected["terms"].append(dict(field="unqualified", value='aap'))
        expected["terms"].append(dict(field="unqualified", value='noot'))
        self.assertEqual(expected, self._convert('aap:noot'))

    def testCreatesEmptyPhraseQueryIfNoValidCharsFound(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        self.assertEqual(expected, self._convert(':'))

    def testStandardAnalyserWithoutStopWords(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        for term in ["no", "is", "the", "only", "option"]:
            expected["terms"].append(dict(field="unqualified", value=term))
        self.assertEqual(expected, self._convert('"no is the only option"'))

    def testDiacritics(self):
        expected = termQuery('title', 'moree')
        self.assertEqual(expected, self._convert('title=Moree'))
        self.assertEqual(expected, self._convert('title=Morée'))
        self.assertEqual(expected, self._convert('title=Morèe'))

        # self._analyzer = MerescoDutchStemmingAnalyzer()
        # query = PhraseQuery()
        # query.add(Term("title", "waar"))
        # query.add(Term("title", "is"))
        # query.add(Term("title", "moree"))
        # query.add(Term("title", "vandaag"))
        # self.assertEquals(query, self._convert('title="Waar is Morée vandaag"'))

    def testDiacriticsShouldBeNormalizedNFC(self):
        pq = dict(type="PhraseQuery", terms=[])
        pq["terms"].append(dict(field="title", value="more"))
        pq["terms"].append(dict(field="title", value="e"))
        self.assertEqual(pq, self._convert(b'title=More\xcc\x81e'.decode())) # Combined
        from unicodedata import normalize
        self.assertEqual('title=Mor\xe9e', normalize('NFC', b'title=More\xcc\x81e'.decode()))
        self.assertEqual(
            termQuery('title', 'moree'),
            self._convert(normalize('NFC', b'title=More\xcc\x81e'.decode())))

    def testIndexRelationTermOutput(self):
        self.assertEqual(
            termQuery('animal', 'cats'),
            self._convert('animal=cats'))
        query = dict(type="PhraseQuery", terms=[])
        query["terms"].append(dict(field="animal", value="cats"))
        query["terms"].append(dict(field="animal", value="dogs"))
        self.assertEqual(query, self._convert('animal="cats dogs"'))
        self.assertEqual(query, self._convert('animal="catS Dogs"'))

    def testIndexRelationExactTermOutput(self):
        self.assertEqual(
            termQuery("animal", "hairy cats"),
            self._convert('animal exact "hairy cats"'))
        self.assertEqual(
            termQuery("animal", "Capital Cats"),
            self._convert('animal exact "Capital Cats"'))

    def testBoost(self):
        query = termQuery("title", "cats", boost=2.0)
        self.assertEqual(query, self._convert("title =/boost=2.0 cats"))

    def testWildcards(self):
        query = prefixQuery('unqualified', 'prefix', 1.0)
        self.assertEqual(query, self._convert('prefix*'))
        self.assertEqual(query, self._convert('PREfix*'))
        query = prefixQuery('field', 'prefix')
        self.assertEqual(query, self._convert('field="PREfix*"'))
        self.assertEqual(query, self._convert('field=prefix*'))
        query = prefixQuery('field', 'oc-0123')
        self.assertEqual(query, self._convert('field="oc-0123*"'))
        query = termQuery('field', 'p')
        self.assertEqual(query, self._convert('field="P*"'))
        #only prefix queries for now
        query = termQuery('field', 'post')
        self.assertEqual(query, self._convert('field="*post"'))

        query = termQuery('field', 'prefix')
        self.assertEqual(query, self._convert('field=prefix**'))

        self.unqualifiedFields = [("field0", 0.2), ("field1", 2.0)]

        query = dict(type="BooleanQuery", clauses=[])
        query["clauses"].append(prefixQuery("field0", "prefix", 0.2))
        query["clauses"][0]["occur"] = "SHOULD"

        query["clauses"].append(prefixQuery("field1", "prefix", 2.0))
        query["clauses"][1]["occur"] = "SHOULD"
        self.assertEqual(query, self._convert("prefix*"))

    def testMagicExact(self):
        exactResult = self._convert('animal exact "cats dogs"')
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('animal', STRINGFIELD)
        self.assertEqual(exactResult, self._convert('animal = "cats dogs"'))

    def testTextRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm='value', upperTerm=None, includeLower=False, includeUpper=True)
        self.assertEqual(q, self._convert('field > value'))
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm='value', upperTerm=None, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('field >= value'))
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm=None, upperTerm='value', includeLower=True, includeUpper=False)
        self.assertEqual(q, self._convert('field < value'))
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm=None, upperTerm='value', includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('field <= value'))

    def testIntRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=1, upperTerm=None, includeLower=False, includeUpper=True)
        self.assertEqual(q, self._convert('intField > 1'))
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=1, upperTerm=None, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('intField >= 1'))
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=False)
        self.assertEqual(q, self._convert('intField < 3'))
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('intField <= 3'))
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=3, upperTerm=3, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('intField = 3'))
        self.assertEqual(q, self._convert(QueryExpression.searchterm(index='intField', relation='exact', term=3)))
        self.assertEqual(q, self._convert(QueryExpression.searchterm(index='intField', relation='=', term=3)))

    def testLongRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=1, upperTerm=None, includeLower=False, includeUpper=True)
        self.assertEqual(q, self._convert('longField > 1'))
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=1, upperTerm=None, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('longField >= 1'))
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=False)
        self.assertEqual(q, self._convert('longField < 3'))
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=True)
        self.assertEqual(q, self._convert('longField <= 3'))

    def testDrilldownFieldQuery(self):
        self.fieldRegistry = FieldRegistry([DrilldownField('field', hierarchical=True)])
        self.assertEqual(
            dict(type="TermQuery", term=dict(field="field", path=["value"], type="DrillDown")),
            self._convert("field = value"))
        self.assertEqual(
            dict(type="TermQuery", term=dict(field="field", path=["value", "value1"], type="DrillDown")),
            self._convert("field = \"value>value1\""))

    def testExcludeUnqualifiedFieldForWhichNoPhraseQueryIsPossibleInCaseOfPhraseQuery(self):
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('noTermFreqField', NO_TERMS_FREQUENCY_FIELD)
        self.unqualifiedFields = [("unqualified", 1.0), ('noTermFreqField', 2.0)]
        expected = dict(type="PhraseQuery", terms=[
            dict(field="unqualified", value="phrase"),
            dict(field="unqualified", value="query")
        ], boost=1.0)
        self.assertEqual(expected, self._convert('"phrase query"'))

    def testQueryForIntField(self):
        expected = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=5, upperTerm=5, includeLower=True, includeUpper=True)
        self.assertEqual(expected, self._convert("intField=5"))

        expected = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=5, upperTerm=5, includeLower=True, includeUpper=True)
        self.assertEqual(expected, self._convert("intField exact 5"))

    def testQueryForLongField(self):
        expected = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=int(5), upperTerm=int(5), includeLower=True, includeUpper=True)
        self.assertEqual(expected, self._convert("longField=5"))

    def testQueryForDoubleField(self):
        expected = dict(type="RangeQuery", rangeType="Double", field='range.double.field', lowerTerm=float(5), upperTerm=float(5), includeLower=True, includeUpper=True)
        self.assertEqual(expected, self._convert("range.double.field=5"))

    def testWildcardQuery(self):
        self.fieldRegistry = FieldRegistry()
        expected = dict(type="WildcardQuery", term=dict(field="field", value="???*"))
        self.assertEqual(expected, self._convert('field=???*'))

    def testUnsupportedCQL(self):
        for relation in ['<>']:
            try:
                self._convert('index %(relation)s term' % locals())
                self.fail()
            except UnsupportedCQL:
                pass

    def testPerQueryUnqualifiedFields(self):
        self.unqualifiedFields = [('aField', 1.0)]
        converter = self._prepareConverter()
        self.assertEqual({
            "type": "BooleanQuery",
            "clauses": [{
                    "type": "TermQuery",
                    "term": {"field": "aField", "value": "value"},
                    'boost': 2.0,
                    'occur': 'SHOULD'
                }, {
                    "type": "TermQuery",
                    "term": {"field": "anotherField", "value": "value"},
                    'boost': 3.0,
                    'occur': 'SHOULD'
            }]},
            converter.convert(
                QueryExpression.searchterm(term="value"),
                unqualifiedTermFields=[('aField', 2.0), ('anotherField', 3.0)]))

    def testReallyIgnoreAnalyzedAwayTerms(self):
        self.assertEqual({'boost': 1.0, 'terms': [], 'type': 'PhraseQuery'}, self._convert('.'))  # will not yield any results, but that's what's desired
        self.assertDictEquals({'terms': [], 'type': 'PhraseQuery'}, self._convert("abc=:;+"))

        self.assertDictEquals({'type': 'BooleanQuery', 'clauses': [{'boost': 1.0, 'term': {'field': 'unqualified', 'value': 'abc'}, 'type': 'TermQuery', 'occur': 'MUST'}, {'boost': 1.0, 'term': {'field': 'unqualified', 'value': 'def'}, 'type': 'TermQuery', 'occur': 'MUST'}]}, self._convert("abc AND :;+ AND def"))

        self.unqualifiedFields = [("unqualified", 1.0), ("moreUnqualified", 1.0)]
        self.assertDictEquals({
            'clauses': [{
                'clauses': [{
                    'boost': 1.0,
                    'occur': 'SHOULD',
                    'term': {'field': 'unqualified', 'value': 'abc'},
                    'type': 'TermQuery'
                }, {
                    'boost': 1.0,
                    'occur': 'SHOULD',
                    'term': {'field': 'moreUnqualified', 'value': 'abc'},
                    'type': 'TermQuery'
                }],
                'occur': 'MUST',
                'type': 'BooleanQuery'
            }, {
                'clauses': [{
                    'boost': 1.0,
                    'occur': 'SHOULD',
                    'term': {'field': 'unqualified', 'value': 'def'},
                    'type': 'TermQuery'
                 }, {
                    'boost': 1.0,
                    'occur': 'SHOULD',
                    'term': {'field': 'moreUnqualified', 'value': 'def'},
                    'type': 'TermQuery'
                }],
                'occur': 'MUST',
                'type': 'BooleanQuery'
            }],
            'type': 'BooleanQuery'}, self._convert("abc AND :;+ AND def"))

    def testOtherCoreTermQuery(self):
        query = ComposedQuery('thisCore')
        query.cores.add('otherCore')
        query.addMatch(
            dict(core='thisCore', uniqueKey='A'),
            dict(core='otherCore', uniqueKey='B')
        )
        self.assertEqual({
            "type": "RelationalLuceneQuery",  # should this not be 'joined' to own core somehow? (with MatchAllDocs)
            "core": "otherCore",
            "collectKeyName": "B",
            "filterKeyName": "B",
            "query": {
                "type": "TermQuery",
                "term": {
                    "field": "field",
                    "value": "value",
                }
            }}, self._convert(QueryExpression.searchterm("otherCore.field", "=", "value"), composedQuery=query))

    @skip('not yet implemented')
    def testOtherCoreAndQuery(self):
        self.assertEqual({
            'type': 'JoinAndQuery',
            'first': {
                "type": "RelationalLuceneQuery",  # should this not be 'joined' to own core somehow?
                "core": "thisCore",
                "collectKeyName": "A",  # where does this keyName come from?
                "filterKeyName": "A",
                "query": {
                    "type": "TermQuery",
                    "term": {
                        "field":"field0",
                        "value": "value",
                    }
                }
            },
            'second': {
                "type": "RelationalLuceneQuery",  # should this not be 'joined' to own core somehow?
                "core": "otherCore",
                "collectKeyName": "A",  # where does this keyName come from?
                "filterKeyName": "A",
                "query": {
                    "type": "TermQuery",
                    "term": {
                        "field":"field",
                        "value": "value",
                    }
                }
            }
        }, self._convert(
                QueryExpression(operator='AND', operands=[
                    QueryExpression.searchterm('field0', '=', 'value'),
                    QueryExpression.searchterm("otherCore.field", "=", "value")
                ])
            )
        )


    def _convert(self, input, **kwargs):
        return self._prepareConverter().convert(self._makeExpression(input), **kwargs)

    def _prepareConverter(self):
        unqualifiedFields = getattr(self, 'unqualifiedFields', [("unqualified", 1.0)])
        return QueryExpressionToLuceneQueryDict(
            unqualifiedTermFields=unqualifiedFields,
            luceneSettings=self._prepareLuceneSettings(),
            ignoreStemmingForWords=getattr(self, '_ignoredStemmingForWords', None)
        )

    def _prepareLuceneSettings(self):
        settings = LuceneSettings()
        if hasattr(self, '_analyzer'):
            settings.analyzer = self._analyzer
        if hasattr(self, 'fieldRegistry'):
            settings.fieldRegistry = self.fieldRegistry
        else:
            settings.fieldRegistry = FieldRegistry()
            settings.fieldRegistry.register("intField", fieldDefinition=INTFIELD)
            settings.fieldRegistry.register("longField", fieldDefinition=LONGFIELD)
        return settings

    def _makeExpression(self, input):
        return cqlToExpression(parseCql(input)) if isinstance(input, str) else input


def termQuery(field, value, boost=None):
    q = dict(type="TermQuery", term=dict(field=field, value=value))
    if boost:
        q["boost"] = boost
    return q

def prefixQuery(field, value, boost=None):
    q = dict(type="PrefixQuery", term=dict(field=field, value=value))
    if boost:
        q["boost"] = boost
    return q
