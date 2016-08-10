# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

from meresco.lucene import LuceneSettings, DrilldownField
from meresco.lucene.fieldregistry import NO_TERMS_FREQUENCY_FIELD, FieldRegistry, LONGFIELD, INTFIELD, STRINGFIELD
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict
from cqlparser import cqlToExpression, parseString as parseCql, UnsupportedCQL
from cqlparser.cqltoexpression import QueryExpression
from seecr.test import SeecrTestCase


class QueryExpressionToLuceneQueryDictTest(SeecrTestCase):
    def testTermQuery(self):
        self.assertConversion({
            "type": "TermQuery",
            "term": {
                "field":"field",
                "value": "value",
            }
        }, QueryExpression.searchterm("field", "=", "value"))
        self.assertConversion({"term": {"field": "field", "value": "value"}, "type": "TermQuery"}, QueryExpression.searchterm("field", "=", "value"))


    def testRightHandSideIsLowercase(self):
        self.assertConversion({'boost': 1.0, 'term': {'field': 'unqualified', 'value': 'cat'}, 'type': 'TermQuery'}, QueryExpression.searchterm(term="CaT"))

    def testOneTermOutputWithANumber(self):
        self.assertConversion({'boost': 1.0, 'term': {'field': 'unqualified', 'value': '2005'}, 'type': 'TermQuery'}, QueryExpression.searchterm(term="2005"))

    def testMatchAllQuery(self):
        self.assertConversion({"type": "MatchAllDocsQuery"}, QueryExpression.searchterm(term="*"))

    def testUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0)]
        self.assertConversion({"type": "TermQuery", "term": {"field": "aField", "value": "value"}, 'boost': 1.0}, QueryExpression.searchterm(term="value"))

    def testMultipleUnqualifiedTermFields(self):
        self.unqualifiedFields = [('aField', 1.0), ('oField', 2.0)]
        self.assertConversion({
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
            }, QueryExpression.searchterm(term="value"))

    def testBooleanAndQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        self.assertConversion({
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
            }, expr)

    def testBooleanOrQuery(self):
        expr = QueryExpression.nested(operator='OR')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        self.assertConversion({
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
            }, expr)

    def testBooleanNotQuery(self):
        expr = QueryExpression.nested(operator='AND')
        expr.operands=[
                QueryExpression.searchterm("field1", "=", "value1"),
                QueryExpression.searchterm("field2", "=", "value2")
            ]
        expr.operands[1].must_not = True
        self.assertConversion({
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
            }, expr)

    def testBooleanNotQueryNested(self):
        expr = QueryExpression.nested(operator='AND')
        nestedNotExpr = QueryExpression.nested(operator='AND')
        nestedNotExpr.must_not = True
        nestedNotExpr.operands = [
            QueryExpression.searchterm("field2", "=", "value2"),
            QueryExpression.searchterm("field3", "=", "value3")
        ]
        expr.operands = [QueryExpression.searchterm("field1", "=", "value1"), nestedNotExpr]
        self.assertConversion({
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
            }, expr)

    def testNotExpression(self):
        expr = QueryExpression.searchterm("field", "=", "value")
        expr.must_not = True
        self.assertConversion({
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
            }, expr)
    def testPhraseOutput(self):
        self.assertConversion({
                "type": "PhraseQuery",
                "boost": 1.0,
                "terms": [
                    {"field": "unqualified", "value": "cats"},
                    {"field": "unqualified", "value": "dogs"}
                ]
            }, QueryExpression.searchterm(term='"cats dogs"'))

    # def testWhitespaceAnalyzer(self):
    #     self._analyzer = WhitespaceAnalyzer()
    #     query = PhraseQuery()
    #     query.add(Term("unqualified", "kat"))
    #     query.add(Term("unqualified", "hond"))
    #     self.assertConversion(query, cql='"kat hond"')

    # def testPhraseOutputDoesNoDutchStemming(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = PhraseQuery()
    #     query.add(Term("unqualified", "katten"))
    #     query.add(Term("unqualified", "honden"))
    #     self.assertConversion(query, cql='"katten honden"')

    # def testDutchStemming(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
    #     self.assertConversion(query, cql='honden')

    # def testDutchStemmingOnlyForGivenFields(self):
    #     self._analyzer = MerescoDutchStemmingAnalyzer(['unqualified'])
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "honden")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "hond")), BooleanClause.Occur.SHOULD)
    #     self.assertConversion(query, cql='honden')

    #     query = TermQuery(Term("field", "honden"))
    #     self.assertConversion(query, cql='field=honden')

    # def testIgnoreStemming(self):
    #     self._ignoredStemmingForWords = ['kate', 'wageningen']
    #     self._analyzer = MerescoDutchStemmingAnalyzer()
    #     query = TermQuery(Term("unqualified", "kate"))
    #     self.assertConversion(query, cql='kate')
    #     query = BooleanQuery()
    #     query.add(TermQuery(Term("unqualified", "katten")), BooleanClause.Occur.SHOULD)
    #     query.add(TermQuery(Term("unqualified", "kat")), BooleanClause.Occur.SHOULD)
    #     self.assertConversion(query, cql='katten')

    def testPhraseQueryIsStandardAnalyzed(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        for term in ["vol.118", "2008", "nr.3", "march", "p.435-444"]:
            expected["terms"].append(dict(field="unqualified", value=term))
        input = '"vol.118 (2008) nr.3 (March) p.435-444"'
        self.assertConversion(expected, cql=input)

    def testOneTermPhraseQueryUsesStandardAnalyzed(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        expected["terms"].append(dict(field="unqualified", value='aap'))
        expected["terms"].append(dict(field="unqualified", value='noot'))
        self.assertConversion(expected, cql='aap:noot')

    def testCreatesEmptyPhraseQueryIfNoValidCharsFound(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        self.assertConversion(expected, cql=':')

    def testStandardAnalyserWithoutStopWords(self):
        expected = dict(type="PhraseQuery", terms=[], boost=1.0)
        for term in ["no", "is", "the", "only", "option"]:
            expected["terms"].append(dict(field="unqualified", value=term))
        self.assertConversion(expected, cql='"no is the only option"')

    def testDiacritics(self):
        expected = termQuery('title', 'moree')
        self.assertConversion(expected, cql='title=Moree')
        self.assertConversion(expected, cql='title=Morée')
        self.assertConversion(expected, cql='title=Morèe')

        # self._analyzer = MerescoDutchStemmingAnalyzer()
        # query = PhraseQuery()
        # query.add(Term("title", "waar"))
        # query.add(Term("title", "is"))
        # query.add(Term("title", "moree"))
        # query.add(Term("title", "vandaag"))
        # self.assertConversion(query, cql='title="Waar is Morée vandaag"')

    def testDiacriticsShouldBeNormalizedNFC(self):
        pq = dict(type="PhraseQuery", terms=[])
        pq["terms"].append(dict(field="title", value="more"))
        pq["terms"].append(dict(field="title", value="e"))
        self.assertConversion(pq, cql='title=More\xcc\x81e') # Combined
        from unicodedata import normalize
        self.assertConversion(termQuery('title', 'moree'), cql=normalize('NFC', unicode('title=More\xcc\x81e')))

    def testIndexRelationTermOutput(self):
        self.assertConversion(termQuery('animal', 'cats'), cql='animal=cats')
        query = dict(type="PhraseQuery", terms=[])
        query["terms"].append(dict(field="animal", value="cats"))
        query["terms"].append(dict(field="animal", value="dogs"))
        self.assertConversion(query, cql='animal="cats dogs"')
        self.assertConversion(query, cql='animal="catS Dogs"')

    def testIndexRelationExactTermOutput(self):
        self.assertConversion(termQuery("animal", "hairy cats"), cql='animal exact "hairy cats"')
        self.assertConversion(termQuery("animal", "Capital Cats"), cql='animal exact "Capital Cats"')

    def testBoost(self):
        query = termQuery("title", "cats", boost=2.0)
        self.assertConversion(query, cql="title =/boost=2.0 cats")

    def testWildcards(self):
        query = prefixQuery('unqualified', 'prefix', 1.0)
        self.assertConversion(query, cql='prefix*')
        self.assertConversion(query, cql='PREfix*')
        query = prefixQuery('field', 'prefix')
        self.assertConversion(query, cql='field="PREfix*"')
        self.assertConversion(query, cql='field=prefix*')
        query = prefixQuery('field', 'oc-0123')
        self.assertConversion(query, cql='field="oc-0123*"')
        query = termQuery('field', 'p')
        self.assertConversion(query, cql='field="P*"')
        #only prefix queries for now
        query = termQuery('field', 'post')
        self.assertConversion(query, cql='field="*post"')

        query = termQuery('field', 'prefix')
        self.assertConversion(query, cql='field=prefix**')

        self.unqualifiedFields = [("field0", 0.2), ("field1", 2.0)]

        query = dict(type="BooleanQuery", clauses=[])
        query["clauses"].append(prefixQuery("field0", "prefix", 0.2))
        query["clauses"][0]["occur"] = "SHOULD"

        query["clauses"].append(prefixQuery("field1", "prefix", 2.0))
        query["clauses"][1]["occur"] = "SHOULD"
        self.assertConversion(query, cql="prefix*")

    def testMagicExact(self):
        exactResult = self.convert(cql='animal exact "cats dogs"')
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('animal', STRINGFIELD)
        self.assertConversion(exactResult, cql='animal = "cats dogs"')

    def testTextRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm='value', upperTerm=None, includeLower=False, includeUpper=True)
        self.assertConversion(q, cql='field > value')
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm='value', upperTerm=None, includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='field >= value')
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm=None, upperTerm='value', includeLower=True, includeUpper=False)
        self.assertConversion(q, cql='field < value')
        q = dict(type="RangeQuery", rangeType="String", field='field', lowerTerm=None, upperTerm='value', includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='field <= value')

    def testIntRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=1, upperTerm=None, includeLower=False, includeUpper=True)
        self.assertConversion(q, cql='intField > 1')
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=1, upperTerm=None, includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='intField >= 1')
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=False)
        self.assertConversion(q, cql='intField < 3')
        q = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='intField <= 3')

    def testLongRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=1, upperTerm=None, includeLower=False, includeUpper=True)
        self.assertConversion(q, cql='longField > 1')
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=1, upperTerm=None, includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='longField >= 1')
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=False)
        self.assertConversion(q, cql='longField < 3')
        q = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=None, upperTerm=3, includeLower=True, includeUpper=True)
        self.assertConversion(q, cql='longField <= 3')

    def testDrilldownFieldQuery(self):
        self.fieldRegistry = FieldRegistry([DrilldownField('field', hierarchical=True)])
        self.assertConversion(dict(type="TermQuery", term=dict(field="field", path=["value"], type="DrillDown")), cql="field = value")
        self.assertConversion(dict(type="TermQuery", term=dict(field="field", path=["value", "value1"], type="DrillDown")), cql="field = \"value>value1\"")

    def testExcludeUnqualifiedFieldForWhichNoPhraseQueryIsPossibleInCaseOfPhraseQuery(self):
        self.fieldRegistry = FieldRegistry()
        self.fieldRegistry.register('noTermFreqField', NO_TERMS_FREQUENCY_FIELD)
        self.unqualifiedFields = [("unqualified", 1.0), ('noTermFreqField', 2.0)]
        expected = dict(type="PhraseQuery", terms=[
                dict(field="unqualified", value="phrase"),
                dict(field="unqualified", value="query")
            ], boost=1.0)
        self.assertConversion(expected, cql='"phrase query"')

    def testQueryForIntField(self):
        expected = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=5, upperTerm=5, includeLower=True, includeUpper=True)
        self.assertConversion(expected, cql="intField=5")

        expected = dict(type="RangeQuery", rangeType="Int", field='intField', lowerTerm=5, upperTerm=5, includeLower=True, includeUpper=True)
        self.assertConversion(expected, cql="intField exact 5")

    def testQueryForLongField(self):
        expected = dict(type="RangeQuery", rangeType="Long", field='longField', lowerTerm=long(5), upperTerm=long(5), includeLower=True, includeUpper=True)
        self.assertConversion(expected, cql="longField=5")

    def testQueryForDoubleField(self):
        expected = dict(type="RangeQuery", rangeType="Double", field='range.double.field', lowerTerm=float(5), upperTerm=float(5), includeLower=True, includeUpper=True)
        self.assertConversion(expected, cql="range.double.field=5")

    def testWildcardQuery(self):
        self.fieldRegistry = FieldRegistry()
        expected = dict(type="WildcardQuery", term=dict(field="field", value="???*"))
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
        converter = QueryExpressionToLuceneQueryDict(
            unqualifiedTermFields=unqualifiedFields,
            luceneSettings=settings,
            ignoreStemmingForWords=getattr(self, '_ignoredStemmingForWords', None)
        )
        return converter.convert(expression)

    def assertConversion(self, expected, expression=None, cql=None):
        result = self.convert(expression=expression, cql=cql)
        self.assertEquals(expected, result)

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