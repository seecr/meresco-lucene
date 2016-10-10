## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015-2016 Stichting Kennisnet http://www.kennisnet.nl
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

from re import compile

from cqlparser import UnsupportedCQL
from cqlparser.cqltoexpression import QueryExpression

from weightless.core import Observable

from java.io import StringReader
from org.meresco.lucene.py_analysis import MerescoStandardAnalyzer
from meresco.components.json import JsonDict


class QueryExpressionToLuceneQueryDict(Observable):
    def __init__(self, unqualifiedTermFields, luceneSettings, ignoreStemmingForWords=None):
        Observable.__init__(self)
        self._unqualifiedTermFields = unqualifiedTermFields
        self._analyzer = luceneSettings.createAnalyzer()
        self._fieldRegistry = luceneSettings.fieldRegistry
        self._ignoreStemmingForWords = set(ignoreStemmingForWords or [])

    def updateUnqualifiedTermFields(self, unqualifiedTermFields):
        self._unqualifiedTermFields = unqualifiedTermFields

    def updateIgnoreStemmingForWords(self, ignoreStemmingForWords):
        self._ignoreStemmingForWords = ignoreStemmingForWords

    def executeQuery(self, query, **kwargs):
        response = yield self.any.executeQuery(luceneQuery=self.convert(query), **kwargs)
        raise StopIteration(response)

    def convert(self, expression, unqualifiedTermFields=None):
        if expression.must_not:
            r = QueryExpression.nested('AND')
            r.operands.append(QueryExpression.searchterm(term='*'))
            r.operands.append(expression)
            expression = r
        return JsonDict(self._expression(expression, unqualifiedTermFields=unqualifiedTermFields or self._unqualifiedTermFields))

    def __call__(self, expression, **kwargs):
        return self.convert(expression, **kwargs)

    def _expression(self, expr, unqualifiedTermFields=None):
        if expr.operator:
            return self._nestedExpression(expr, unqualifiedTermFields=unqualifiedTermFields)
        if expr.index is None:
            if expr.term == '*':
                return dict(type="MatchAllDocsQuery")
            queries = []
            for index, boost in (unqualifiedTermFields or []):
                query = self._determineQuery(index, expr.term)
                if query["type"] == "PhraseQuery" and not self._fieldRegistry.phraseQueryPossible(index):
                    continue
                query['boost'] = boost
                queries.append(query)
            if len(queries) == 1:
                return queries[0]
            q = dict(type="BooleanQuery", clauses=[])
            for query in queries:
                if query['type'] == 'BooleanQuery' and not query['clauses']:
                    continue
                if query['type'] == "PhraseQuery" and not query['terms']:
                    continue
                query['occur'] = OCCUR['OR']
                q['clauses'].append(query)
            return q
        else:
            if expr.relation in ['==', 'exact'] or \
                    (expr.relation == '=' and self._fieldRegistry.isUntokenized(expr.index)):
                query = self._createQuery(expr.index, expr.term)
            elif expr.relation in ['<','<=','>=','>']:
                query = self._termRangeQuery(expr.index, expr.relation, expr.term)
            elif expr.relation == '=':
                query = self._determineQuery(expr.index, expr.term)
            else:
                raise UnsupportedCQL("'%s' not supported for the field '%s'" % (expr.relation, expr.index))
            if expr.relation_boost:
                query['boost'] = expr.relation_boost
            return query

    def _nestedExpression(self, expr, unqualifiedTermFields):
        q = dict(type="BooleanQuery", clauses=[])
        for operand in expr.operands:
            occur = OCCUR[expr.operator]
            if operand.must_not:
                occur = OCCUR['NOT']
            query = self._expression(operand, unqualifiedTermFields=unqualifiedTermFields)
            if query['type'] == 'BooleanQuery' and not query['clauses']:
                continue
            if query['type'] == 'PhraseQuery' and not query['terms']:
                continue
            query['occur'] = occur
            q['clauses'].append(query)
        return q

    def _determineQuery(self, index, termString):
        terms = self._pre_analyzeToken(index, termString)
        if len(terms) == 1:
            if prefixRegexp.match(termString):
                return dict(type="PrefixQuery", term=self._createStringTerm(index, terms[0]))
            else:
                terms = self._post_analyzeToken(index, terms[0])
                if len(terms) == 1:
                    return self._createQuery(index, terms[0])
                q = dict(type="BooleanQuery", clauses=[])
                for term in terms:
                    query = self._createQuery(index, term)
                    query['occur'] = OCCUR["OR"]
                    q["clauses"].append(query)
                return q
        else:
            if '???*' == termString:
                return dict(type="WildcardQuery", term=self._createStringTerm(index, termString))
            query = dict(type="PhraseQuery", terms=[])
            for term in terms:
                query['terms'].append(self._createStringTerm(index, term))
            return query

    def _termRangeQuery(self, index, relation, termString):
        field = index
        if '<' in relation:
            lowerTerm, upperTerm = None, termString
        else:
            lowerTerm, upperTerm = termString, None
        rangeQueryType, pythonType = self._fieldRegistry.rangeQueryAndType(field)
        lowerTerm = pythonType(lowerTerm) if lowerTerm else None
        upperTerm = pythonType(upperTerm) if upperTerm else None
        includeLower, includeUpper = relation == '>=' or lowerTerm is None, relation == '<=' or upperTerm is None
        return rangeQuery(rangeQueryType, field, lowerTerm, upperTerm, includeLower, includeUpper)

    def _pre_analyzeToken(self, index, token):
        if isinstance(self._analyzer, MerescoStandardAnalyzer):
            return list(self._analyzer.pre_analyse(index, token))
        return list(MerescoStandardAnalyzer.readTokenStream(self._analyzer.tokenStream("dummy field name", StringReader(token))))

    def _post_analyzeToken(self, index, token):
        if token in self._ignoreStemmingForWords:
            return [token]
        if isinstance(self._analyzer, MerescoStandardAnalyzer):
            return list(self._analyzer.post_analyse(index, token))
        return [token]

    def _createQuery(self, field, term):
        if self._fieldRegistry.isNumeric(field):
            rangeQueryType, pythonType = self._fieldRegistry.rangeQueryAndType(field)
            term = pythonType(term) if term else None
            return rangeQuery(rangeQueryType, field, term, term, True, True)
        else:
            return dict(type="TermQuery", term=self._createStringTerm(field, term))

    def _createStringTerm(self, field, value):
        if self._fieldRegistry.isDrilldownField(field):
            if self._fieldRegistry.isHierarchicalDrilldown(field):
                path = value.split('>')
            else:
                path = [value]
            return dict(field=field, path=path, type="DrillDown")
        return dict(field=field, value=value)


def rangeQuery(rangeQueryType, field, lowerTerm, upperTerm, includeLower, includeUpper):
    return dict(type="RangeQuery", rangeType=rangeQueryType, field=field, lowerTerm=lowerTerm, upperTerm=upperTerm, includeLower=includeLower, includeUpper=includeUpper)

prefixRegexp = compile(r'^([\w-]{2,})\*$') # pr*, prefix* ....

OCCUR = {
    'AND': "MUST",
    'OR': "SHOULD",
    'NOT': "MUST_NOT"
}
