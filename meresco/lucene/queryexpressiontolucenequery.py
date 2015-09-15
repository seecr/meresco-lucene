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

from org.apache.lucene.index import Term
from org.apache.lucene.search import TermQuery, BooleanQuery, BooleanClause, MatchAllDocsQuery, WildcardQuery, PhraseQuery, PrefixQuery
from org.meresco.lucene.analysis import MerescoStandardAnalyzer
from weightless.core import Observable
from java.io import StringReader
from re import compile
from cqlparser import UnsupportedCQL

class QueryExpressionToLuceneQuery(Observable):

    def __init__(self, unqualifiedTermFields, luceneSettings, ignoreStemmingForWords=None):
        Observable.__init__(self)
        self._unqualifiedTermFields = unqualifiedTermFields
        self._analyzer = luceneSettings.analyzer
        self._fieldRegistry = luceneSettings.fieldRegistry
        self._ignoreStemmingForWords = set(ignoreStemmingForWords or [])

    def updateUnqualifiedTermFields(self, unqualifiedTermFields):
        self._unqualifiedTermFields = unqualifiedTermFields

    def updateIgnoreStemmingForWords(self, ignoreStemmingForWords):
        self._ignoreStemmingForWords = ignoreStemmingForWords

    def executeQuery(self, query, **kwargs):
        response = yield self.any.executeQuery(luceneQuery=self.convert(query), **kwargs)
        raise StopIteration(response)

    def convert(self, expression):
        return self._expression(expression)

    def __call__(self, expression):
        return self.convert(expression)

    def _expression(self, expr):
        if expr.operator:
            return self._nestedExpression(expr)
        if expr.index is None:
            if expr.term == '*':
                return MatchAllDocsQuery()
            queries = []
            for index, boost in self._unqualifiedTermFields:
                query = self._determineQuery(index, expr.term)
                if isinstance(query, PhraseQuery) and not self._fieldRegistry.phraseQueryPossible(index):
                    continue
                query.setBoost(boost)
                queries.append(query)
            if len(queries) == 1:
                return queries[0]
            q = BooleanQuery()
            for query in queries:
                q.add(query, OCCUR['OR'])
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
                query.setBoost(expr.relation_boost)
            return query

    def _nestedExpression(self, expr):
        q = BooleanQuery()
        for operand in expr.operands:
            occur = OCCUR[expr.operator]
            if operand.must_not:
                occur = OCCUR['NOT']
            q.add(self._expression(operand), occur)
        return q


    def _determineQuery(self, index, termString):
        terms = self._pre_analyzeToken(index, termString)
        if len(terms) == 1:
            if prefixRegexp.match(termString):
                return PrefixQuery(self._createStringTerm(index, terms[0]))
            else:
                terms = self._post_analyzeToken(index, terms[0])
                if len(terms) == 1:
                    return self._createQuery(index, terms[0])
                query = BooleanQuery()
                for term in terms:
                    query.add(self._createQuery(index, term), BooleanClause.Occur.SHOULD)
                return query
        else:
            if '???*' == termString:
                return WildcardQuery(self._createStringTerm(index, termString))
            query = PhraseQuery()
            for term in terms:
                query.add(self._createStringTerm(index, term))
            return query

    def _termRangeQuery(self, index, relation, termString):
        field = index
        if '<' in relation:
            lowerTerm, upperTerm = None, termString
        else:
            lowerTerm, upperTerm = termString, None
        includeLower, includeUpper = relation == '>=', relation == '<='
        rangeQuery, pythonType = self._fieldRegistry.rangeQueryAndType(field)
        lowerTerm = pythonType(lowerTerm) if lowerTerm else None
        upperTerm = pythonType(upperTerm) if upperTerm else None
        return rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper)

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
            rangeQuery, pythonType = self._fieldRegistry.rangeQueryAndType(field)
            term = pythonType(term) if term else None
            return rangeQuery(field, term, term, True, True)
        else:
            return TermQuery(self._createStringTerm(field, term))

    def _createStringTerm(self, field, value):
        if self._fieldRegistry.isDrilldownField(field):
            if self._fieldRegistry.isHierarchicalDrilldown(field):
                value = value.split('>')
            return self._fieldRegistry.makeDrilldownTerm(field, value)
        return Term(field, value)


prefixRegexp = compile(r'^([\w-]{2,})\*$') # pr*, prefix* ....

OCCUR = {
    'AND': BooleanClause.Occur.MUST,
    'OR': BooleanClause.Occur.SHOULD,
    'NOT': BooleanClause.Occur.MUST_NOT
}
