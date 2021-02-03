## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2018, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2015-2016, 2018, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from re import compile

from cqlparser import UnsupportedCQL
from cqlparser.cqltoexpression import QueryExpression

from weightless.core import Transparent

from java.io import StringReader
from org.meresco.lucene.py_analysis import MerescoStandardAnalyzer
from meresco.components.json import JsonDict


class QueryExpressionToLuceneQueryDict(Transparent):
    def __init__(self, unqualifiedTermFields, luceneSettings, ignoreStemmingForWords=None):
        Transparent.__init__(self)
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
        return response

    def convert(self, expression, unqualifiedTermFields=None, composedQuery=None):
        if expression.must_not:
            r = QueryExpression.nested('AND')
            r.operands.append(QueryExpression.searchterm(term='*'))
            r.operands.append(expression)
            expression = r
        return JsonDict(_Converter(
            analyzer=self._analyzer,
            fieldRegistry=self._fieldRegistry,
            ignoreStemmingForWords=self._ignoreStemmingForWords,
            unqualifiedTermFields=unqualifiedTermFields or self._unqualifiedTermFields,
            composedQuery=composedQuery).convert(expression))

    def __call__(self, expression, **kwargs):
        return self.convert(expression, **kwargs)


class _Converter(object):
    def __init__(self, analyzer, fieldRegistry, ignoreStemmingForWords, unqualifiedTermFields, composedQuery):
        self._analyzer = analyzer
        self._fieldRegistry = fieldRegistry
        self._ignoreStemmingForWords = ignoreStemmingForWords
        self._unqualifiedTermFields = unqualifiedTermFields or []
        self._composedQuery = composedQuery
        self._resultsFrom = composedQuery.resultsFrom if composedQuery else None
        self._cores = composedQuery.cores if composedQuery else set([])

    def convert(self, expr):
        if expr.operator:
            return self._nestedExpression(expr)
        if expr.index is None:
            return self._unqualifiedQuery(expr)
        return self._fieldQuery(expr)

    def _nestedExpression(self, expr):
        q = dict(type="BooleanQuery", clauses=[])
        for operand in expr.operands:
            occur = OCCUR[expr.operator]
            if operand.must_not:
                occur = OCCUR['NOT']
            query = self.convert(operand)
            if self._isEmptyQuery(query):
                continue
            query['occur'] = occur
            q['clauses'].append(query)
        return q

    def _unqualifiedQuery(self, expr):
        if expr.term == '*':
            return dict(type="MatchAllDocsQuery")
        queries = []
        for index, boost in self._unqualifiedTermFields:
            query = self._determineQuery(index, expr.term)
            if query["type"] == "PhraseQuery" and not self._fieldRegistry.phraseQueryPossible(index):
                continue
            query['boost'] = boost
            queries.append(query)
        if len(queries) == 1:
            return queries[0]
        q = dict(type="BooleanQuery", clauses=[])
        for query in queries:
            if self._isEmptyQuery(query):
                continue
            query['occur'] = OCCUR['OR']
            q['clauses'].append(query)
        return q

    def _fieldQuery(self, expr):
        core, field = self._parseCorePrefix(expr.index)
        if not expr.relation in ['=', '==', 'exact', '>', '>=', '<=', '<']:
            raise UnsupportedCQL("'%s' not supported for the field '%s'" % (expr.relation, field))
        if self._fieldRegistry.isNumeric(field):
            query = self._termRangeQuery(field, expr.relation, expr.term)
        else:
            if expr.relation in ['==', 'exact'] or \
                    (expr.relation == '=' and self._fieldRegistry.isUntokenized(field)):  # TODO: use fieldRegistry for specific core...
                query = self._createTermQuery(field, expr.term)
            elif expr.relation in ['<','<=','>=','>']:
                query = self._termRangeQuery(field, expr.relation, expr.term)
            else: # expr.relation == '=':
                query = self._determineQuery(field, expr.term)

        if expr.relation_boost:
            query['boost'] = expr.relation_boost

        if core and not core == self._resultsFrom:
            keyName = self._composedQuery.keyName(core, self._resultsFrom)
            query = dict(type='RelationalLuceneQuery', core=core, collectKeyName=keyName, filterKeyName=keyName, query=query)
        return query

    def _determineQuery(self, index, termString):
        terms = self._pre_analyzeToken(index, termString)
        if len(terms) == 1:
            if prefixRegexp.match(termString):
                return dict(type="PrefixQuery", term=self._createStringTerm(index, terms[0]))
            else:
                terms = self._post_analyzeToken(index, terms[0])
                if len(terms) == 1:
                    return self._createTermQuery(index, terms[0])
                q = dict(type="BooleanQuery", clauses=[])
                for term in terms:
                    query = self._createTermQuery(index, term)
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
        rangeQueryType, pythonType = self._fieldRegistry.rangeQueryAndType(field)
        termValue = pythonType(termString) if termString else None
        if relation in {'=', '==', 'exact'}:
            return rangeQuery(rangeQueryType, field, termValue, termValue, True, True)
        if '<' in relation:
            lowerTerm, upperTerm = None, termValue
        else:
            lowerTerm, upperTerm = termValue, None
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

    def _createTermQuery(self, field, term):
        return dict(type="TermQuery", term=self._createStringTerm(field, term))

    def _createStringTerm(self, field, value):
        if self._fieldRegistry.isDrilldownField(field):
            if self._fieldRegistry.isHierarchicalDrilldown(field):
                path = value.split('>')
            else:
                path = [value]
            return dict(field=field, path=path, type="DrillDown")
        return dict(field=field, value=value)

    def _isEmptyQuery(self, query):
        return \
            (query['type'] == 'BooleanQuery' and not query['clauses']) or \
            (query['type'] == "PhraseQuery" and not query['terms'])

    def _parseCorePrefix(self, field):
        if not self._composedQuery:
            return None, field
        if field.startswith(self._resultsFrom):
            return self._resultsFrom, field
        core = self._resultsFrom
        try:
            tmpcore, tail = field.split('.', 1)
            if tmpcore in self._cores:
                core = tmpcore
                field = tail
        except ValueError:
            pass
        return core, field


def rangeQuery(rangeQueryType, field, lowerTerm, upperTerm, includeLower, includeUpper):
    return dict(type="RangeQuery", rangeType=rangeQueryType, field=field, lowerTerm=lowerTerm, upperTerm=upperTerm, includeLower=includeLower, includeUpper=includeUpper)

prefixRegexp = compile(r'^([\w-]{2,})\*$') # pr*, prefix* ....

OCCUR = {
    'AND': "MUST",
    'OR': "SHOULD",
    'NOT': "MUST_NOT"
}
