## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from org.apache.lucene.search import TermQuery, BooleanClause, BooleanQuery, PrefixQuery, PhraseQuery, MatchAllDocsQuery, TermRangeQuery
from org.apache.lucene.index import Term
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute
from java.io import StringReader

from cqlparser import CqlVisitor, UnsupportedCQL
from re import compile
from org.meresco.lucene.analysis import MerescoStandardAnalyzer


class LuceneQueryComposer(object):
    def __init__(self, unqualifiedTermFields, luceneSettings):
        self._additionalKwargs = dict(
                unqualifiedTermFields=unqualifiedTermFields,
                analyzer=luceneSettings.analyzer,
                fieldRegistry=luceneSettings.fieldRegistry,
            )

    def compose(self, ast):
        (result, ) = _Cql2LuceneQueryVisitor(
            node=ast,
            **self._additionalKwargs).visit()
        return result


class _Cql2LuceneQueryVisitor(CqlVisitor):
    def __init__(self, unqualifiedTermFields, node, analyzer, fieldRegistry):
        CqlVisitor.__init__(self, node)
        self._unqualifiedTermFields = unqualifiedTermFields
        self._analyzer = analyzer
        self._fieldRegistry = fieldRegistry

    def visitSCOPED_CLAUSE(self, node):
        clause = CqlVisitor.visitSCOPED_CLAUSE(self, node)
        if len(clause) == 1:
            return clause[0]
        lhs, operator, rhs = clause
        query = BooleanQuery()
        query.add(lhs, LHS_OCCUR[operator])
        query.add(rhs, RHS_OCCUR[operator])
        return query

    def visitSEARCH_CLAUSE(self, node):
        # possible children:
        # CQL_QUERY
        # SEARCH_TERM
        # INDEX, RELATION, SEARCH_TERM
        firstChild = node.children[0].name
        results = CqlVisitor.visitSEARCH_CLAUSE(self, node)
        if firstChild == 'SEARCH_TERM':
            (unqualifiedRhs,) = results
            if unqualifiedRhs == '*':
                return MatchAllDocsQuery()
            subQueries = []
            for fieldname, boost in self._unqualifiedTermFields:
                subQuery = self._termOrPhraseQuery(fieldname, unqualifiedRhs)
                if isinstance(subQuery, PhraseQuery) and not self._fieldRegistry.phraseQueryPossible(fieldname):
                    continue
                subQuery.setBoost(boost)
                subQueries.append(subQuery)
            if len(subQueries) == 1:
                query = subQueries[0]
            else:
                query = BooleanQuery()
                for subQuery in subQueries:
                    query.add(subQuery, BooleanClause.Occur.SHOULD)
            return query
        elif firstChild == 'INDEX':
            (left, (relation, boost), right) = results
            if relation in ['==', 'exact'] or (relation == '=' and self._fieldRegistry.isUntokenized(left)):
                query = TermQuery(self._createTerm(left, right))
            elif relation == '=':
                query = self._termOrPhraseQuery(left, right)
            elif relation in ['<','<=','>=','>']:
                query = self._termRangeQuery(left, relation, right)
            else:
                raise UnsupportedCQL("'%s' not supported for the field '%s'" % (relation, left))

            query.setBoost(boost)
            return query
        else:
            ((query,),) = results
            return query

    def visitRELATION(self, node):
        results = CqlVisitor.visitRELATION(self, node)
        if len(results) == 1:
            relation = results[0]
            boost = 1.0
        else:
            (relation, (modifier, comparitor, value)) = results
            boost = float(value)
        return relation, boost

    def _termOrPhraseQuery(self, index, termString):
        terms = self._pre_analyzeToken(termString)
        if len(terms) == 1:
            if prefixRegexp.match(termString):
                return PrefixQuery(self._createTerm(index, terms[0]))
            else:
                terms = self._post_analyzeToken(terms[0])
                if len(terms) == 1:
                    return TermQuery(self._createTerm(index, terms[0]))
                query = BooleanQuery()
                for term in terms:
                    query.add(TermQuery(self._createTerm(index, term)), BooleanClause.Occur.SHOULD)
                return query
        else:
            query = PhraseQuery()
            for term in terms:
                query.add(self._createTerm(index, term))
            return query

    def _termRangeQuery(self, index, relation, termString):
        field = index
        if '<' in relation:
            lowerTerm, upperTerm = None, termString
        else:
            lowerTerm, upperTerm = termString, None
        includeLower, includeUpper = relation == '>=', relation == '<='
        return TermRangeQuery.newStringRange(field, lowerTerm, upperTerm, includeLower, includeUpper)

    def _pre_analyzeToken(self, token):
        if isinstance(self._analyzer, MerescoStandardAnalyzer):
            return list(self._analyzer.pre_analyse(token))
        return list(MerescoStandardAnalyzer.readTokenStream(self._analyzer.tokenStream("dummy field name", StringReader(token))))

    def _post_analyzeToken(self, token):
        if isinstance(self._analyzer, MerescoStandardAnalyzer):
            return list(self._analyzer.post_analyse(token))
        return [token]

    def _createTerm(self, field, value):
        if self._fieldRegistry.isDrilldownField(field):
            if self._fieldRegistry.isHierarchicalDrilldown(field):
                value = value.split('>')
            return self._fieldRegistry.makeDrilldownTerm(field, value)
        return Term(field, value)


prefixRegexp = compile(r'^([\w-]{2,})\*$') # pr*, prefix* ....

LHS_OCCUR = {
    "AND": BooleanClause.Occur.MUST,
    "OR" : BooleanClause.Occur.SHOULD,
    "NOT": BooleanClause.Occur.MUST
}
RHS_OCCUR = {
    "AND": BooleanClause.Occur.MUST,
    "OR" : BooleanClause.Occur.SHOULD,
    "NOT": BooleanClause.Occur.MUST_NOT
}
