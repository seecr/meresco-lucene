# -*- encoding: utf-8 -*-
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

from unittest import TestCase


from cqlparser import parseString as parseCql, UnsupportedCQL
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer

from org.apache.lucene.search import TermQuery, BooleanClause, BooleanQuery, PrefixQuery, PhraseQuery, MatchAllDocsQuery, TermRangeQuery
from org.apache.lucene.index import Term

from org.meresco.lucene.analysis import MerescoDutchStemmingAnalyzer
from meresco.lucene.fieldregistry import FieldRegistry, NO_TERMS_FREQUENCY_FIELDTYPE
from org.apache.lucene.document import StringField
from meresco.lucene import DrilldownField, LuceneSettings
from org.apache.lucene.facet import DrillDownQuery


class LuceneQueryComposerTest(TestCase):
    def setUp(self):
        super(LuceneQueryComposerTest, self).setUp()
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings())

    def testOneTermOutput(self):
        self.assertConversion(TermQuery(Term("unqualified", "cat")), "cat")

    def testRightHandSideIsLowercase(self):
        self.assertConversion(TermQuery(Term("unqualified", "cat")), "CaT")

    def testOneTermOutputWithANumber(self):
        self.assertConversion(TermQuery(Term("unqualified", "2005")), "2005")

    def testPhraseOutput(self):
        query = PhraseQuery()
        query.add(Term("unqualified", "cats"))
        query.add(Term("unqualified", "dogs"))
        self.assertConversion(query,'"cats dogs"')

    def testPhraseOutputDutchStemming(self):
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings(analyzer=MerescoDutchStemmingAnalyzer()))
        query = PhraseQuery()
        query.add(Term("unqualified", "katten"))
        query.add(Term("unqualified", "kat"))
        query.add(Term("unqualified", "honden"))
        query.add(Term("unqualified", "hond"))
        query.add(Term("unqualified", "honden")) # repeat immediatly after first occurence
        query.add(Term("unqualified", "hond"))
        query.add(Term("unqualified", "katten"))  # repeat later, with word in between
        query.add(Term("unqualified", "kat"))
        query.add(Term("unqualified", "kat@hond.org"))  # keyword
        query.add(Term("unqualified", "xyz"))           # not stemmable
        query.add(Term("unqualified", "xyz"))           # repeat to keep TF right
        self.assertConversion(query, '"katten honden honden katten kat@hond.org xyz xyz"')

    def testWhatHappensWithEnglishWordsWithDutchStemming(self):
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings(analyzer=MerescoDutchStemmingAnalyzer()))
        query = BooleanQuery()
        query.add(TermQuery(Term("unqualified", "kate")), BooleanClause.Occur.SHOULD)
        query.add(TermQuery(Term("unqualified", "kat")), BooleanClause.Occur.SHOULD)
        self.assertConversion(query, 'kate')
        query = TermQuery(Term("unqualified", "kat"))
        self.assertConversion(query, 'kat')

    def testPhraseQueryIsStandardAnalyzed(self):
        expected = PhraseQuery()
        for term in ["vol.118", "2008", "nr.3", "march", "p.435-444"]:
            expected.add(Term("unqualified", term))
        input = '"vol.118 (2008) nr.3 (March) p.435-444"'
        self.assertConversion(expected, input)

    def testOneTermPhraseQueryUsesStandardAnalyzed(self):
        expected = PhraseQuery()
        expected.add(Term('unqualified', 'aap'))
        expected.add(Term('unqualified', 'noot'))
        self.assertConversion(expected, 'aap:noot')

    def testStandardAnalyserWithoutStopWords(self):
        expected = PhraseQuery()
        for term in ["no", "is", "the", "only", "option"]:
            expected.add(Term("unqualified", term))
        self.assertConversion(expected, '"no is the only option"')

    def testDiacritics(self):
        self.assertConversion(TermQuery(Term('title', 'moree')), 'title=Moree')
        self.assertConversion(TermQuery(Term('title', 'moree')), 'title=Morée')
        self.assertConversion(TermQuery(Term('title', 'moree')), 'title=Morèe')

    def testDiacriticsShouldBeNormalizedNFC(self):
        pq = PhraseQuery()
        pq.add(Term("title", "more"))
        pq.add(Term("title", "e"))
        self.assertConversion(pq, 'title=More\xcc\x81e') # Combined `
        from unicodedata import normalize
        self.assertConversion(TermQuery(Term('title', 'moree')), normalize('NFC', unicode('title=More\xcc\x81e')))

    def testIndexRelationTermOutput(self):
        self.assertConversion(TermQuery(Term("animal", "cats")), 'animal=cats')
        query = PhraseQuery()
        query.add(Term("animal", "cats"))
        query.add(Term("animal", "dogs"))
        self.assertConversion(query, 'animal="cats dogs"')
        self.assertConversion(query, 'animal="catS Dogs"')

    def testIndexRelationExactTermOutput(self):
        self.assertConversion(TermQuery(Term("animal", "hairy cats")), 'animal exact "hairy cats"')
        self.assertConversion(TermQuery(Term("animal", "Capital Cats")), 'animal exact "Capital Cats"')

    def testBooleanAndTermOutput(self):
        query = BooleanQuery()
        query.add(TermQuery(Term('unqualified', 'cats')), BooleanClause.Occur.MUST)
        query.add(TermQuery(Term('unqualified', 'dogs')), BooleanClause.Occur.MUST)
        self.assertConversion(query, 'cats AND dogs')

    def testBooleanOrTermOutput(self):
        query = BooleanQuery()
        query.add(TermQuery(Term('unqualified', 'cats')), BooleanClause.Occur.SHOULD)
        query.add(TermQuery(Term('unqualified', 'dogs')), BooleanClause.Occur.SHOULD)
        self.assertConversion(query, 'cats OR dogs')

    def testBooleanNotTermOutput(self):
        query = BooleanQuery()
        query.add(TermQuery(Term('unqualified', 'cats')), BooleanClause.Occur.MUST)
        query.add(TermQuery(Term('unqualified', 'dogs')), BooleanClause.Occur.MUST_NOT)
        self.assertConversion(query, 'cats NOT dogs')

    def testBraces(self):
        self.assertConversion(TermQuery(Term('unqualified', 'cats')), '(cats)')
        innerQuery = BooleanQuery()
        innerQuery.add(TermQuery(Term('unqualified', 'cats')), BooleanClause.Occur.MUST)
        innerQuery.add(TermQuery(Term('unqualified', 'dogs')), BooleanClause.Occur.MUST)
        outerQuery = BooleanQuery()
        outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD)
        outerQuery.add(TermQuery(Term('unqualified', 'mice')), BooleanClause.Occur.SHOULD)

        self.assertConversion(outerQuery, '(cats AND dogs) OR mice')

    def testBoost(self):
        query = TermQuery(Term("title", "cats"))
        query.setBoost(2.0)
        self.assertConversion(query, "title =/boost=2.0 cats")

    def testUnqualifiedTermFields(self):
        composer = LuceneQueryComposer(unqualifiedTermFields=[("field0", 0.2), ("field1", 2.0)], luceneSettings=LuceneSettings())
        ast = parseCql("value")
        result = composer.compose(ast)
        query = BooleanQuery()
        left = TermQuery(Term("field0", "value"))
        left.setBoost(0.2)
        query.add(left, BooleanClause.Occur.SHOULD)

        right = TermQuery(Term("field1", "value"))
        right.setBoost(2.0)
        query.add(right, BooleanClause.Occur.SHOULD)

        self.assertEquals(type(query), type(result))
        self.assertEquals(repr(query), repr(result))

    def testWildcards(self):
        query = PrefixQuery(Term('unqualified', 'prefix'))
        self.assertConversion(query, 'prefix*')
        self.assertConversion(query, 'PREfix*')
        query = PrefixQuery(Term('field', 'prefix'))
        self.assertConversion(query, 'field="PREfix*"')
        self.assertConversion(query, 'field=prefix*')
        query = PrefixQuery(Term('field', 'oc-0123'))
        self.assertConversion(query, 'field="oc-0123*"')
        query = TermQuery(Term('field', 'p'))
        self.assertConversion(query, 'field="P*"')
        #only prefix queries for now
        query = TermQuery(Term('field', 'post'))
        self.assertConversion(query, 'field="*post"')

        query = TermQuery(Term('field', 'prefix'))
        self.assertConversion(query, 'field=prefix**')

        result = LuceneQueryComposer(unqualifiedTermFields=[("field0", 0.2), ("field1", 2.0)], luceneSettings=LuceneSettings()).compose(parseCql("prefix*"))

        query = BooleanQuery()
        left = PrefixQuery(Term("field0", "prefix"))
        left.setBoost(0.2)
        query.add(left, BooleanClause.Occur.SHOULD)

        right = PrefixQuery(Term("field1", "prefix"))
        right.setBoost(2.0)
        query.add(right, BooleanClause.Occur.SHOULD)

        self.assertEquals(type(query), type(result))
        self.assertEquals(repr(query), repr(result))

    def testMagicExact(self):
        exactResult = self.composer.compose(parseCql('animal exact "cats dogs"'))
        fieldRegistry = FieldRegistry()
        fieldRegistry.register('animal', StringField.TYPE_NOT_STORED)
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings(fieldRegistry=fieldRegistry))
        self.assertConversion(exactResult, 'animal = "cats dogs"')

    def testMatchAllQuery(self):
        self.assertConversion(MatchAllDocsQuery(), '*')

    def testTextRangeQuery(self):
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        self.assertConversion(TermRangeQuery.newStringRange('field', 'value', None, False, False), 'field > value')
        self.assertConversion(TermRangeQuery.newStringRange('field', 'value', None, True, False), 'field >= value')
        self.assertConversion(TermRangeQuery.newStringRange('field', None, 'value', False, False), 'field < value')
        self.assertConversion(TermRangeQuery.newStringRange('field', None, 'value', False, True), 'field <= value')

    def testDrilldownFieldQuery(self):
        fieldRegistry = FieldRegistry([DrilldownField('field')])
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings(fieldRegistry=fieldRegistry))
        self.assertConversion(TermQuery(DrillDownQuery.term("$facets", "field", "value")), "field = value")

    def testExcludeUnqualifiedFieldForWhichNoPhraseQueryIsPossibleInCaseOfPhraseQuery(self):
        fieldRegistry = FieldRegistry()
        fieldRegistry.register('noTermFreqField', NO_TERMS_FREQUENCY_FIELDTYPE)
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0), ('noTermFreqField', 2.0)], luceneSettings=LuceneSettings(fieldRegistry=fieldRegistry))
        expected = PhraseQuery()
        expected.add(Term("unqualified", "phrase query"))
        self.assertConversion(expected, '"phrase query"')

    def assertConversion(self, expected, input):
        result = self.composer.compose(parseCql(input))
        self.assertEquals(type(expected), type(result), "expected %s, but got %s" % (repr(expected), repr(result)))
        self.assertEquals(repr(expected), repr(result))
        # self.assertEquals(expected, result, "expected %s['%s'], but got %s['%s']" % (repr(expected), str(expected), repr(result), str(result)))

    def testUnsupportedCQL(self):
        for relation in ['<>']:
            try:
                LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings()).compose(parseCql('index %(relation)s term' % locals()))
                self.fail()
            except UnsupportedCQL:
                pass
