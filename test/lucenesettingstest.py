## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2014-2016, 2018, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from seecr.test import SeecrTestCase
from copy import copy
from meresco.lucene import LuceneSettings
from meresco.lucene.fieldregistry import FieldRegistry

DEFAULTS = dict(
    lruTaxonomyWriterCacheSize = 4000,
    mergePolicy = dict(type = "TieredMergePolicy", segmentsPerTier=8.0, maxMergeAtOnce=2),
    similarity = {'type': 'BM25Similarity'},
    numberOfConcurrentTasks = 6,
    analyzer = {'type': 'MerescoStandardAnalyzer'},
    drilldownFields = [],
    commitCount = 100000,
    commitTimeout = 10,
    cacheFacetOrdinals = True,
    verbose = True,
)

class LuceneSettingsTest(SeecrTestCase):

    def testOne(self):
        settings = LuceneSettings()
        self.assertTrue(settings.verbose)
        newSettings = settings.clone(verbose=False)
        self.assertTrue(settings.verbose)
        self.assertFalse(newSettings.verbose)

    def testAsPostDict(self):
        settings = LuceneSettings()
        self.assertEqual(DEFAULTS, settings.asPostDict())

    def testPostDictWithDrilldownFields(self):
        fieldRegistry = FieldRegistry()
        fieldRegistry.registerDrilldownField("field0", hierarchical=True, multiValued=False)
        fieldRegistry.registerDrilldownField("field1", hierarchical=True, multiValued=True, indexFieldName="$facets_2")
        settings = LuceneSettings(fieldRegistry=fieldRegistry)
        soll = copy(DEFAULTS)
        soll['drilldownFields'] = [
            {'dim': 'field0', 'hierarchical': True, 'fieldname': None, 'multiValued': False},
            {'dim': 'field1', 'hierarchical': True, 'fieldname': '$facets_2', 'multiValued': True}]
        self.assertEqual(soll, settings.asPostDict())

    def testConfigureOrdinalsCache(self):
        settings = LuceneSettings(cacheFacetOrdinals=False)
        soll = copy(DEFAULTS)
        soll['cacheFacetOrdinals'] = False
        ist = settings.asPostDict()
        self.assertEqual(soll, ist)

    def testConfigureMergePolicy(self):
        settings = LuceneSettings(mergePolicy={'type':'LogDocMergePolicy',
            'mergeFactor': 2,
            'maxMergeDocs': 100})
        soll = copy(DEFAULTS)
        soll['mergePolicy'] = dict(type='LogDocMergePolicy', mergeFactor=2, maxMergeDocs=100)
        ist = settings.asPostDict()
        self.assertEqual(soll, ist)

    def testGetters(self):
        settings = LuceneSettings(cacheFacetOrdinals=False)
        self.assertTrue(settings.verbose)
        self.assertEqual({'type':'MerescoStandardAnalyzer'}, settings.analyzer)

    def testCreateDefaultAnalyzers(self):
        settings = LuceneSettings()
        analyzer = settings.createAnalyzer()
        self.assertEqual("MerescoStandardAnalyzer", analyzer.getClass().getSimpleName())

    def testCreateNonDefaultAnalyzer(self):
        settings = LuceneSettings(analyzer=dict(type="MerescoDutchStemmingAnalyzer", stemmingFields=["field_a", "field_b"]))
        analyzer = settings.createAnalyzer()
        self.assertEqual("MerescoDutchStemmingAnalyzer", analyzer.getClass().getSimpleName())
        self.assertEqual(["field_a", "field_b"], analyzer.getStemmingFields())

    def testCreateWhiteSpaceAnalyzer(self):
        settings = LuceneSettings(analyzer=dict(type="WhitespaceAnalyzer"))
        analyzer = settings.createAnalyzer()
        self.assertEqual("WhitespaceAnalyzer", analyzer.getClass().getSimpleName())
