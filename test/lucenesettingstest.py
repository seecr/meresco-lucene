## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import SeecrTestCase
from meresco.lucene import LuceneSettings
from meresco.lucene.fieldregistry import FieldRegistry

class LuceneSettingsTest(SeecrTestCase):

    def testOne(self):
        settings = LuceneSettings()
        self.assertTrue(settings.verbose)

        newSettings = settings.clone(verbose=False)
        self.assertTrue(settings.verbose)
        self.assertFalse(newSettings.verbose)

    def testAsPostDict(self):
        settings = LuceneSettings()
        self.assertEqual({
                'lruTaxonomyWriterCacheSize': 4000,
                'maxMergeAtOnce': 2,
                'similarity': {'type': 'BM25Similarity'},
                'numberOfConcurrentTasks': 6,
                'segmentsPerTier': 8.0,
                'analyzer': {'type': 'MerescoStandardAnalyzer'},
                'drilldownFields': [],
                'commitCount': 100000,
                'commitTimeout': 10
            }, settings.asPostDict())

    def testPostDictWithDrilldownFields(self):
        fieldRegistry = FieldRegistry()
        fieldRegistry.registerDrilldownField("field0", hierarchical=True, multiValued=False)
        fieldRegistry.registerDrilldownField("field1", hierarchical=True, multiValued=True, indexFieldName="$facets_2")
        settings = LuceneSettings(fieldRegistry=fieldRegistry)
        self.assertEqual({
                'lruTaxonomyWriterCacheSize': 4000,
                'maxMergeAtOnce': 2,
                'similarity': {'type': 'BM25Similarity'},
                'numberOfConcurrentTasks': 6,
                'segmentsPerTier': 8.0,
                'analyzer': {'type': 'MerescoStandardAnalyzer'},
                'drilldownFields': [
                    {'dim': 'field0', 'hierarchical': True, 'fieldname': None, 'multiValued': False},
                    {'dim': 'field1', 'hierarchical': True, 'fieldname': '$facets_2', 'multiValued': True}],
                'commitCount': 100000,
                'commitTimeout': 10
            }, settings.asPostDict())
