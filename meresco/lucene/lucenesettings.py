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

from copy import copy
from org.meresco.lucene.analysis import MerescoStandardAnalyzer
from meresco.components.json import JsonDict
from meresco.lucene.fieldregistry import FieldRegistry
from org.apache.lucene.search.similarities import BM25Similarity

class LuceneSettings(object):
    def __init__(self,
                commitTimeout=10,
                commitCount=100000,
                readonly=False,
                lruTaxonomyWriterCacheSize=4000,
                analyzer=MerescoStandardAnalyzer(),
                _analyzer=dict(type="MerescoStandardAnalyzer"),
                similarity=BM25Similarity(),
                _similarity=dict(type="BM25Similarity"),
                fieldRegistry=FieldRegistry(),
                maxMergeAtOnce=2,
                segmentsPerTier=8.0,
                numberOfConcurrentTasks=6,
                verbose=True,
            ):
        self.commitTimeout = commitTimeout
        self.commitCount = commitCount
        self.readonly = readonly
        self.lruTaxonomyWriterCacheSize = lruTaxonomyWriterCacheSize
        self.analyzer = analyzer
        self._analyzer = _analyzer
        self.similarity = similarity
        self._similarity = _similarity
        self.fieldRegistry = fieldRegistry
        self.maxMergeAtOnce = maxMergeAtOnce
        self.segmentsPerTier = segmentsPerTier
        self.numberOfConcurrentTasks = numberOfConcurrentTasks
        self.verbose = verbose

    def clone(self, **kwargs):
        arguments = copy(self.__dict__)
        arguments.update(kwargs)
        return LuceneSettings(**arguments)

    def asPostDict(self):
        drilldownFields = []
        fieldRegistry = self.fieldRegistry
        for fieldname in fieldRegistry._hierarchicalDrilldownFieldNames:
            drilldownFields.append({
                    "dim": fieldname,
                    "hierarchical": True,
                    "multiValued": fieldname in fieldRegistry._multivaluedDrilldownFieldNames,
                    "fieldname": fieldRegistry._indexFieldNames[fieldname]
                })
        return JsonDict(
                commitTimeout=self.commitTimeout,
                commitCount=self.commitCount,
                lruTaxonomyWriterCacheSize=self.lruTaxonomyWriterCacheSize,
                analyzer=self._analyzer,
                similarity=self._similarity,
                maxMergeAtOnce=self.maxMergeAtOnce,
                segmentsPerTier=self.segmentsPerTier,
                numberOfConcurrentTasks=self.numberOfConcurrentTasks,
                drilldownFields=drilldownFields
            )