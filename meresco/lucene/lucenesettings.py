## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from copy import copy
from org.meresco.lucene.py_analysis import MerescoStandardAnalyzer, MerescoDutchStemmingAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer

from java.lang import Class
from meresco.components.json import JsonDict
from meresco.lucene.fieldregistry import FieldRegistry

SETTING_NAMES = ["commitTimeout", "commitCount", "readonly", "lruTaxonomyWriterCacheSize", "analyzer", "similarity", "mergePolicy", "numberOfConcurrentTasks", "cacheFacetOrdinals", "verbose"]

class LuceneSettings(object):
    def __init__(self,
                commitTimeout=10,
                commitCount=100000,
                readonly=False,
                lruTaxonomyWriterCacheSize=4000,
                analyzer=dict(type="MerescoStandardAnalyzer"),
                similarity=dict(type="BM25Similarity"),
                mergePolicy=dict(type="TieredMergePolicy", maxMergeAtOnce=2, segmentsPerTier=8.0),
                fieldRegistry=FieldRegistry(),
                numberOfConcurrentTasks=6,
                cacheFacetOrdinals=True,
                verbose=True,
            ):
        local = locals()
        for name in SETTING_NAMES:
            self.__dict__['_' + name] = local[name]
        self.fieldRegistry = fieldRegistry
   
    @property
    def analyzer(self):
        return self._analyzer

    '''Is this the right place for this method?'''
    def createAnalyzer(self):
        config = self._analyzer
        if config['type'] == "MerescoStandardAnalyzer":
            return MerescoStandardAnalyzer()
        elif config['type'] == "MerescoDutchStemmingAnalyzer":
            return MerescoDutchStemmingAnalyzer(config['stemmingFields'])
        elif config['type'] == "WhitespaceAnalyzer":
            return WhitespaceAnalyzer()
        raise Exception("No support for type " + str(self._analyzer))

    def similarity(self):
        return self._similarity

    @property
    def verbose(self):
        return self._verbose

    def clone(self, **kwargs):
        arguments = dict((k[1:],v) for k,v in self.__dict__.iteritems() if k[1:] in SETTING_NAMES)
        arguments.update(kwargs)
        return LuceneSettings(**arguments)

    def asPostDict(self):
        drilldownFields = []
        fieldRegistry = self.fieldRegistry
        for fieldname, options in fieldRegistry.drilldownFieldNames.items():
            drilldownFields.append({
                    "dim": fieldname,
                    "hierarchical": options["hierarchical"],
                    "multiValued": options["multiValued"],
                    "fieldname": options["indexFieldName"]
                })
        result = JsonDict(drilldownFields = drilldownFields)
        result.update((k[1:],v) for k,v in self.__dict__.iteritems() if k[1:] in SETTING_NAMES)
        return result
