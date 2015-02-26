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

from os.path import abspath, dirname                              #DO_NOT_DISTRIBUTE
from os import system                                             #DO_NOT_DISTRIBUTE
from glob import glob                                             #DO_NOT_DISTRIBUTE
from sys import path as systemPath                                #DO_NOT_DISTRIBUTE
projectDir = dirname(dirname(abspath(__file__)))                  #DO_NOT_DISTRIBUTE
system('find %s -name "*.pyc" | xargs rm -f' % projectDir)        #DO_NOT_DISTRIBUTE
for path in glob(projectDir+'/deps.d/*'):                         #DO_NOT_DISTRIBUTE
    systemPath.insert(0, path)                                    #DO_NOT_DISTRIBUTE
systemPath.insert(0, projectDir)                                  #DO_NOT_DISTRIBUTE

import unittest
from sys import version
if version >= '2.7':
    from warnings import simplefilter
    simplefilter('default')


from composedquerytest import ComposedQueryTest
from cqltolucenequerytest import CqlToLuceneQueryTest
from dedupfiltercollectortest import DeDupFilterCollectorTest
from fieldregistrytest import FieldRegistryTest
from fields2lucenedoctest import Fields2LuceneDocTest
from groupcollectortest import GroupCollectorTest
from lrucachetest import LruCacheTest
from lucenequerycomposertest import LuceneQueryComposerTest
from luceneremotetest import LuceneRemoteTest
from luceneresponsetest import LuceneResponseTest
from lucenetest import LuceneTest
from multicqltolucenequerytest import MultiCqlToLuceneQueryTest
from multilucenetest import MultiLuceneTest
from termfrequencysimilaritytest import TermFrequencySimilarityTest
from termnumeratortest import TermNumeratorTest
from supercollectortest import SuperCollectorTest
from superindexsearchertest import SuperIndexSearcherTest
from facetsupercollectortest import FacetSuperCollectorTest
from lucenesettingstest import LuceneSettingsTest
from shingleindextest import ShingleIndexTest

if __name__ == '__main__':
    unittest.main()

