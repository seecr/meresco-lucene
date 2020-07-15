## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecrdeps import includeParentAndDeps       #DO_NOT_DISTRIBUTE
includeParentAndDeps(__file__)                   #DO_NOT_DISTRIBUTE

import unittest
from sys import version
if version >= '2.7':
    from warnings import simplefilter
    simplefilter('default')


from composedquerytest import ComposedQueryTest
from conversiontest import ConversionTest
from converttocomposedquerytest import ConvertToComposedQueryTest
from extractfilterqueriestest import ExtractFilterQueriesTest
from fieldregistrytest import FieldRegistryTest
from fields2lucenedoctest import Fields2LuceneDocTest
from fieldslisttolucenedocumenttest import FieldsListToLuceneDocumentTest
from lucenequerycomposertest import LuceneQueryComposerTest
from luceneremotetest import LuceneRemoteTest
from luceneresponsetest import LuceneResponseTest
from lucenesettingstest import LuceneSettingsTest
from lucenetest import LuceneTest
from adaptertolucenequerytest import AdapterToLuceneQueryTest
from multilucenetest import MultiLuceneTest
from queryexpressiontolucenequerydicttest import QueryExpressionToLuceneQueryDictTest
from suggestionindexcomponenttest import SuggestionIndexComponentTest

from pylucene.lucenekeyvaluestoretest import LuceneKeyValueStoreTest

if __name__ == '__main__':
    unittest.main()
