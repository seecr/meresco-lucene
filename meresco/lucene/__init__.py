## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from seecr.tools.build import buildIfNeeded                         #DO_NOT_DISTRIBUTE
from os.path import join, dirname, abspath                          #DO_NOT_DISTRIBUTE
try:                                                                #DO_NOT_DISTRIBUTE
    buildIfNeeded(                                                  #DO_NOT_DISTRIBUTE
        srcDir="src_pylucene",                                      #DO_NOT_DISTRIBUTE
        soFilename=join("meresco_lucene", "_meresco_lucene.*.so"),  #DO_NOT_DISTRIBUTE
        buildCommand="cd {srcDir}; ./build.sh",                     #DO_NOT_DISTRIBUTE
        findRootFor=abspath(__file__))                              #DO_NOT_DISTRIBUTE
except RuntimeError as e:                                           #DO_NOT_DISTRIBUTE
    print("Building failed!\n{}\n".format(str(e)))                  #DO_NOT_DISTRIBUTE
    exit(1)                                                         #DO_NOT_DISTRIBUTE

from meresco.pylucene import getJVM
VM = getJVM()
from meresco_lucene import initVM
VMM = initVM()

from .fieldregistry import SORTED_PREFIX, UNTOKENIZED_PREFIX, KEY_PREFIX, NUMERIC_PREFIX, RANGE_DOUBLE_PREFIX
from ._version import version
from .luceneresponse import LuceneResponse
from ._lucene import Lucene
from .lucenesettings import LuceneSettings
from .fields2lucenedoc import Fields2LuceneDoc
from .multilucene import MultiLucene
from .composedquery import ComposedQuery
from .drilldownfield import DrilldownField
from .fieldslisttolucenedocument import FieldsListToLuceneDocument
from .utils import readFixedBitSet
