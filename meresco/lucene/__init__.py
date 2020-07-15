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

from os.path import dirname, abspath, join, isfile                               #DO_NOT_DISTRIBUTE
from os import stat, system, walk                                                #DO_NOT_DISTRIBUTE
from sys import exit, path as sysPath                                            #DO_NOT_DISTRIBUTE
from itertools import chain                                                      #DO_NOT_DISTRIBUTE
mydir = dirname(abspath(__file__))                                               #DO_NOT_DISTRIBUTE
srcDir = join(dirname(dirname(mydir)), 'src_pylucene')                           #DO_NOT_DISTRIBUTE
libDir = join(dirname(dirname(mydir)), 'lib')                                    #DO_NOT_DISTRIBUTE
sofile = join(libDir, 'meresco_lucene', '_meresco_lucene.so')                    #DO_NOT_DISTRIBUTE
merescoLuceneFiles = chain(*[[join(d,f) for f in fs if f.endswith(".java")]      #DO_NOT_DISTRIBUTE
                        for d, _, fs in walk(join(srcDir, 'org'))])              #DO_NOT_DISTRIBUTE
lastMtime = max(stat(f).st_mtime for f in merescoLuceneFiles)                    #DO_NOT_DISTRIBUTE
if not isfile(sofile) or stat(sofile).st_mtime < lastMtime:                      #DO_NOT_DISTRIBUTE
    result = system('cd %s; ./build.sh' % srcDir)                                #DO_NOT_DISTRIBUTE
    if result:                                                                   #DO_NOT_DISTRIBUTE
        exit(result)                                                             #DO_NOT_DISTRIBUTE
sysPath.insert(0, libDir)                                                        #DO_NOT_DISTRIBUTE


from meresco.pylucene import getJVM
VM = getJVM()
from meresco_lucene import initVM
VMM = initVM()

from fieldregistry import SORTED_PREFIX, UNTOKENIZED_PREFIX, KEY_PREFIX, NUMERIC_PREFIX, RANGE_DOUBLE_PREFIX
from _version import version
from luceneresponse import LuceneResponse
from _lucene import Lucene
from lucenesettings import LuceneSettings
from fields2lucenedoc import Fields2LuceneDoc
from multilucene import MultiLucene
from composedquery import ComposedQuery
from drilldownfield import DrilldownField
from fieldslisttolucenedocument import FieldsListToLuceneDocument
from utils import readFixedBitSet
