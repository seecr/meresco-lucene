## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from warnings import simplefilter
simplefilter('default')

import unittest

if __name__ == '__main__':
    unittest.main()

