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

from seecr.test import SeecrTestCase
from meresco.lucene.multiquery import MultiQuery

class MultiQueryTest(SeecrTestCase):
    def testValidateMultiQuery(self):
        multiQuery = MultiQuery()
        def assertValueError(message):
            try:
                multiQuery.validate()
                self.fail()
            except ValueError, e:
                self.assertEquals(message, str(e))
        assertValueError("Unsupported number of cores, expected exactly 2.")
        multiQuery.add(core='coreA', query=None)
        assertValueError("Unsupported number of cores, expected exactly 2.")
        multiQuery.add(core='coreB', query=None)
        assertValueError("Core for results not specified, use resultsFrom(core='core')")
        multiQuery.resultsFrom(core='coreC')
        assertValueError("Core in resultsFrom does not match the available cores, 'coreC' not in ['coreA', 'coreB']")
        multiQuery.resultsFrom(core='coreA')
        assertValueError("No match set for cores")
        multiQuery.addMatch(coreC='keyC', coreD='keyE')
        assertValueError("No match set for cores: ('coreA', 'coreB')")
        multiQuery.addMatch(coreA='keyA', coreB='keyB')
        multiQuery.validate()

    def testKeyNames(self):
        multiQuery = MultiQuery()
        multiQuery.add(core='coreA', query=None)
        multiQuery.add(core='coreB', query=None)
        multiQuery.addMatch(coreA='keyA', coreB='keyB')
        self.assertEquals(('keyA', 'keyB'), multiQuery.keyNames('coreA', 'coreB'))
        self.assertEquals(('keyB', 'keyA'), multiQuery.keyNames('coreB', 'coreA'))
