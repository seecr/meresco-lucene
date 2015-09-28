## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase, CallTrace
from meresco.lucene import LuceneSettings, VM, Lucene
from meresco.lucene.fieldregistry import FieldRegistry
from os.path import join
import gc


class LuceneTestCase(SeecrTestCase):

    def setUp(self, fieldRegistry=FieldRegistry()):
        super(LuceneTestCase, self).setUp()
        self._javaObjects = self._getJavaObjects()
        self._reactor = CallTrace('reactor', methods={'addTimer': lambda seconds, callback: CallTrace('timer')})
        self._defaultSettings = LuceneSettings(commitCount=1, commitTimeout=1, fieldRegistry=fieldRegistry)
        self.lucene = Lucene(
            join(self.tempdir, 'lucene'),
            reactor=self._reactor,
            settings=self._defaultSettings,
        )
        self.observer = CallTrace()
        self.lucene.addObserver(self.observer)

    def tearDown(self):
        try:
            self._reactor.calledMethods.reset() # don't keep any references.
            self.lucene.close()
            self.lucene = None
            gc.collect()
            diff = self._getJavaObjects() - self._javaObjects
            self.assertEquals(0, len(diff), diff)
        finally:
            SeecrTestCase.tearDown(self)

    def _getJavaObjects(self):
        refs = VM._dumpRefs(classes=True)
        return set(
                [(c, refs[c])
                for c in refs.keys()
                if c != 'class java.lang.Class' and
                    c != 'class org.apache.lucene.document.Field' and # Fields are kept in FieldRegistry for reusing
                    c != 'class org.apache.lucene.document.NumericDocValuesField' and
                    c != 'class org.apache.lucene.facet.FacetsConfig'
            ])

