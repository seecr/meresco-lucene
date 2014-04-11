## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from meresco.lucene.cache import LruCache

from org.apache.lucene.search import QueryWrapperFilter, TermQuery
from org.apache.lucene.index import Term

class LruCacheTest(SeecrTestCase):

    def testGet(self):
        cache = LruCache(keyEqualsFunction=lambda q1, q2: q1.equals(q2), createFunction=lambda q: QueryWrapperFilter(q))
        f1 = cache.get(TermQuery(Term("field1", "value1")))
        f2 = cache.get(TermQuery(Term("field1", "value1")))
        self.assertEquals(f1, f2)
        f3 = cache.get(TermQuery(Term("field1", "value2")))
        self.assertNotEquals(f1, f3)

    def testCacheSize(self):
        cache = LruCache(keyEqualsFunction=lambda q1, q2: q1.equals(q2), createFunction=lambda q: QueryWrapperFilter(q), size=2)
        f1 = cache.get(TermQuery(Term("field1", "value1")))
        f2 = cache.get(TermQuery(Term("field1", "value2")))
        self.assertNotEquals(f1, f2)
        f3 = cache.get(TermQuery(Term("field1", "value1")))
        self.assertEquals(f3, f1)
        cache.get(TermQuery(Term("field1", "value3")))
        f4 = cache.get(TermQuery(Term("field1", "value1")))
        f5 = cache.get(TermQuery(Term("field1", "value2")))
        self.assertEquals(f4, f1)
        self.assertNotEquals(f5, f2)

    def testEqualsDefault(self):
        cache = LruCache(createFunction=lambda a: object())
        a = cache.get("testa")
        b = cache.get("testb")
        a2 = cache.get("testa")
        self.assertEquals(a, a2)
        self.assertNotEquals(a, b)

    def testClearCache(self):
        cache = LruCache(createFunction=lambda a: object())
        a = cache.get("testa")
        a2 = cache.get("testa")
        cache.clear()
        a3 = cache.get("testa")
        self.assertEquals(a, a2)
        self.assertNotEquals(a, a3)
