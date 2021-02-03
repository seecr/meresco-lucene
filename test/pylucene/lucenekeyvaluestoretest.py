## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2014-2017, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from os.path import join

from seecr.test import SeecrTestCase

from meresco.lucene.pylucene.lucenekeyvaluestore import LuceneKeyValueStore


class LuceneKeyValueStoreTest(SeecrTestCase):
    def testSetDeleteGet(self):
        store = LuceneKeyValueStore(join(self.tempdir, 'kv'))
        store['1'] = 'aap'
        store['1'] = 'noot'
        store['2'] = 'mies'
        del store['2']
        self.assertEqual('noot', store['1'])
        self.assertEqual('noot', store.get('1'))

        self.assertRaises(KeyError, lambda: store['2'])
        try:
            store['3']
            self.fail()
        except KeyError as e:
            self.assertEqual("KeyError('3',)", repr(e))
        self.assertEqual(None, store.get('2'))
        self.assertEqual('mies', store.get('3', 'mies'))

        store.close()
        store = None

        store = LuceneKeyValueStore(join(self.tempdir, 'kv'))
        self.assertEqual('noot', store['1'])
        self.assertRaises(KeyError, lambda: store['2'])

    def testAllStringified(self):
        store = LuceneKeyValueStore(join(self.tempdir, 'kv'))
        store[1] = None
        self.assertEqual('None', store[1])
        self.assertEqual('None', store['1'])

    def testUnimplemented(self):
        store = LuceneKeyValueStore(join(self.tempdir, 'kv'))
        store['1'] = 'aap'
        self.assertRaises(NotImplementedError, lambda: len(store))
        self.assertRaises(NotImplementedError, lambda: iter(store))
        self.assertRaises(NotImplementedError, lambda: store.items())
        self.assertRaises(NotImplementedError, lambda: store.keys())
        self.assertRaises(NotImplementedError, lambda: store.values())

    def testCommit(self):
        store = LuceneKeyValueStore(join(self.tempdir, 'kv'))
        store['1'] = 'aap'
        store.commit()
        self.assertEqual('aap', store['1'])