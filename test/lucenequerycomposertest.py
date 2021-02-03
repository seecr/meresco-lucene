# -*- encoding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2015, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from unittest import TestCase

from cqlparser import parseString as parseCql
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer

from meresco.lucene.fieldregistry import FieldRegistry, INTFIELD, LONGFIELD
from meresco.lucene import LuceneSettings


class LuceneQueryComposerTest(TestCase):
    def setUp(self):
        super(LuceneQueryComposerTest, self).setUp()
        fieldRegistry = FieldRegistry()
        fieldRegistry.register("intField", fieldDefinition=INTFIELD)
        fieldRegistry.register("longField", fieldDefinition=LONGFIELD)
        self.composer = LuceneQueryComposer(unqualifiedTermFields=[("unqualified", 1.0)], luceneSettings=LuceneSettings(fieldRegistry=fieldRegistry))

    def testOneTermOutput(self):
        self.assertConversion({"type": "TermQuery", "term": {"field": "unqualified", "value": "cat"}, "boost": 1.0}, "cat")

    def assertConversion(self, expected, input):
        result = self.composer.compose(parseCql(input))
        self.assertEqual(expected, result)
        # self.assertEquals(expected, result, "expected %s['%s'], but got %s['%s']" % (repr(expected), str(expected), repr(result), str(result)))

