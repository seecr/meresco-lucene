## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
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

from weightless.io.utils import asProcess

from ._connect import _Connect
from meresco.core import Observable


class LuceneCommit(Observable):
    def __init__(self, host, port, **kwargs):
        Observable.__init__(self, **kwargs)
        self._connect = _Connect(host, port, observable=self)

    def initialize(self):
        return
        yield

    def commit(self):
        asProcess(self._connect.send('/commit/'))
