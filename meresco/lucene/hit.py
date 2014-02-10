## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from hashlib import md5

class Hit(object):

    def __init__(self, id, **kwargs):
        self.id = id
        self.__dict__.update(kwargs)

    def __eq__(self, other):
        return self.__class__.__name__ == other.__class__.__name__ and \
            self.__dict__ == other.__dict__

    def __hash__(self):
        h = md5()
        for k,v in self.__dict__.items():
            h.update(k)
            h.update(v)
        return hash(h.digest())

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, ', '.join("%s=%s" % (k, repr(v)) for k, v in self.__dict__.items()))
