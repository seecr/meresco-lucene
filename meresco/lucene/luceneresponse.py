## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2014, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
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

from json import loads, dumps, JSONEncoder, JSONDecoder
from .hit import Hit


class LuceneResponse(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    @classmethod
    def fromJson(cls, json):
        return cls(**loads(json, cls=LuceneResponseJsonDecoder))

    def asJson(self, **kwargs):
        return dumps(vars(self), cls=LuceneResponseJsonEncoder, **kwargs)

    def __str__(self):
        return "LuceneResponse(%s)" % self.asJson(sort_keys=True)


class LuceneResponseJsonEncoder(JSONEncoder):
    def default(self, o):
        if type(o) is Hit:
            d = {"__class__": Hit.__name__}
            d.update(o.__dict__)
            return d
        return JSONEncoder.default(self, o)


class LuceneResponseJsonDecoder(JSONDecoder):
    def __init__(self, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.dict_to_object, **kwargs)

    def dict_to_object(self, d):
        if Hit.__name__ == d.pop("__class__", None):
            return Hit(**d)
        return d
