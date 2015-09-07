## begin license ##
#
# "Edurep" is a service for searching in educational repositories.
# "Edurep" is developed for Stichting Kennisnet (http://www.kennisnet.nl) by
# Seek You Too (http://www.cq2.nl). The project is based on the opensource
# project Meresco (http://www.meresco.com).
#
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015 Stichting Kennisnet http://www.kennisnet.nl
#
# This file is part of "Edurep"
#
# "Edurep" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Edurep" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Edurep"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from meresco.core import Observable
from cqlparser.cqltoexpression import cqlToExpression

class ComposedQueryCqlToExpression(Observable):

    def executeComposedQuery(self, query):
        convertDict = dict((core, cqlToExpression) for core in query.cores)
        query.convertWith(**convertDict)
        response = yield self.any.executeComposedQuery(query=query)
        raise StopIteration(response)