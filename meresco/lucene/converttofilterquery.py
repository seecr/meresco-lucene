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

from cqlparser.cqltoexpression import QueryExpression

class ConvertToFilterQuery(object):

    def __init__(self, availableCores):
        self._availableCores = set(availableCores)

    def convert(self, expression, core):
        filterQueries = {}
        if expression.operator is None:
            e = QueryExpression.nested('AND')
            e.operands.append(expression)
            expression = e
        if expression.operator == 'AND':
            oldOperands = expression.operands
            expression.operands = []
            for operand in oldOperands:
                operandCores = list(self.coresInExpression(expression=operand, core=core))
                if len(operandCores) == 1 and operandCores[0] != core:
                    removeCoreFromFieldname(operandCores[0], operand)
                    filterQueries.setdefault(operandCores[0], []).append(operand)
                else:
                    expression.operands.append(operand)
            if len(expression.operands) == 1:
                expression = expression.operands[0]
            elif len(expression.operands) == 0:
                expression = None
        return expression, filterQueries

    def _otherCores(self, core):
        return set(self._availableCores - set([core]))

    def coresInExpression(self, expression, core):
        if expression.operator:
            result = set()
            for operand in expression.operands:
                result.update(self.coresInExpression(operand, core))
            return result
        return set([self._findCorePrefix(expression.index, core)])

    def _findCorePrefix(self, field, core):
        if field and '.' in field:
            possibleCore, f = field.split('.', 1)
            if possibleCore in self._otherCores(core):
                return possibleCore
        return core

def removeCoreFromFieldname(core, expression):
    if expression.operator:
        for operand in expression.operands:
            removeCoreFromFieldname(core, operand)
        return
    if expression.index and expression.index.startswith(core + "."):
        expression.index = expression.index[len(core)+1:]