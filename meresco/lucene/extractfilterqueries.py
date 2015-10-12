## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015 Stichting Kennisnet http://www.kennisnet.nl
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

from cqlparser.cqltoexpression import QueryExpression

class TooComplexQueryExpression(Exception):
    pass

class ExtractFilterQueries(object):
    def __init__(self, availableCores):
        self._availableCores = set(availableCores)

    def convert(self, expression, core):
        filterQueries = {}
        if expression.operator is None or expression.operator == 'OR':
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
        if expression and self.coresInExpression(expression=expression, core=core) != set([core]):
            raise TooComplexQueryExpression('Multiple core query detected, but unable to convert to a correct composed query')
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