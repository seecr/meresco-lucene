#!/usr/bin/env python
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

TEMPLATE = """<ucp:updateRequest xmlns:ucp="info:lc/xmlns/update-v1">
    <srw:version xmlns:srw="http://www.loc.gov/zing/srw/">1.0</srw:version>
    <ucp:action>info:srw/action/1/replace</ucp:action>
    <ucp:recordIdentifier>%(identifier)s</ucp:recordIdentifier>
    <srw:record xmlns:srw="http://www.loc.gov/zing/srw/">
        <srw:recordPacking>xml</srw:recordPacking>
        <srw:recordSchema>data</srw:recordSchema>
        <srw:recordData><document xmlns='http://meresco.org/namespace/example'>
    %(fields)s
</document></srw:recordData>
    </srw:record>
</ucp:updateRequest>"""

from random import choice, randint
from string import ascii_letters, digits

randomWord = lambda length: ''.join(choice(ascii_letters + digits) for i in xrange(length))
randomSentence = lambda words, maxlength: ' '.join(randomWord(randint(2,maxlength)) for w in xrange(words))

def createRecord(recordNumber):
    identifier = 'record:%s' % recordNumber
    fields = '\n'.join([
        "<field1>%s</field1>" % randomWord(10),
        "<field2>value%s</field2>" % (recordNumber % 10),
        "<intfield1>%s</intfield1>" % (1000+recordNumber),
        "<intfield2>%s</intfield2>" % randint(100, 999),
        "<intfield3>%s</intfield3>" % randint(5000, 5010),
        "<field3>%s</field3>" % randomSentence(10, 12),
        ])
    return TEMPLATE % locals()

def main():
    for recordNumber in xrange(3, 101):
        data = createRecord(recordNumber)
        filename = '%05d_record:%s.updateRequest' % (recordNumber, recordNumber)
        with open(filename, 'w') as f:
            print filename
            f.write(data)

if __name__ == '__main__':
    main()