#!/usr/bin/env python

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

for recordNumber in xrange(3, 101):
    identifier = 'record:%s' % recordNumber
    fields = '\n'.join([
        "<field1>%s</field1>" % randomWord(10),
        "<field2>value%s</field2>" % (recordNumber % 10),
        "<intfield1>%s</intfield1>" % (1000+recordNumber),
        "<intfield2>%s</intfield2>" % randint(100, 999),
        "<intfield3>%s</intfield3>" % randint(5000, 5010),
        "<field3>%s</field3>" % randomSentence(10, 12),
        ])
    filename = '%05d_record:%s.updateRequest' % (recordNumber, recordNumber)
    with open(filename, 'w') as f:
        print filename
        f.write(TEMPLATE % locals())
