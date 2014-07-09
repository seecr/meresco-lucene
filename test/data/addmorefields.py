#!/usr/bin/env python
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

from os.path import abspath, dirname, join
from os import listdir
mypath = dirname(abspath(__file__))

def main():
    thedir = join(mypath, 'main')
    for req in listdir(thedir):
        if not req.endswith('.updateRequest'):
            continue
        f = join(thedir, req)
        print f
        number = int(req.split('_')[0])
        parentnr = number % 2
        childnr = number % 3
        grandchildnr = number % 5
        newfield = '''<fieldHier>
            <value>parent{parentnr}</value>
            <value>child{childnr}</value>
            <value>grandchild{grandchildnr}</value>
        </fieldHier>\n'''.format(**locals())
        data = open(f).read().replace('</document>', newfield+'</document>')
        with open(f, 'w') as g:
            g.write(data)

if __name__ == '__main__':
    main()