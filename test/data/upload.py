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

from os.path import dirname, abspath, join, basename
from lxml.etree import XML, tostring
from os import listdir, getpid
from sys import argv, exit
from seecr.test.utils import postRequest
from optparse import OptionParser

mypath = dirname(abspath(__file__))

def main(port):
    uploadUpdateRequests(mypath, '/update', port)

def uploadUpdateRequests(datadir, uploadPath, uploadPort):
    requests = (join(datadir, r) for r in sorted(listdir(datadir)) if r.endswith('.updateRequest'))
    for filename in requests:
        _uploadUpdateRequest(filename, uploadPath, uploadPort)

def _uploadUpdateRequest(filename, uploadPath, uploadPort):
    print 'http://localhost:%s%s' % (uploadPort, uploadPath), '<-', basename(filename)[:-len('.updateRequest')]
    updateRequest = open(filename).read()
    XML(updateRequest)
    header, body = postRequest(uploadPort, uploadPath, updateRequest)
    if '200 Ok' not in header:
        print 'No 200 Ok response, but:\n', header
        exit(123)
    if "srw:diagnostics" in tostring(body):
        print tostring(body)
        exit(1234)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p", "--port", type=int)
    parser.add_option("", "--pid", action="store_true", default=False)
    options, arguments = parser.parse_args()
    if options.port is None:
        print """Usage: %s --port <portnumber>
        This will upload all requests in this directory to the given server on localhost.""" % argv[0]
        exit(1)
    if options.pid:
        print '>> pid :', getpid()
    main(options.port)