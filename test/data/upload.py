#!/usr/bin/env python3
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013, 2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from seecrdeps import includeParentAndDeps       #DO_NOT_DISTRIBUTE
includeParentAndDeps(__file__, scanForDeps=True) #DO_NOT_DISTRIBUTE

from os.path import dirname, abspath, join, basename
from lxml.etree import XML, tostring
from os import listdir, getpid
from sys import argv, exit
from seecr.test.utils import postRequest
from time import time
from seecr.utils import read_from_file

mypath = dirname(abspath(__file__))

def main(port, **kwargs):
    t0 = time()
    try:
        uploadUpdateRequests(mypath, port)
    finally:
        t1 = time()
        print('Took %s seconds' % (t1 - t0))

def uploadUpdateRequests(datadir, uploadPort):
    for core in ['main', 'main2']:
        coreDir = join(datadir, core)
        uploadPath = '/update_%s' % basename(coreDir)
        requests = (join(coreDir, r) for r in sorted(listdir(coreDir)) if r.endswith('.updateRequest'))
        for filename in requests:
            print('http://localhost:%s%s' % (uploadPort, uploadPath), '<-', basename(filename)[:-len('.updateRequest')])
            updateRequest = read_from_file(filename)
            _uploadUpdateRequest(updateRequest, uploadPath, uploadPort)

def _uploadUpdateRequest(updateRequest, uploadPath, uploadPort):
    XML(updateRequest)
    statusAndHeaders, body = postRequest(uploadPort, uploadPath, updateRequest)
    if statusAndHeaders['StatusCode'] != '200':
        print('No 200 OK response, but:\n', status, header, body)
        exit(123)
    if "srw:diagnostics" in tostring(body, encoding=str):
        print(tostring(body, encoding=str))
        exit(1234)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description="""This will upload all requests in this directory to the given server on localhost.
        If used with random will create <number> new random records starting with start number (default 1)""")
    parser.add_argument("-p", "--port", type=int, required=True)
    parser.add_argument("--pid", action="store_true", default=False)
    args = parser.parse_args()
    if args.pid:
        print('>> pid :', getpid())
    main(**vars(args))
