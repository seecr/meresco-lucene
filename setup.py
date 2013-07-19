#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009-2010 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

version = '$Version: trunk$'[9:-1].strip()

from distutils.core import setup
from distutils.extension import Extension
from os import walk
from os.path import join

scripts = []
for path, dirs, files in walk('bin'):
    for file in files:
        scripts.append(join(path, file))
packages = []
for path, dirs, files in walk('meresco'):
    if '__init__.py' in files and path != 'meresco':
        packages.append(path.replace('/', '.'))


setup(
    name = 'meresco-lucene',
    packages = [
        'meresco',                      #DO_NOT_DISTRIBUTE
    ] + packages,
    scripts=scripts,
    package_data={},
    version = version,
    url = 'http://seecr.nl',
    author = 'Seecr (Seek You Too B.V.)',
    author_email = 'info@seecr.nl',
    description = 'A set of components and tools to integrate Lucene into Meresco',
    long_description = '"Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco',
    license = 'GPLv2',
    platforms = 'all',
)
