/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class Meresco3DVectorTest {

        @Test
        public void testSimpleTextEntries() {
                Meresco3DVector v1 = new Meresco3DVector();
                v1.setEntry(0, 3.0);
                v1.setEntry(1, 2.0);
                v1.setEntry(2, 1.0);
                Meresco3DVector v2 = new Meresco3DVector();
                v2.setEntry(0, 3.0);
                assertEquals(0.555, v1.distance(v2), 0.001);
                Meresco3DVector v3 = new Meresco3DVector();
                v3.setEntry(0, 3.0);
                v3.setEntry(1, 2.0);
                assertEquals(0.192, v1.distance(v3), 0.001);
                Meresco3DVector v4 = new Meresco3DVector();
                v4.setEntry(0, 3.0);
                v4.setEntry(1, 2.0);
                v4.setEntry(2, 1.0);
                assertEquals(0.0, v1.distance(v4), 0.001);
        }

        @Test
        public void testOntologyEntries() {
                Meresco3DVector v1 = new Meresco3DVector();
                v1.setEntry(0, 3.0);
                v1.setEntry(1, 2.0);
                v1.setEntry(2, 1.0);
                Meresco3DVector v2 = new Meresco3DVector();
                v2.setEntry(0, 3.0);
                v2.setEntry(1, 2.0);
                v2.setEntry(2, 1.0);
                assertEquals(0.0, v1.distance(v2), 0.001);
        }
}
