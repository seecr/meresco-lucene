/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene.numerate;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;
import org.meresco.lucene.SeecrTestCase;
import org.meresco.lucene.numerate.TermNumerator;

public class TermNumeratorTest extends SeecrTestCase {

    @Test
    public void test() throws IOException {
        TermNumerator numerator = new TermNumerator(this.tmpDir);
        assertEquals(1, numerator.numerateTerm("string1"));
        assertEquals(2, numerator.numerateTerm("string2"));
        assertEquals(1, numerator.numerateTerm("string1"));
    }

    @Test
    public void testCommit() throws IOException {
        TermNumerator numerator = new TermNumerator(this.tmpDir);
        assertEquals(1, numerator.numerateTerm("string1"));
        assertEquals("string1", numerator.getTerm(1));
        assertEquals(2, numerator.numerateTerm("string2"));
        numerator.commit();
        assertEquals("string2", numerator.getTerm(2));
    }
    
    @Test
    public void testGetTerm() throws IOException {
        TermNumerator numerator = new TermNumerator(this.tmpDir);
        int one = numerator.numerateTerm("one");
        assertEquals("one", numerator.getTerm(one));
        int two = numerator.numerateTerm("two");
        assertEquals("two", numerator.getTerm(two));
        try {
            numerator.numerateTerm("");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

}
