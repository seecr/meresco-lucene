/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

import java.lang.Math;

public final class HashSetLinear
{
    private final double HALF_RANGE = Math.pow(2, 63);
    private final long[] hashArray;
    private final int size;
    private final int half_size;
    private final double scaleFactor;

    public HashSetLinear(int size) {
        this.size = size;
        this.half_size = size >> 1;
        this.hashArray = new long[(int) Math.ceil(size * 1.1)];
        this.scaleFactor = size / 2 / HALF_RANGE;
    }

    public boolean contains(long hash) {
        int index = (int) (scaleFactor * hash) + half_size;
        while (true) {
            long h = hashArray[index++];
            if (h == hash) {
                return true;
            } else if (h == 0 || h > hash || index == this.hashArray.length) {
                return false;
            }
        }
    }
    
    public void add(long hash) {
        if (hash == 0) 
        	throw new RuntimeException("0 is not allowed");
    	int index = (int) (scaleFactor * hash) + half_size;
    	long v = this.hashArray[index];
    	while (v != 0) {
    		if (v == hash)
        		return;
    		if (v > hash) {
    			this.hashArray[index] = hash;
    			hash = v;
    		}
		    v = this.hashArray[++index];
    	}
    	this.hashArray[index] = hash;
    }
};