/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class SuperCollector<SubCollectorType extends SubCollector> {

    protected List<SubCollectorType> subs = new ArrayList<SubCollectorType>();

    /**
     * Called before collecting from each {@link AtomicReaderContext} in a
     * separate thread. The returned {@link SubCollector} need not be thread
     * safe as its scope is limited to one segment.
     * 
     * The SubCollector is kept in a list and accessible by {@link #subs}.
     * 
     * @param context
     *            next atomic reader context
     * @throws IOException
     */
    public SubCollectorType subCollector() throws IOException {
        SubCollectorType sub = this.createSubCollector();
        this.subs.add(sub);
        return sub;
    }

    /**
     * Lower level factory method for SubCollectors.
     * 
     * @param context
     *            is an AtomicReaderContext
     * @return SubCollector for this context
     * @throws IOException
     */
    abstract protected SubCollectorType createSubCollector() throws IOException;
}
