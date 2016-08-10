/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.suggestion;

import java.io.IOException;

import org.eclipse.jetty.server.Server;
import org.meresco.lucene.OutOfMemoryShutdown;
import org.meresco.lucene.Shutdown;

public class SuggestionShutdown extends OutOfMemoryShutdown implements Shutdown {
    private Server server;
    private SuggestionIndex suggestionIndex;

    public SuggestionShutdown(final Server server, final SuggestionIndex suggestionIndex) {
        this.server = server;
        this.suggestionIndex = suggestionIndex;
    }

    public void shutdown() {
        System.out.println("Shutting down suggestions. Please wait...");
        try {
            suggestionIndex.close();
            System.out.println("Shutdown suggestions completed.");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Shutdown suggestions failed.");
        }
        try {
            // Stop the server after(!) closing Lucene etc.
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Http-server stopped");
        System.err.flush();
        System.out.flush();
        System.exit(0);
   }
}
