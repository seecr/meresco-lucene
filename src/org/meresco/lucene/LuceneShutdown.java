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

package org.meresco.lucene;

import java.io.File;
import java.util.List;

import org.eclipse.jetty.server.Server;
import org.meresco.lucene.numerate.TermNumerator;

public class LuceneShutdown extends OutOfMemoryShutdown implements Shutdown {

    private static final String SHUTDOWN_FAILED_MARKER = "shutdown.failed.marker";
    private Server server;
    private List<Lucene> lucenes;
    private TermNumerator termNumerator;
    private String stateDir;

    public LuceneShutdown(final Server server, final List<Lucene> lucenes, final TermNumerator termNumerator, String stateDir) {
        this.server = server;
        this.lucenes = lucenes;
        this.termNumerator = termNumerator;
        this.stateDir = stateDir;
        if (new File(this.stateDir, SHUTDOWN_FAILED_MARKER).exists()) {
            throw new RuntimeException("Previous shutdown failed. Won't start!!");
        }
    }
    
    public synchronized void shutdown() {
        boolean successful = true;
        System.out.println("Shutting down lucene. Please wait...");

        try {
            termNumerator.close();
            System.out.println("Shutdown termNumerator completed.");
        } catch (Exception e) {
            e.printStackTrace();
            successful = false;
            System.out.println("Shutdown termNumerator failed.");
        }
        for (Lucene lucene : lucenes) {
            try {
                lucene.close();
                System.out.println("Shutdown " + lucene.name + " completed.");
            } catch (Exception e) {
                e.printStackTrace();
                successful = false;
                System.out.println("Shutdown " + lucene.name + " failed.");
            }
        }
        try {
            // Stop the server after(!) closing Lucene etc.
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Http-server stopped");

        if (!successful) {
            try {
                new File(this.stateDir, SHUTDOWN_FAILED_MARKER).createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.err.flush();
        System.out.flush();
        System.exit(0);
   }
}
