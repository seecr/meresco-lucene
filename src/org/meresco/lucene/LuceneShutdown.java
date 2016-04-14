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
