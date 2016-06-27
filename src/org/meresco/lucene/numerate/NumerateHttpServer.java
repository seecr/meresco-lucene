/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class NumerateHttpServer {

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option option = new Option("p", "port", true, "Port number");
        option.setType(Integer.class);
        option.setRequired(true);
        options.addOption(option);

        option = new Option("d", "stateDir", true, "Directory in which lucene data is located");
        option.setType(String.class);
        option.setRequired(true);
        options.addOption(option);

        PosixParser parser = new PosixParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (MissingOptionException e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("start-numerate-server" , options);
            System.exit(1);
        }

        Integer port = new Integer(commandLine.getOptionValue("p"));
        String stateDir = commandLine.getOptionValue("d");

        if (Charset.defaultCharset() != Charset.forName("UTF-8")) {
        System.err.println("file.encoding must be UTF-8.");
            System.exit(1);
        }

        TermNumerator termNumerator = new TermNumerator(new File(stateDir, "keys-termnumerator"));

        ExecutorThreadPool pool = new ExecutorThreadPool(50, 200, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
        Server server = new Server(pool);
        ServerConnector http = new ServerConnector(server, new HttpConnectionFactory());
        http.setPort(port);
        server.addConnector(http);

        registerShutdownHandler(stateDir, termNumerator, server);

        server.setHandler(new NumerateHandler(termNumerator));
        server.start();
        server.join();
    }

    public static void registerShutdownHandler(final String stateDir, final TermNumerator termNumerator, final Server server) {
        File runningMarker = new File(stateDir, "running.marker");
        if (runningMarker.exists()) {
            System.err.println("Previous shutdown failed, will not start");
            System.exit(1);
        } else {
            try {
                new FileOutputStream(runningMarker).close();
                runningMarker.deleteOnExit();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Signal.handle(new Signal("TERM"), new SignalHandler() {
            public void handle(Signal sig) {
                shutdown(server, termNumerator);
            }
        });
        Signal.handle(new Signal("INT"), new SignalHandler() {
            public void handle(Signal sig) {
                shutdown(server, termNumerator);
            }
        });
    }

    static void shutdown(final Server server, final TermNumerator termNumerator) {
        System.out.println("Shutting down termNumerator. Please wait...");
        try {
            termNumerator.close();
            System.out.println("Shutdown termNumerator completed.");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Shutdown termNumerator failed.");
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
