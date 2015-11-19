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

package org.meresco.lucene.http;

import java.io.File;
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
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.sun.net.httpserver.HttpServer;

public class LuceneHttpServer {
    private static HttpServer server = null;

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
            helpFormatter.printHelp("start-lucene-server" , options);
            System.exit(1);
        }

        Integer port = new Integer(commandLine.getOptionValue("p"));
        String storeLocation = commandLine.getOptionValue("d");

        if (Charset.defaultCharset() != Charset.forName("UTF-8")) {
        System.err.println("file.encoding must be UTF-8.");
            System.exit(1);
        }

        Lucene lucene = new Lucene(new File(storeLocation), new LuceneSettings());
        
        ExecutorThreadPool pool = new ExecutorThreadPool(50, 200, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
        Server server = new Server(pool);
        ServerConnector http = new ServerConnector(server, new HttpConnectionFactory());
        http.setPort(port);
        server.addConnector(http);
                
        registerShutdownHandler(lucene, server);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        ContextHandler context = new ContextHandler("/query");
        context.setHandler(new QueryHandler(lucene));
        contexts.addHandler(context);
        
        context = new ContextHandler("/update");
        context.setHandler(new UpdateHandler(lucene));
        contexts.addHandler(context);

        context = new ContextHandler("/delete");
        context.setHandler(new DeleteHandler(lucene));
        contexts.addHandler(context);

        server.setHandler(contexts);
        server.start();
        server.join();
    }

    static void registerShutdownHandler(final Lucene lucene, final Server server) {
        Signal.handle(new Signal("TERM"), new SignalHandler() {
            public void handle(Signal sig) {
                shutdown(server, lucene);
            }
        });
        Signal.handle(new Signal("INT"), new SignalHandler() {
            public void handle(Signal sig) {
                shutdown(server, lucene);
            }
        });
    }

    static void shutdown(final Server server, final Lucene lucene) {
        System.out.println("Shutting down lucene. Please wait...");
        try {
            lucene.close();
            System.out.println("Shutdown completed.");
            System.out.flush();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.flush();
            System.out.println("Shutdown failed.");
            System.out.flush();
        }
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Http-server stopped");
        System.exit(0);
   }
}
