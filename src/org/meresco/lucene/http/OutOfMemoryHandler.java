package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Shutdown;

public abstract class OutOfMemoryHandler extends AbstractHandler {

    private final Shutdown shutdown;

    public OutOfMemoryHandler(Shutdown shutdown) {
        this.shutdown = shutdown;
    }

    @Override
    public final void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            doHandle(target, baseRequest, request, response);
        } catch (OutOfMemoryError e) {
            this.shutdown.shutdownInOutOfMemorySituation();
        }
    }

    public abstract void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException;
}
