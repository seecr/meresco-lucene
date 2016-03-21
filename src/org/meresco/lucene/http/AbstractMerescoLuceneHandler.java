package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Shutdown;
import org.meresco.lucene.Utils;

public abstract class AbstractMerescoLuceneHandler extends AbstractHandler {

    private final Shutdown shutdown;

    public AbstractMerescoLuceneHandler(Shutdown shutdown) {
        this.shutdown = shutdown;
    }

    @Override
    public final void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        try {
            doHandle(target, baseRequest, request, response);
        } catch (OutOfMemoryError e) {
            this.shutdown.shutdownInOutOfMemorySituation();
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
        }
        baseRequest.setHandled(true);
    }

    public abstract void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception;
}
