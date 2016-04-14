package org.meresco.lucene;

public abstract class OutOfMemoryShutdown implements Shutdown {

    public OutOfMemoryShutdown() {
        super();
    }

    public void shutdownInOutOfMemorySituation() {
        System.out.println("Shutting down due to OutOfMemoryError");
        shutdown();
    }
}