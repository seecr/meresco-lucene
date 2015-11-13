package org.meresco.lucene;

import java.io.File;
import java.io.IOException;

public class TestUtils {
    public static File createTempDirectory() throws IOException {
        final File temp;
        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }
        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }
        temp.deleteOnExit();
        return temp;
    }

    static public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i=0; i<files.length; i++) {
                if (files[i].isDirectory()) {
                   deleteDirectory(files[i]);
                } else {
                   files[i].delete();
                }
            }
        }
        return path.delete();
    }
}
