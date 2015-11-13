package org.meresco.lucene;

import java.io.File;

import org.junit.After;
import org.junit.Before;

public class SeecrTestCase {

    protected File tmpDir;

    public SeecrTestCase() {
        super();
    }

    @Before
    public void setUp() throws Exception {
        this.tmpDir = TestUtils.createTempDirectory();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDirectory(this.tmpDir);
    }

}