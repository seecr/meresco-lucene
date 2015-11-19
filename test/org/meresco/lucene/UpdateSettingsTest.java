package org.meresco.lucene;

import static org.junit.Assert.*;

import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.http.SettingsHandler;

public class UpdateSettingsTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"commitCount\": 1, \"commitTimeout\": 1}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(1, settings.commitCount);
        assertEquals(1, settings.commitTimeout);
    }

}
