package test;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.DocumentStringToDocument;

public class DocumentStringToDocumentTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testStringField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StringField")
                    .add("name", "name")
                    .add("value", "value"))
                .build();
        Document result = convert(json.toString());
        assertEquals(StringField.TYPE_NOT_STORED, result.getField("name").fieldType());
        assertEquals("value", result.getField("name").stringValue());
    }
    
    @Test
    public void testStringFieldStored() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StringFieldStored")
                    .add("name", "name")
                    .add("value", "value"))
                .build();
        Document result = convert(json.toString());
        assertEquals(StringField.TYPE_STORED, result.getField("name").fieldType());
        assertEquals("value", result.getField("name").stringValue());
    }

    @Test
    public void testTextField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "TextField")
                    .add("name", "name")
                    .add("value", "value"))
                .build();
        Document result = convert(json.toString());
        assertEquals(TextField.TYPE_NOT_STORED, result.getField("name").fieldType());
        assertEquals("value", result.getField("name").stringValue());
    }

    private Document convert(String documentString) {
        return new DocumentStringToDocument(new StringReader(documentString)).convert();
    }
}
