package org.meresco.lucene;

import java.io.StringReader;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;

public class DocumentStringToDocument {

    private JsonArray object;

    public DocumentStringToDocument(String document) {
        JsonReader jsonReader = Json.createReader(new StringReader(document));
        object = (JsonArray) jsonReader.read();
    }

    public Document convert() {
        Document doc = new Document();
        Iterator<JsonValue> iterator = object.iterator();
        while (iterator.hasNext()) {
            JsonObject jsonValue = (JsonObject) iterator.next();
            doc.add(createField(jsonValue));
        }
        return doc;
    }

    private IndexableField createField(JsonObject jsonValue) {
        switch (jsonValue.getString("type")) {
        case "StringField":
            return new Field(jsonValue.getString("name"), jsonValue.getString("value"), StringField.TYPE_NOT_STORED);
        case "StringFieldStored":
            return new Field(jsonValue.getString("name"), jsonValue.getString("value"), StringField.TYPE_STORED);
        case "TextField":
            return new Field(jsonValue.getString("name"), jsonValue.getString("value"), TextField.TYPE_NOT_STORED);
        }
        throw new UnknownTypeException();
    }
}

class UnknownTypeException extends RuntimeException {};