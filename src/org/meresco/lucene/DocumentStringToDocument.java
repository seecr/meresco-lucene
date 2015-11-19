package org.meresco.lucene;

import java.io.Reader;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;

public class DocumentStringToDocument {

    private JsonArray object;

    public DocumentStringToDocument(Reader documentReader) {
        JsonReader jsonReader = Json.createReader(documentReader);
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

    private IndexableField createField(JsonObject jsonField) {
        String name = jsonField.getString("name");
        Field field = null;
        switch (jsonField.getString("type")) {
            case "StringField":
                field = new Field(name, jsonField.getString("value"), maybeAddTermVectors(jsonField, StringField.TYPE_NOT_STORED));
                break;
            case "StringFieldStored":
                field = new Field(name, jsonField.getString("value"), maybeAddTermVectors(jsonField, StringField.TYPE_STORED));
                break;
            case "TextField":
                field = new Field(name, jsonField.getString("value"), maybeAddTermVectors(jsonField, TextField.TYPE_NOT_STORED));
                break;
            case "NoTermsFrequencyField":
                field = new Field(name, jsonField.getString("value"), NO_TERMS_FREQUENCY_FIELD);
                break;
            case "IntField":
                field = new IntField(name, jsonField.getInt("value"), Store.NO);
                break;
            case "DoubleField":
                field = new DoubleField(name, jsonField.getInt("value"), Store.NO);
                break;
            case "LongField":
                field = new LongField(name, jsonField.getInt("value"), Store.NO);
                break;
            case "NumericField":
                field = new NumericDocValuesField(name, jsonField.getJsonNumber("value").longValue());
                break;
            case "FacetField":
                JsonArray jsonArray = jsonField.getJsonArray("path");
                String[] path = new String[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    path[i] = jsonArray.getString(i);
                }
                field = new FacetField(name, path);
        }
        if (field == null)
            throw new UnknownTypeException();
        
        return field;
    }
    
    private FieldType maybeAddTermVectors(JsonObject jsonValue, FieldType type) {
        if (!jsonValue.getBoolean("termVectors", false))
            return type;
        
        FieldType fieldType = new FieldType(type);
        fieldType.setStoreTermVectors(true);
        return fieldType;
    }
    
    private static final FieldType NO_TERMS_FREQUENCY_FIELD = new FieldType();
    static {
        NO_TERMS_FREQUENCY_FIELD.setIndexed(true);
        NO_TERMS_FREQUENCY_FIELD.setTokenized(true);
        NO_TERMS_FREQUENCY_FIELD.setOmitNorms(true);
        NO_TERMS_FREQUENCY_FIELD.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
    }
}

class UnknownTypeException extends RuntimeException {};

