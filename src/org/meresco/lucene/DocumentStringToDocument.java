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

