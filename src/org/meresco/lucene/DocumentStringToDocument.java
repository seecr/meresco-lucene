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

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.meresco.lucene.numerate.TermNumerator;

public class DocumentStringToDocument {
    private JsonArray object;
    private TermNumerator termNumerator;

    public DocumentStringToDocument(Reader documentReader, TermNumerator termNumerator) {
        JsonReader jsonReader = Json.createReader(documentReader);
        object = (JsonArray) jsonReader.read();
        this.termNumerator = termNumerator;
    }

    public Document convert() throws IOException {
        Document doc = new Document();
        Iterator<JsonValue> iterator = object.iterator();
        while (iterator.hasNext()) {
            JsonObject jsonValue = (JsonObject) iterator.next();
            doc.add(createField(jsonValue));
        }
        return doc;
    }

    private IndexableField createField(JsonObject jsonField) throws IOException {
        String name = jsonField.getString("name");
        Field field = null;
        switch (jsonField.getString("type")) {
            case "StringField":
                String stringValue = jsonField.getString("value");
                if (jsonField.getBoolean("sort", false))
                    field = new SortedDocValuesField(name, new BytesRef(stringValue));
                else
                    field = new Field(name, stringValue, maybeAddTermVectors(jsonField, StringField.TYPE_NOT_STORED));
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
                int intValue = jsonField.getInt("value");
                if (jsonField.getBoolean("sort", false))
                    field = new SortedNumericDocValuesField(name, intValue);
                else
                    field = new IntPoint(name, intValue);
                break;
            case "DoubleField":
                double doubleValue = jsonField.getJsonNumber("value").doubleValue();
                if (jsonField.getBoolean("sort", false))
                    field = new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(doubleValue));
                else
                    field = new DoublePoint(name, doubleValue);
                break;
            case "LongField":
                long longValue = jsonField.getJsonNumber("value").longValue();
                if (jsonField.getBoolean("sort", false))
                    field = new SortedNumericDocValuesField(name, longValue);
                else
                    field = new LongPoint(name, longValue);
                break;
            case "NumericField":
                field = new NumericDocValuesField(name, jsonField.getJsonNumber("value").longValue());
                break;
            case "KeyField":
            	int value;
            	if (jsonField.get("value").getValueType().equals(ValueType.STRING)) {
            		value = termNumerator.numerateTerm(jsonField.getString("value"));
            	}
            	else {
            		value = jsonField.getInt("value");
            	}
            	field = new NumericDocValuesField(name, value);
                break;
            case "FacetField":
                JsonArray jsonArray = jsonField.getJsonArray("path");
                String[] path = new String[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    path[i] = jsonArray.getString(i);
                }
                field = new FacetField(name, path);
        }
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
        NO_TERMS_FREQUENCY_FIELD.setTokenized(true);
        NO_TERMS_FREQUENCY_FIELD.setOmitNorms(true);
        NO_TERMS_FREQUENCY_FIELD.setIndexOptions(IndexOptions.DOCS);
    }
}