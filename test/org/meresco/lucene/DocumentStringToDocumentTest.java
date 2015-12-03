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

import static org.junit.Assert.*;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.DocumentStringToDocument;

public class DocumentStringToDocumentTest {

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
        IndexableFieldType fieldType = result.getField("name").fieldType();
        assertEquals(TextField.TYPE_NOT_STORED, fieldType);
        assertFalse(fieldType.storeTermVectors());
        assertEquals("value", result.getField("name").stringValue());
    }

    @Test
    public void testTextFieldWithTermVectors() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "TextField")
                    .add("termVectors", true)
                    .add("name", "name")
                    .add("value", "value"))
                .build();
        Document result = convert(json.toString());
        IndexableFieldType fieldType = result.getField("name").fieldType();
        assertTrue(fieldType.storeTermVectors());
        assertEquals("value", result.getField("name").stringValue());
    }

    @Test
    public void testNoTermsFrequencyField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "NoTermsFrequencyField")
                    .add("name", "name")
                    .add("value", "value"))
                .build();
        Document result = convert(json.toString());
        IndexableField field = result.getField("name");
        assertTrue(field.fieldType().indexed());
        assertTrue(field.fieldType().tokenized());
        assertTrue(field.fieldType().omitNorms());
        assertEquals(FieldInfo.IndexOptions.DOCS_ONLY, field.fieldType().indexOptions());
        assertEquals("value", field.stringValue());
    }

    @Test
    public void testIntField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "IntField")
                    .add("name", "name")
                    .add("value", 1))
                .build();
        Document result = convert(json.toString());
        assertEquals(IntField.TYPE_NOT_STORED, result.getField("name").fieldType());
        assertEquals(1, result.getField("name").numericValue().intValue());
    }

    @Test
    public void testLongField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "LongField")
                    .add("name", "name")
                    .add("value", 1))
                .build();
        Document result = convert(json.toString());
        assertEquals(LongField.TYPE_NOT_STORED, result.getField("name").fieldType());
        assertEquals(1, result.getField("name").numericValue().longValue());
    }

    @Test
    public void testDoubleField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "DoubleField")
                    .add("name", "name")
                    .add("value", 1))
                .build();
        Document result = convert(json.toString());
        assertEquals(DoubleField.TYPE_NOT_STORED, result.getField("name").fieldType());
        assertEquals(1, result.getField("name").numericValue().doubleValue(), 0);
    }

    @Test
    public void testNumericField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "NumericField")
                    .add("name", "name")
                    .add("value", 1))
                .build();
        Document result = convert(json.toString());
        assertEquals(NumericDocValuesField.TYPE, result.getField("name").fieldType());
        assertEquals(1, result.getField("name").numericValue().doubleValue(), 0);
    }

    @Test
    public void testFacetField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "FacetField")
                    .add("name", "name")
                    .add("path", Json.createArrayBuilder()
                            .add("path")
                            .add("sub")))
                .build();
        Document result = convert(json.toString());
        assertEquals(1, result.getFields().size());
        FacetField field = (FacetField) result.getFields().get(0);
        assertEquals("name", field.dim);
        assertArrayEquals(new String[] { "path", "sub"}, field.path);
    }

    private Document convert(String documentString) {
        return new DocumentStringToDocument(new StringReader(documentString)).convert();
    }
}
