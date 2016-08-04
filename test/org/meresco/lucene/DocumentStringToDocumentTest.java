/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;
import org.meresco.lucene.numerate.TermNumerator;

public class DocumentStringToDocumentTest {
    TermNumerator mockTermNumerator = new MockTermNumerator();

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
    public void testSortedStringField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StringField")
                    .add("name", "name")
                    .add("value", "value")
                    .add("sort", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(DocValuesType.SORTED, result.getField("name").fieldType().docValuesType());
        assertEquals(new BytesRef("value"), result.getField("name").binaryValue());
    }

    @Test
    public void testKeyField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "KeyField")
                    .add("name", "name")
                    .add("value", "value"))
                .add(Json.createObjectBuilder()
                	.add("type", "KeyField")
					.add("name", "name2")
					.add("value", 153))
                .build();
        Document result = convert(json.toString());
        assertEquals(NumericDocValuesField.TYPE, result.getField("name").fieldType());
        assertEquals(43, result.getField("name").numericValue().intValue());

        assertEquals(NumericDocValuesField.TYPE, result.getField("name2").fieldType());
        assertEquals(153, result.getField("name2").numericValue().intValue());
    }

    @Test
    public void testStringFieldStored() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StringField")
                    .add("name", "name")
                    .add("value", "value")
                    .add("stored", JsonValue.TRUE))
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
                    .add("termVectors", JsonValue.TRUE)
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
        assertTrue(field.fieldType().tokenized());
        assertTrue(field.fieldType().omitNorms());
        assertEquals(IndexOptions.DOCS, field.fieldType().indexOptions());
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
        assertEquals(new IntPoint("name", 1).fieldType(), result.getField("name").fieldType());
        assertEquals(1, result.getField("name").numericValue().intValue());
    }

    @Test
    public void testSortedIntField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "IntField")
                    .add("name", "name")
                    .add("value", 1)
                    .add("sort", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(DocValuesType.NUMERIC, result.getField("name").fieldType().docValuesType());
        assertEquals(1, result.getField("name").numericValue().intValue());
    }

    @Test
    public void testStoredIntField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "IntField")
                    .add("name", "name")
                    .add("value", 1)
                    .add("stored", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(new StoredField("name", 1).fieldType(), result.getField("name").fieldType());
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
        assertEquals(new LongPoint("name", 1).fieldType(), result.getField("name").fieldType());
        assertEquals(1L, result.getField("name").numericValue().longValue());
    }

    @Test
    public void testSortedLongField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "LongField")
                    .add("name", "name")
                    .add("value", 1L)
                    .add("sort", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(DocValuesType.NUMERIC, result.getField("name").fieldType().docValuesType());
        assertEquals(1L, result.getField("name").numericValue().longValue());
    }

    @Test
    public void testStoredLongField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "LongField")
                    .add("name", "name")
                    .add("value", 1)
                    .add("stored", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(new StoredField("name", 1L).fieldType(), result.getField("name").fieldType());
        assertEquals(1L, result.getField("name").numericValue().longValue());
    }

    @Test
    public void testDoubleField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "DoubleField")
                    .add("name", "name")
                    .add("value", 1.5))
                .build();
        Document result = convert(json.toString());
        assertEquals(new DoublePoint("name", 1).fieldType(), result.getField("name").fieldType());
        assertEquals(1.5D, result.getField("name").numericValue().doubleValue(), 0);
    }

    @Test
    public void testSortedDoubleField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "DoubleField")
                    .add("name", "name")
                    .add("value", 1.5D)
                    .add("sort", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(DocValuesType.NUMERIC, result.getField("name").fieldType().docValuesType());
        assertEquals(NumericUtils.doubleToSortableLong(1.5D), result.getField("name").numericValue().longValue());
    }

    @Test
    public void testStoredDoubleField() {
        JsonArray json = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "DoubleField")
                    .add("name", "name")
                    .add("value", 1.1D)
                    .add("stored", JsonValue.TRUE))
                .build();
        Document result = convert(json.toString());
        assertEquals(new StoredField("name", 1.1D).fieldType(), result.getField("name").fieldType());
        assertEquals(1.1D, result.getField("name").numericValue().doubleValue(), 0);
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
        Reader reader = new StringReader(documentString);
        try {
            return new DocumentStringToDocument(reader, mockTermNumerator).convert();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class MockTermNumerator extends TermNumerator {
        int ord = 42;
        public int numerateTerm(String value) {
            return ++ord;
        }
    }
}
