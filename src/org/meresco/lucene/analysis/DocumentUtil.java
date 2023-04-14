/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2017 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.analysis.Analyzer;

import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;


import org.meresco.lucene.analysis.TextField;


public class DocumentUtil {

    /* All facets are the same, so we use a fixed default config. */
    static FacetsConfig.DimConfig defaultDimConfig = new FacetsConfig.DimConfig();
    static {
        defaultDimConfig.hierarchical = true;
        defaultDimConfig.multiValued = true;
        defaultDimConfig.requireDimCount = true;
        defaultDimConfig.drillDownTermsIndexing = FacetsConfig.DrillDownTermsIndexing.ALL;
    };

    static FacetsConfig config = new FacetsConfig() {
        @Override
        protected FacetsConfig.DimConfig getDefaultDimConfig() {
            return defaultDimConfig;
        };
    };

    public static void add_StringFields(Document doc, String[] prefixes, int i, String tag, String value, Field.Store store, boolean facets) {
        for (int j=0; j<=i; j++) {
            String fieldname = prefixes[j] + tag;
            doc.add(new StringField(fieldname, value, store));
            if (facets && !prefixes[j].isEmpty()) {
                doc.add(new FacetField(fieldname + ".facet", value));
            }
        }

    };
    public static void add_TextFields(Document doc, String[] prefixes, int i, String tag, String value, Field.Store store, int gap, Analyzer analyzer, boolean facets) {
        for (int j=0; j<=i; j++) {
            String fieldname = prefixes[j] + tag;
            doc.add(new TextField(fieldname, value, store, gap, analyzer));
            if (facets && !prefixes[j].isEmpty()) {
                doc.add(new FacetField(fieldname + ".facet", value));
            }
        }
    };
    public static Document build_facets_with_all_bells_and_whisles(Document doc, TaxonomyWriter taxoWriter) throws IOException {
        return config.build(taxoWriter, doc);
    }
}
