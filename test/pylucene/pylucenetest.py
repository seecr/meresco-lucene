
from seecr.test import SeecrTestCase

from org.meresco.lucene import Lucene, LuceneSettings, LuceneResponse, QueryData, JsonQueryConverter, MultiLucene, ComposedQuery
FacetRequest = JsonQueryConverter.FacetRequest
from org.apache.lucene.search import TermQuery, PhraseQuery, BooleanQuery, BooleanClause, MatchAllDocsQuery
from org.apache.lucene.document import Document, TextField, Field
from org.apache.lucene.facet import FacetField
from java.nio.file import Path
from java.util import ArrayList

class PyLuceneTest(SeecrTestCase):

    def testFacets(self):
        luc = Lucene("core_name", Path.of(self.tmp_path.as_posix()))
        luc.initSettings(LuceneSettings())
        populate_lhs(luc)

        self.assertEqual(5, luc.numDocs())

        q = QueryData()
        q.query = MatchAllDocsQuery()
        q.facets = ArrayList()
        q.facets.add(FacetRequest('NAAM', 10))
        q.facets.add(FacetRequest('name.length', 10))

        response = luc.executeQuery(q)

        self.assertEqual(5, response.total)

        facets = {}
        for drill_d in response.drilldownData:
            drill_d = LuceneResponse.DrilldownData.cast_(drill_d)
            f_results = facets.setdefault(drill_d.fieldname, [])
            f_result = {'terms':{}, 'core': drill_d.core}
            f_results.append(f_result)
            for term in drill_d.terms:
                term = LuceneResponse.DrilldownData.Term.cast_(term)
                f_result['terms'][term.label] = term.count

        self.assertEqual({
            'NAAM': [{
                'core': 'core_name',
                'terms': {
                    'AAP': 1,
                    'NOOT': 1,
                    'MIES': 1,
                    'WIM': 1,
                    'ZUS': 1,
                }
            }],
            'name.length': [{
                'core': 'core_name',
                'terms': {
                    '3': 3,
                    '4': 2,
                }
            }]
            }, facets)


def populate_lhs(core):
    for i, name in enumerate(['aap', 'noot', 'mies', 'wim', 'zus']):
        document = Document()

        # key = numerator.numerateTerm(name)
        # document.add(NumericDocValuesField("lhs-id2", key))              # for Meresco's Join

        document.add(TextField('name', name, Field.Store.NO))
        document.add(FacetField('NAAM', name.upper()))
        document.add(FacetField('name.length', str(len(name))))
        core.addDocument(name, document)
    core.commit()
