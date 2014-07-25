from seecr.test import SeecrTestCase

from meresco.lucene import Clusters
from meresco.lucene.luceneresponse import LuceneResponse
from meresco.lucene.hit import Hit


class ClustersTest(SeecrTestCase):
    def testClusters(self):
        clusters = Clusters()
        total = 999
        hits = [
            Hit('urn:1', local={'fieldTermSets': {'termvector.field': set(['value0', 'value1'])}}),
            Hit('urn:2', local={'fieldTermSets': {'termvector.field': set(['value0', 'value1', 'value2'])}}),
            Hit('urn:3', local={'fieldTermSets': {'termvector.field': set(['other'])}}),
            Hit('urn:4', local={'fieldTermSets': {'termvector.field': set(['other'])}}),
            Hit('urn:5', local={'fieldTermSets': {'field': set(['other'])}})
        ]
        luceneResponse = LuceneResponse(total=total, hits=hits, drilldownData=[])
        clusters.clusterResponse(luceneResponse)
        self.assertClusteringEquals([
                {'label': None, 'hits': [Hit('urn:1'), Hit('urn:2')]},
                {'label': None, 'hits': [Hit('urn:3'), Hit('urn:4')]},
                {'label': None, 'hits': [Hit('urn:5')]},
            ], luceneResponse.clusters)

    def assertClusteringEquals(self, expected, clustering):
        self.assertEquals(len(expected), len(clustering), clustering)
        for i, cluster in enumerate(expected):
            self.assertEquals(cluster['label'], clustering[i]['label'])
            self.assertEquals([hit.id for hit in cluster['hits']], [hit.id for hit in clustering[i]['hits']])
