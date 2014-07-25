from meresco.core import Observable


class Clusters(Observable):
    def clusterResponse(self, response):
        clusters = []
        for hit in response.hits:
            self._addToClusters(clusters, hit)
        if clusters:
            response.clusters = [{'hits': hits, 'label': None} for _, hits in clusters]

    def _addToClusters(self, clusters, hit):
        def aggregateDistance(d):
            # return sqrt(sum(distance ** 2 for distance in d.values()) / len(d))
            return max(d.values())

        THRESHOLD = 0.3

        clusterFound = False
        fieldTermSets = hit.local['fieldTermSets']
        for cluster in clusters:
            clusterFieldTerms, hits = cluster
            fieldDistances = {}
            for field, terms in fieldTermSets.items():
                fieldDistances[field] = overlap(terms, clusterFieldTerms.get(field, []))
            if fieldDistances and aggregateDistance(fieldDistances) > THRESHOLD:
                hits.append(hit)  # adapt centroid (?)
                clusterFound = True

        if not clusterFound:
            clusters.append((fieldTermSets, [hit]))


def overlap(s1, s2):
    intersectionSize = len(s1.intersection(s2))
    unionSize = len(s1.union(s2))
    if unionSize == intersectionSize:
        return 1.0
    if intersectionSize == 0:
        return 0.0
    return intersectionSize / float(len(s1.union(s2)))



