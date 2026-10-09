"""Vérifications sur des graphes dont on connaît la réponse (bibliothèque standard unittest)."""
import random
import shutil
import tempfile
import unittest
from pathlib import Path

import social_graph as sg


def reference_pagerank(ids, links, iterations, reset=0.15):
    """Récurrence séquentielle indépendante, en Python pur (sans redistribution des puits)."""
    out = {i: sum(1 for s, _ in links if s == i) for i in ids}
    ranks = {i: 1.0 for i in ids}
    for _ in range(iterations):
        ranks = {i: reset + (1 - reset) * sum(ranks[s] / out[s] for s, d in links if d == i) for i in ids}
    total = sum(ranks.values())
    return {i: r / total for i, r in ranks.items()}


def reference_components(ids, links):
    """Union-find : plus petit identifiant de chaque composante, sens ignoré."""
    parent = {i: i for i in ids}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for s, d in links:
        a, b = find(s), find(d)
        parent[max(a, b)] = min(a, b)
    return {i: find(i) for i in ids}


class SocialGraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = sg.create_spark("PySpark Social Graph tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def graph(self, names, links):
        vertices = self.spark.createDataFrame(list(names.items()), sg.USERS_SCHEMA)
        edges = self.spark.createDataFrame(links, sg.LINKS_SCHEMA)
        return vertices, edges

    def run_metrics(self, names, links, iterations=20):
        vertices, edges = self.graph(names, links)
        return {r["id"]: r for r in sg.metrics(vertices, edges, iterations).collect()}

    def test_degrees_components_isolated_and_pagerank(self):
        links = [(1, 2), (2, 1), (2, 3)]
        result = self.run_metrics({1: "A", 2: "B", 3: "C", 4: "Isolé"}, links)
        self.assertEqual([result[i]["followers"] for i in (1, 2, 3, 4)], [1, 1, 1, 0])
        self.assertEqual([result[i]["following"] for i in (1, 2, 3, 4)], [1, 2, 0, 0])
        self.assertEqual({result[i]["component"] for i in (1, 2, 3)}, {1})
        self.assertEqual(result[4]["component"], 4)
        expected = reference_pagerank([1, 2, 3, 4], links, 20)
        for i in expected:
            self.assertAlmostEqual(result[i]["pagerank"], expected[i], delta=1e-10)
        self.assertAlmostEqual(sum(r["pagerank"] for r in result.values()), 1.0, delta=1e-10)

    def test_symmetric_cycle(self):
        result = self.run_metrics({1: "A", 2: "B", 3: "C"}, [(1, 2), (2, 3), (3, 1)], 10)
        for row in result.values():
            self.assertAlmostEqual(row["pagerank"], 1 / 3, delta=1e-10)

    def test_random_graph_against_references(self):
        rng = random.Random(7)
        ids = list(range(1, 61))
        links = sorted({(rng.choice(ids[:50]), rng.choice(ids[:50])) for _ in range(90)} - {(i, i) for i in ids})
        result = self.run_metrics({i: f"U{i}" for i in ids}, links, 15)
        pr = reference_pagerank(ids, links, 15)
        cc = reference_components(ids, links)
        for i in ids:
            self.assertAlmostEqual(result[i]["pagerank"], pr[i], delta=1e-10)
            self.assertEqual(result[i]["component"], cc[i])

    def test_load_cleans_and_validates(self):
        folder = Path(tempfile.mkdtemp(prefix="graph-checks-"))
        try:
            (folder / "users.csv").write_text("id,name\n1,A\n2,B\n3,Isolé\n", encoding="utf-8")
            (folder / "follows.csv").write_text("src,dst\n1,2\n1,2\n1,1\n", encoding="utf-8")
            vertices, edges = sg.load(self.spark, str(folder))
            self.assertEqual((vertices.count(), edges.count()), (3, 1))
            (folder / "follows.csv").write_text("src,dst\n1,99\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                sg.load(self.spark, str(folder))
            (folder / "users.csv").write_text("id,name\n1,A\n1,B\n", encoding="utf-8")
            (folder / "follows.csv").write_text("src,dst\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                sg.load(self.spark, str(folder))
        finally:
            shutil.rmtree(folder)


if __name__ == "__main__":
    unittest.main()
