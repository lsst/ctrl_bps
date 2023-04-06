# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Unit tests for the clustering methods.
"""

# Turn off "doesn't conform to snake_case naming style" because matching
# the unittest casing.
# pylint: disable=invalid-name

import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from cqg_test_utils import make_test_clustered_quantum_graph
from lsst.ctrl.bps import ClusteredQuantumGraph, QuantaCluster
from qg_test_utils import make_test_quantum_graph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestQuantaCluster(unittest.TestCase):
    def setUp(self):
        self.qgraph = make_test_quantum_graph()
        nodes = [n for n in self.qgraph.getNodesForTask(self.qgraph.findTaskDefByLabel("T1"))]
        self.qnode1 = nodes[0]
        self.qnode2 = nodes[1]

    def tearDown(self):
        pass

    def testQgraphNodeIds(self):
        qc = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        self.assertEqual(qc.qgraph_node_ids, frozenset([self.qnode1.nodeId]))

    def testQuantaCountsNone(self):
        qc = QuantaCluster("NoQuanta", "the_label")
        self.assertEqual(qc.quanta_counts, Counter())

    def testQuantaCounts(self):
        qc = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        self.assertEqual(qc.quanta_counts, Counter({"T1": 1}))

    def testAddQuantumNode(self):
        qc = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        qc.add_quantum_node(self.qnode2)
        self.assertEqual(qc.quanta_counts, Counter({"T1": 2}))

    def testAddQuantum(self):
        qc = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        qc.add_quantum(self.qnode2.quantum, self.qnode2.taskDef.label)
        self.assertEqual(qc.quanta_counts, Counter({"T1": 2}))

    def testStr(self):
        qc = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        self.assertIn(qc.name, str(qc))
        self.assertIn("T1", str(qc))
        self.assertIn("tags", str(qc))

    def testEqual(self):
        qc1 = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        qc2 = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        self.assertEqual(qc1, qc2)

    def testNotEqual(self):
        qc1 = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        qc2 = QuantaCluster.from_quantum_node(self.qnode2, "{node_number}")
        self.assertNotEqual(qc1, qc2)

    def testHash(self):
        qc1 = QuantaCluster.from_quantum_node(self.qnode1, "{node_number}")
        qc2 = QuantaCluster.from_quantum_node(self.qnode2, "{node_number}")
        self.assertNotEqual(hash(qc1), hash(qc2))


class TestClusteredQuantumGraph(unittest.TestCase):
    """Tests for single_quantum_clustering method."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.qgraph, self.cqg1 = make_test_clustered_quantum_graph(self.tmpdir)

    def tearDown(self):
        pass
        # if self.tmpdir is not None and os.path.exists(self.tmpdir):
        #    shutil.rmtree(self.tmppath, ignore_errors=True)

    def testName(self):
        self.assertEqual(self.cqg1.name, "cqg1")

    def testQgraph(self):
        """Test qgraph method"""
        self.assertEqual(self.cqg1.qgraph, self.qgraph)

    # def testAddClusterSingle(self):
    #    """Test add_cluster method for single new cluster."""

    def testGetClusterExists(self):
        """Test get_cluster method where cluster exists."""
        self.assertEqual("T1_1_2", self.cqg1.get_cluster("T1_1_2").name)

    def testGetClusterMissing(self):
        """Test get_cluster method where cluster doesn't exist."""
        with self.assertRaises(KeyError):
            _ = self.cqg1.get_cluster("Not_There")

    # def testGetQuantumNodeExists(self):
    #     """Test get_quantum_node method where node exists."""
    #
    # def testGetQuantumNodeMissing(self):
    #     """Test get_quantum_node method where node doesn't exist."""

    def testClusters(self):
        """Test clusters method returns in correct order"""

    def testSuccessorsExisting(self):
        """Test successors method returns existing successors."""
        self.assertEqual([x for x in self.cqg1.successors("T1_1_2")], ["T23_1_2"])

    def testSuccessorsNone(self):
        """Test successors method handles no successors."""
        # check iterable and empty
        self.assertEqual(len([x for x in self.cqg1.successors("T4_1_2")]), 0)

    def testPredecessorsExisting(self):
        """Test predecessors method returns existing predecessors."""
        self.assertEqual([x for x in self.cqg1.predecessors("T23_1_2")], ["T1_1_2"])

    def testPredecessorsNone(self):
        """Test predecessors method handles no predecessors."""
        # check iterable and empty
        self.assertEqual(len([x for x in self.cqg1.predecessors("T1_1_2")]), 0)

    # def testAddDependency(self):

    def testSaveAndLoad(self):
        path = Path(f"{self.tmpdir}/save_1.pickle")
        self.cqg1.save(path)
        self.assertTrue(path.is_file() and path.stat().st_size)
        test_cqg = ClusteredQuantumGraph.load(path)
        self.assertEqual(self.cqg1, test_cqg)


if __name__ == "__main__":
    unittest.main()
