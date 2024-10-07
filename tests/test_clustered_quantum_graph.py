# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
import shutil
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from cqg_test_utils import make_test_clustered_quantum_graph
from lsst.ctrl.bps import ClusteredQuantumGraph, QuantaCluster
from qg_test_utils import make_test_quantum_graph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestQuantaCluster(unittest.TestCase):
    """Tests for clustering."""

    def setUp(self):
        self.qgraph = make_test_quantum_graph()
        nodes = list(self.qgraph.getNodesForTask(self.qgraph.findTaskDefByLabel("T1")))
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
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testName(self):
        self.assertEqual(self.cqg1.name, "cqg1")

    def testQgraph(self):
        """Test qgraph method."""
        self.assertEqual(self.cqg1.qgraph, self.qgraph)

    def testGetClusterExists(self):
        """Test get_cluster method where cluster exists."""
        self.assertEqual("T1_1_2", self.cqg1.get_cluster("T1_1_2").name)

    def testGetClusterMissing(self):
        """Test get_cluster method where cluster doesn't exist."""
        with self.assertRaises(KeyError):
            _ = self.cqg1.get_cluster("Not_There")

    def testClusters(self):
        """Test clusters method returns in correct order."""
        retval = list(self.cqg1.clusters())

        # Save min and max locations of a label in retval for later comparison.
        label_to_index = {}
        for index, cluster in enumerate(retval):
            minmax = label_to_index.setdefault(cluster.label, (len(retval) + 1, -1))
            label_to_index[cluster.label] = (min(minmax[0], index), max(minmax[1], index))

        # assert see all of T1 before see any of clusterT2T3
        self.assertLess(label_to_index["T1"][1], label_to_index["clusterT2T3"][0])

    def testSuccessorsExisting(self):
        """Test successors method returns existing successors."""
        self.assertEqual(list(self.cqg1.successors("T1_1_2")), ["T23_1_2"])

    def testSuccessorsNone(self):
        """Test successors method handles no successors."""
        # check iterable and empty
        self.assertEqual(len(list(self.cqg1.successors("T4_1_2"))), 0)

    def testPredecessorsExisting(self):
        """Test predecessors method returns existing predecessors."""
        self.assertEqual(list(self.cqg1.predecessors("T23_1_2")), ["T1_1_2"])

    def testPredecessorsNone(self):
        """Test predecessors method handles no predecessors."""
        # check iterable and empty
        self.assertEqual(len(list(self.cqg1.predecessors("T1_1_2"))), 0)

    def testSaveAndLoad(self):
        path = Path(f"{self.tmpdir}/save_1.pickle")
        self.cqg1.save(path)
        self.assertTrue(path.is_file() and path.stat().st_size)
        test_cqg = ClusteredQuantumGraph.load(path)
        self.assertEqual(self.cqg1, test_cqg)

    def testValidateOK(self):
        # Test nothing raised on valid clustered quantum graph
        self.cqg1.validate()

    def testValidateNotDAG(self):
        # Add bad edge to make not a DAG
        qc1 = self.cqg1.get_cluster("T1_1_2")
        qc2 = self.cqg1.get_cluster("T23_1_2")
        self.cqg1.add_dependency(qc2, qc1)

        with self.assertRaises(RuntimeError) as cm:
            self.cqg1.validate()
            self.assertIn("is not a directed acyclic graph", str(cm))

    def testValidateMissingQuanta(self):
        # Remove Quanta from cluster
        qc2 = self.cqg1.get_cluster("T23_1_2")
        qc2._qgraph_node_ids = qc2._qgraph_node_ids[:-1]

        with self.assertRaises(RuntimeError) as cm:
            self.cqg1.validate()
            self.assertIn("does not equal number in quantum graph", str(cm))

    def testValidateDuplicateId(self):
        # Add new Quanta with duplicate Quantum
        qc1 = self.cqg1.get_cluster("T1_1_2")
        qnode = self.cqg1.get_quantum_node(next(iter(qc1.qgraph_node_ids)))
        qc = QuantaCluster.from_quantum_node(qnode, "DuplicateId")
        self.cqg1.add_cluster(qc)
        qc2 = self.cqg1.get_cluster("T23_1_2")
        self.cqg1.add_dependency(qc2, qc)

        with self.assertRaises(RuntimeError) as cm:
            self.cqg1.validate()
            self.assertIn("occurs in at least 2 clusters", str(cm))


if __name__ == "__main__":
    unittest.main()
