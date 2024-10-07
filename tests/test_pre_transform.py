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
import os
import shutil
import tempfile
import unittest

from lsst.ctrl.bps import BpsConfig, ClusteredQuantumGraph
from lsst.ctrl.bps.pre_transform import cluster_quanta, create_quantum_graph, execute
from lsst.daf.butler import DimensionUniverse
from lsst.pipe.base import QuantumGraph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestExecute(unittest.TestCase):
    """Test execution."""

    def setUp(self):
        self.file = tempfile.NamedTemporaryFile("w+")

    def tearDown(self):
        self.file.close()

    def testSuccessfulExecution(self):
        """Test exit status if command succeeded."""
        status = execute("true", self.file.name)
        self.assertIn("true", self.file.read())
        self.assertEqual(status, 0)

    def testFailingExecution(self):
        """Test exit status if command failed."""
        status = execute("false", self.file.name)
        self.assertIn("false", self.file.read())
        self.assertNotEqual(status, 0)


class TestCreatingQuantumGraph(unittest.TestCase):
    """Test quantum graph creation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testCreatingQuantumGraph(self):
        """Test if a config command creates appropriately named qgraph file."""
        settings = {
            "createQuantumGraph": "touch {qgraphFile}",
            "submitPath": self.tmpdir,
            "whenSaveJobQgraph": "NEVER",
            "uniqProcName": "my_test",
            "qgraphFileTemplate": "{uniqProcName}.qgraph",
        }
        config = BpsConfig(settings, search_order=[])
        create_quantum_graph(config, self.tmpdir)
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, config["qgraphFileTemplate"])))

    def testCreatingQuantumGraphFailure(self):
        """Test if an exception is raised when creating qgraph file fails."""
        settings = {
            "createQuantumGraph": "false",
            "submitPath": self.tmpdir,
        }
        config = BpsConfig(settings, search_order=[])
        with self.assertRaises(RuntimeError):
            create_quantum_graph(config, self.tmpdir)


class TestClusterQuanta(unittest.TestCase):
    """Test cluster_quanta method.  Other tests cover functions
    cluster_quanta calls so mocking them here.
    """

    @unittest.mock.patch.object(ClusteredQuantumGraph, "validate")
    def testValidate(self, mock_validate):
        """Test that actually calls validate per config."""
        mock_validate.side_effect = RuntimeError("Fake error")
        settings = {
            "clusterAlgorithm": "lsst.ctrl.bps.quantum_clustering_funcs.single_quantum_clustering",
            "uniqProcName": "my_test",
            "validateClusteredQgraph": True,
        }
        config = BpsConfig(settings, search_order=[])
        qgraph = QuantumGraph({}, universe=DimensionUniverse())
        with self.assertRaises(RuntimeError) as cm:
            _ = cluster_quanta(config, qgraph, "a_name")
            self.assertIn("Fake error", str(cm))

    @unittest.mock.patch.object(ClusteredQuantumGraph, "validate")
    def testNoValidate(self, mock_validate):
        """Test that doesn't call validate per config."""
        mock_validate.side_effect = RuntimeError("Fake error")
        settings = {
            "clusterAlgorithm": "lsst.ctrl.bps.quantum_clustering_funcs.single_quantum_clustering",
            "uniqProcName": "my_test",
            "validateClusteredQgraph": False,
        }
        config = BpsConfig(settings, search_order=[])
        qgraph = QuantumGraph({}, universe=DimensionUniverse())
        _ = cluster_quanta(config, qgraph, "a_name")


if __name__ == "__main__":
    unittest.main()
