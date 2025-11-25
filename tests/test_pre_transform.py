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
import errno
import logging
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig, BpsSubprocessError, ClusteredQuantumGraph
from lsst.ctrl.bps.pre_transform import cluster_quanta, create_quantum_graph, execute, update_quantum_graph
from lsst.pipe.base.tests.mocks import InMemoryRepo

TESTDIR = os.path.abspath(os.path.dirname(__file__))
_LOG = logging.getLogger(__name__)


class TestExecute(unittest.TestCase):
    """Test execution."""

    def setUp(self):
        self.file = tempfile.NamedTemporaryFile("w+")
        self.logger = logging.getLogger("lsst.ctrl.bps")

    def tearDown(self):
        self.file.close()

    def testSuccessfulExecution(self):
        """Test exit status if command succeeded."""
        content = "Successful execution"
        command = f"{sys.executable} -c 'print(\"{content}\")'"
        with self.assertLogs(logger=self.logger, level="INFO") as cm:
            status = execute(command, self.file.name)
        self.assertIn(content, cm.output[0])
        self.file.seek(0)
        file_contents = self.file.read()
        self.assertIn(command, file_contents)
        self.assertIn(content, file_contents)
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
        self.settings = {
            "createQuantumGraph": "touch {qgraphFile}",
            "submitPath": self.tmpdir,
            "whenSaveJobQgraph": "NEVER",
            "uniqProcName": "my_test",
            "qgraphFileTemplate": "{uniqProcName}.qg",
        }
        self.logger = logging.getLogger("lsst.ctrl.bps")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testSuccess(self):
        """Test if a new quantum graph was created successfully."""
        config = BpsConfig(self.settings, search_order=[])
        with self.assertLogs(logger=self.logger, level="INFO") as cm:
            qgraph_filename = create_quantum_graph(config, self.tmpdir)
        _, command = config.search("createQuantumGraph", opt={"curvals": {"qgraphFile": qgraph_filename}})
        self.assertIn(command, cm.output[0])
        self.assertTrue(os.path.exists(qgraph_filename))

    def testCommandMissing(self):
        """Test if error is caught when the command is missing."""
        del self.settings["createQuantumGraph"]
        config = BpsConfig(self.settings, search_order=[])
        with self.assertRaisesRegex(KeyError, "command.*not found"):
            create_quantum_graph(config, self.tmpdir)

    def testFailure(self):
        """Test if error is caught when the quantum graph creation fails."""
        self.settings["createQuantumGraph"] = "bash -c  'exit 2'"
        config = BpsConfig(self.settings, search_order=[])
        with self.assertRaises(BpsSubprocessError) as cm:
            create_quantum_graph(config, self.tmpdir)
        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.assertIn("non-zero exit code", str(cm.exception))


class TestUpdatingQuantumGraph(unittest.TestCase):
    """Test quantum graph update."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)
        self.settings = {
            "updateQuantumGraph": "bash -c 'echo foo > {qgraphFile}'",
            "submitPath": self.tmpdir,
            "whenSaveJobQgraph": "NEVER",
            "uniqProcName": "my_test",
            "qgraphFileTemplate": "{uniqProcName}.qg",
            "inputQgraphFile": f"{self.tmpdir}/src.qg",
        }
        self.logger = logging.getLogger("lsst.ctrl.bps")

        # Create a file in the temporary directory that will serve as
        # the file with a quantum graph that needs updating.
        self.src = Path(self.settings["inputQgraphFile"])
        self.src.write_text("foo\n")

        self.backup = Path(f"{self.src.parent}/{self.src.stem}_orig{self.src.suffix}")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testSuccess(self):
        """Test if the quantum graph was updated."""
        config = BpsConfig(self.settings, search_order=[])
        with self.assertLogs(logger=self.logger, level="INFO") as cm:
            update_quantum_graph(config, str(self.src), self.tmpdir)
        _, command = config.search("updateQuantumGraph", opt={"curvals": {"qgraphFile": str(self.src)}})
        self.assertIn("backing up", cm.output[0].lower())
        self.assertIn("completed", cm.output[1].lower())
        self.assertIn(command, cm.output[2])
        self.assertTrue(self.src.read_text(), "bar\n")
        self.assertTrue(self.backup.is_file())
        self.assertTrue(self.backup.read_text(), "foo\n")

    def testSuccessInPlace(self):
        """Test if a quantum graph was updated inplace."""
        config = BpsConfig(self.settings, search_order=[])
        with self.assertLogs(logger=self.logger, level="INFO") as cm:
            update_quantum_graph(config, str(self.src), self.tmpdir, inplace=True)
        _, command = config.search("updateQuantumGraph", opt={"curvals": {"qgraphFile": str(self.src)}})
        self.assertIn(command, cm.output[0])
        self.assertTrue(self.src.read_text(), "bar\n")
        self.assertFalse(self.backup.is_file())

    def testCommandMissing(self):
        """Test if error is caught when the command is missing."""
        del self.settings["updateQuantumGraph"]
        config = BpsConfig(self.settings, search_order=[])
        with self.assertRaisesRegex(KeyError, "command.*not found"):
            update_quantum_graph(config, str(self.src), self.tmpdir)

    def testFailure(self):
        """Test if error is caught when the command fails."""
        self.settings["updateQuantumGraph"] = "bash -c  'exit 2'"
        config = BpsConfig(self.settings, search_order=[])
        with self.assertRaises(BpsSubprocessError) as cm:
            update_quantum_graph(config, str(self.src), self.tmpdir)
        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.assertRegex(str(cm.exception), "non-zero exit code")


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
        with InMemoryRepo() as repo:
            qgraph = repo.make_quantum_graph()
        with self.assertRaisesRegex(RuntimeError, "Fake error"):
            _ = cluster_quanta(config, qgraph, "a_name")

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
        with InMemoryRepo() as repo:
            qgraph = repo.make_quantum_graph()
        _ = cluster_quanta(config, qgraph, "a_name")


if __name__ == "__main__":
    unittest.main()
