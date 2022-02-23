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
import os
import shutil
import tempfile
import unittest

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.pre_transform import create_quantum_graph, execute

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestExecute(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
