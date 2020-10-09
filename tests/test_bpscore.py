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
from random import choice
from string import ascii_letters
import lsst.ctrl.bps.bps_core as bps


class TestExecute(unittest.TestCase):

    def setUp(self):
        self.file = tempfile.NamedTemporaryFile("w+")

    def tearDown(self):
        self.file.close()

    def testSuccessfulExecution(self):
        """Test exit status if command succeeded."""
        status = bps.execute('true', self.file.name)
        self.assertIn('true', self.file.read())
        self.assertEqual(status, 0)

    def testFailingExecution(self):
        """Test exit status if command failed."""
        status = bps.execute('false', self.file.name)
        self.assertIn('false', self.file.read())
        self.assertNotEqual(status, 0)


class TestPrettyPrinting(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testTaskLabelFormatting(self):
        """Test if a task label is properly formatted."""
        original = "'raw+{band: i, instrument: HSC, detector: 17}'"
        expected = "'raw\nband=i\n instrument=HSC\n detector=17'"
        result = bps.pretty_dataset_label(original)
        self.assertEqual(result, expected)


class TestBpsCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testCreatingQuantumGraph(self):
        """Test if an appropriately named pickle file created."""
        directory = tempfile.mkdtemp(prefix="bps")
        filename = "".join(choice(ascii_letters) for _ in range(8))
        path = os.path.join(directory, f"{filename}.pickle")
        settings = {
            "global": {
                "createQuantumGraph": "touch {qgraphfile}",
                "submitPath": directory,
                "uniqProcName": filename,
            }
        }
        config = bps.BpsConfig(settings, search_order=["global"])
        core = bps.BpsCore(config)
        core._create_quantum_graph()
        self.assertTrue(os.path.exists(path))
        if os.path.exists(directory):
            shutil.rmtree(directory)

    def testCreatingQuantumGraphFailure(self):
        """Test if an exception is raised when creating quantum graph fails."""
        directory = tempfile.mkdtemp(prefix="bps")
        filename = "".join(choice(ascii_letters) for _ in range(8))
        settings = {
            "global": {
                "createQuantumGraph": "false",
                "submitPath": directory,
                "uniqProcName": filename,
            }
        }
        config = bps.BpsConfig(settings, search_order=["global"])
        core = bps.BpsCore(config)
        with self.assertRaises(RuntimeError):
            core._create_quantum_graph()
        if os.path.exists(directory):
            shutil.rmtree(directory)


if __name__ == "__main__":
    unittest.main()
