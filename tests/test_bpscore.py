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
