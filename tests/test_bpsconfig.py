import os
import unittest
import yaml
from lsst.daf.butler.core.config import Config
from lsst.ctrl.bps.bps_config import BpsConfig


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestBpsConfigConstructor(unittest.TestCase):

    def setUp(self):
        self.filename = os.path.join(TESTDIR, "data/config.yaml")
        with open(self.filename, "r") as f:
            self.dictionary = yaml.safe_load(f)

    def tearDown(self):
        pass

    def testFromFilename(self):
        """Test initialization from a file."""
        config = BpsConfig(self.filename)
        self.assertIn("foo", config)

    def testFromDict(self):
        """Test initialization from a dictionary."""
        config = BpsConfig(self.dictionary)
        self.assertIn("bar", config)

    def testFromConfig(self):
        """Test initialization from other Config object."""
        c = Config(self.dictionary)
        config = BpsConfig(c)
        self.assertIn("baz", config)

    def testFromBpsConfig(self):
        """Test initialization from other BpsConfig object."""
        pass

    def testInvalidArg(self):
        """Test if exception is raised for an argument of unsupported type."""
        sequence = ["foo", "bar", "baz"]
        with self.assertRaises(RuntimeError):
            BpsConfig(sequence)


class TestBpsConfigSearch(unittest.TestCase):

    def setUp(self):
        filename = os.path.join(TESTDIR, "data/config.yaml")
        self.config = BpsConfig(filename, search_order=["baz", "bar", "foo"])
        os.environ["GARPLY"] = "garply"

    def tearDown(self):
        del os.environ["GARPLY"]

    def testSectionSearchOrder(self):
        """Test if sections are searched in the prescribed order."""
        key = "qux"
        found, value = self.config.search(key)
        self.assertEqual(found, True)
        self.assertEqual(value, 2)

    def testCurrentValues(self):
        """Test if a current value overrides of the one in configuration."""
        found, value = self.config.search("qux", opt={"curvals": {"qux": 3}})
        self.assertEqual(found, True)
        self.assertEqual(value, 3)

    def testSearchobjValues(self):
        """Test if a serachobj value overrides of the one in configuration."""
        options = {"searchobj": {"qux": 4}}
        found, value = self.config.search("qux", opt=options)
        self.assertEqual(found, True)
        self.assertEqual(value, 4)

    def testSubsectionSearch(self):
        options = {"curvals": {"curr_baz": "garply"}}
        found, value = self.config.search("qux", opt=options)
        self.assertEqual(found, True)
        self.assertEqual(value, 3)

    def testDefault(self):
        """Test if a default value are properly set."""
        found, value = self.config.search("quux", opt={"default": 4})
        self.assertEqual(found, True)
        self.assertEqual(value, 4)

    def testReplaceOn(self):
        """Test if environmental variables are replaced, if requested."""
        found, value = self.config.search("grault", opt={"replaceVars": True})
        self.assertEqual(found, True)
        self.assertEqual(value, "garply/waldo")

    def testReplaceOff(self):
        """Test if environmental variables are not replaced, if requested."""
        found, value = self.config.search("grault", opt={"replaceVars": False})
        self.assertEqual(found, True)
        self.assertEqual(value, "${GARPLY}/waldo")

    def testRequired(self):
        """Test if exception is raised if a required setting is missing."""
        with self.assertRaises(KeyError):
            self.config.search("fred", opt={"required": True})


if __name__ == "__main__":
    unittest.main()
