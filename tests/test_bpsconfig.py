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
import unittest

import yaml
from lsst.ctrl.bps import BpsConfig
from lsst.daf.butler.core.config import Config

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
        c = BpsConfig(self.dictionary)
        config = BpsConfig(c)
        self.assertIn("foo", config)

    def testInvalidArg(self):
        """Test if exception is raised for an argument of unsupported type."""
        sequence = ["wibble", "wobble", "wubble", "flob"]
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
        found, value = self.config.search("qux", opt={"curvals": {"qux": -3}})
        self.assertEqual(found, True)
        self.assertEqual(value, -3)

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
        """Test if a default value is properly set."""
        found, value = self.config.search("plugh", opt={"default": 4})
        self.assertEqual(found, True)
        self.assertEqual(value, 4)

    def testVariables(self):
        """Test combinations of expandEnvVars, replaceEnvVars,
        and replaceVars."""
        test_opt = {"expandEnvVars": False, "replaceEnvVars": False, "replaceVars": False}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "${GARPLY}/waldo/{qux:03}")

        test_opt = {"expandEnvVars": False, "replaceEnvVars": False, "replaceVars": True}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "${GARPLY}/waldo/002")

        test_opt = {"expandEnvVars": False, "replaceEnvVars": True, "replaceVars": False}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "<ENV:GARPLY>/waldo/{qux:03}")

        test_opt = {"expandEnvVars": False, "replaceEnvVars": True, "replaceVars": True}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "<ENV:GARPLY>/waldo/002")

        test_opt = {"expandEnvVars": True, "replaceEnvVars": False, "replaceVars": False}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "garply/waldo/{qux:03}")

        test_opt = {"expandEnvVars": True, "replaceEnvVars": False, "replaceVars": True}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "garply/waldo/002")

        test_opt = {"expandEnvVars": True, "replaceEnvVars": True, "replaceVars": False}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "garply/waldo/{qux:03}")

        test_opt = {"expandEnvVars": True, "replaceEnvVars": True, "replaceVars": True}
        found, value = self.config.search("grault", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(found, True)
        self.assertEqual(value, "garply/waldo/002")

    def testRequired(self):
        """Test if exception is raised if a required setting is missing."""
        with self.assertRaises(KeyError):
            self.config.search("fred", opt={"required": True})


if __name__ == "__main__":
    unittest.main()
