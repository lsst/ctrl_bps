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
import unittest

import yaml

from lsst.ctrl.bps import BPS_SEARCH_ORDER, BpsConfig
from lsst.daf.butler import Config

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestBpsConfigConstructor(unittest.TestCase):
    """Test BpsConfig construction."""

    def setUp(self):
        self.filename = os.path.join(TESTDIR, "data/config.yaml")
        with open(self.filename) as f:
            self.dictionary = yaml.safe_load(f)

    def tearDown(self):
        pass

    def testFromFilename(self):
        """Test initialization from a file, but without defaults."""
        config = BpsConfig(self.filename)
        self.assertEqual(set(config), {"foo", "bar", "baz"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(set(config["foo"]), {"qux"})
        self.assertEqual(set(config["bar"]), {"qux"})
        self.assertEqual(set(config["baz"]), {"grault", "garply"})
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testFromDict(self):
        """Test initialization from a dictionary."""
        config = BpsConfig(self.dictionary)
        self.assertEqual(set(config), {"foo", "bar", "baz"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(set(config["foo"]), {"qux"})
        self.assertEqual(set(config["bar"]), {"qux"})
        self.assertEqual(set(config["baz"]), {"grault", "garply"})
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testFromConfig(self):
        """Test initialization from other Config object."""
        config_daf = Config(self.dictionary)
        config = BpsConfig(config_daf)
        self.assertEqual(set(config), {"foo", "bar", "baz"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(set(config["foo"]), {"qux"})
        self.assertEqual(set(config["bar"]), {"qux"})
        self.assertEqual(set(config["baz"]), {"grault", "garply"})
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testFromBpsConfig(self):
        """Test initialization from other BpsConfig object."""
        config_ref = BpsConfig(self.dictionary)
        config = BpsConfig(config_ref)
        self.assertEqual(set(config), set(config_ref))
        self.assertEqual(config.search_order, config_ref.search_order)

    def testDefaultsInclusion(self):
        """Test including defaults."""
        config = BpsConfig(self.filename, defaults={"quux": 1})
        self.assertEqual(set(config), {"foo", "bar", "baz", "quux"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["quux"], 1)
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testDefaultsOverride(self):
        """Test overriding defaults."""
        config = BpsConfig(self.filename, defaults={"foo": {"qux": 2}, "quux": 1})
        self.assertEqual(set(config), {"foo", "bar", "baz", "quux"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["quux"], 1)
        self.assertEqual(config["foo"], BpsConfig({"qux": 1}, search_order=[]))
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testWmsFromCmdline(self):
        """Test initialization when WMS class specified at the cmdline."""
        with unittest.mock.patch.dict(
            os.environ, {"BPS_WMS_SERVICE_CLASS": "wms_test_utils.WmsServiceFromEnv"}
        ):
            filename = os.path.join(TESTDIR, "data/config_with_wms.yaml")
            config = BpsConfig(
                filename,
                defaults={"wmsServiceClass": "wms_test_utils.WmsServiceFromDefaults"},
                wms_service_class_fqn="wms_test_utils.WmsServiceFromCmdline",
            )
        self.assertEqual(set(config), {"corge", "wmsServiceClass"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["corge"], "cmdline")

    def testWmsFromConfig(self):
        """Test initialization when WMS class specified in the config."""
        with unittest.mock.patch.dict(
            os.environ, {"BPS_WMS_SERVICE_CLASS": "wms_test_utils.WmsServiceFromEnv"}
        ):
            filename = os.path.join(TESTDIR, "data/config_with_wms.yaml")
            config = BpsConfig(
                filename,
                defaults={"wmsServiceClass": "wms_test_utils.WmsServiceFromDefaults"},
                wms_service_class_fqn=None,
            )
        self.assertEqual(set(config), {"corge", "wmsServiceClass"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["corge"], "config")
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testWmsFromEnv(self):
        """Test initialization with WMS class in the env."""
        with unittest.mock.patch.dict(
            os.environ, {"BPS_WMS_SERVICE_CLASS": "wms_test_utils.WmsServiceFromEnv"}
        ):
            config = BpsConfig(
                {},
                defaults={"wmsServiceClass": "wms_test_utils.WmsServiceFromDefaults"},
                wms_service_class_fqn=None,
            )
        self.assertEqual(set(config), {"corge", "wmsServiceClass"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["corge"], "env")
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testWmsFromDefaults(self):
        """Test initialization  with a default WMS class."""
        config = BpsConfig({}, defaults={"wmsServiceClass": "wms_test_utils.WmsServiceFromDefaults"})
        self.assertEqual(set(config), {"corge", "wmsServiceClass"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(config["corge"], "defaults")
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testDefaultsExclusion(self):
        config = BpsConfig(
            self.filename, defaults=None, wms_service_class_fqn="wms_test_utils.WmsServiceFromCmdline"
        )
        self.assertEqual(set(config), {"foo", "bar", "baz"} | set(BPS_SEARCH_ORDER))
        self.assertEqual(set(config["foo"]), {"qux"})
        self.assertEqual(set(config["bar"]), {"qux"})
        self.assertEqual(set(config["baz"]), {"grault", "garply"})
        self.assertEqual(config.search_order, BPS_SEARCH_ORDER)

    def testInvalidArg(self):
        """Test if exception is raised for an argument of unsupported type."""
        sequence = ["wibble", "wobble", "wubble", "flob"]
        with self.assertRaises(ValueError):
            BpsConfig(sequence)


class TestBpsConfigGet(unittest.TestCase):
    """Test retrieval of items from BpsConfig."""

    def setUp(self):
        self.config = BpsConfig({"foo": "bar"})

    def tearDown(self):
        pass

    def testKeyExistsNoDefault(self):
        """Test if the value is returned when the key is in the dictionary."""
        self.assertEqual("bar", self.config.get("foo"))

    def testKeyExistsDefaultProvided(self):
        """Test if the value is returned when the key is in the dictionary."""
        self.assertEqual("bar", self.config.get("foo", "qux"))

    def testKeyMissingNoDefault(self):
        """Test if the provided default is returned if the key is missing."""
        self.assertEqual("", self.config.get("baz"))

    def testKeyMissingDefaultProvided(self):
        """Test if the custom default is returned if the key is missing."""
        self.assertEqual("qux", self.config.get("baz", "qux"))


class TestBpsConfigSearch(unittest.TestCase):
    """Test searching of BpsConfig."""

    def setUp(self):
        filename = os.path.join(TESTDIR, "data/config.yaml")
        self.config = BpsConfig(filename, search_order=["baz", "bar", "foo"], defaults={})
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

    def testReplaceVars(self):
        """Test replaceVars method."""
        test_opt = {"default": 555}
        orig_str = "<ENV:GARPLY>/waldo/{qux:03}/{notthere}"
        value = self.config.replace_vars(orig_str, opt=test_opt)
        self.assertEqual(value, "<ENV:GARPLY>/waldo/002/")
        self.assertEqual(test_opt["default"], 555)

    def testReplaceVarsSkipNames(self):
        test_opt = {"default": 555, "skipNames": ["qgraphFile", "butlerConfig"]}
        orig_str = "<ENV:GARPLY>/waldo/{qux:03} {qgraphFile} {butlerConfig}"
        value = self.config.replace_vars(orig_str, opt=test_opt)
        self.assertEqual(value, "<ENV:GARPLY>/waldo/002 {qgraphFile} {butlerConfig}")
        self.assertEqual(test_opt["default"], 555)

    def testVariables(self):
        """Test combinations of expandEnvVars, replaceEnvVars,
        and replaceVars.
        """
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
        self.assertEqual(value, "garply/waldo/002")

    def testRequired(self):
        """Test if exception is raised if a required setting is missing."""
        with self.assertRaises(KeyError):
            self.config.search("fred", opt={"required": True})


class TestBpsConfigGenerateConfig(unittest.TestCase):
    """Test BpsConfig.generate_config and bpsEval methods."""

    def setUp(self):
        # Just to shorten string length in tests
        self.test_prefix = "lsst.ctrl.bps.tests.config_test_utils"

        filename = os.path.join(TESTDIR, "data/initialize_config.yaml")
        self.config = BpsConfig(filename, BPS_SEARCH_ORDER, defaults={})

    def testUnparsableValue(self):
        config = BpsConfig(
            {
                "p1": 3,
                "p3": 16,
            },
            BPS_SEARCH_ORDER,
        )
        # invalid function name
        config["bpsGenerateConfig"] = "not a valid name({p1}, param3={p3})"
        with self.assertRaisesRegex(ValueError, "Unparsable bpsGenerateConfig value='not a valid"):
            config.generate_config()

    def testMissingParen(self):
        config = BpsConfig(
            {
                "p1": 3,
                "p3": 16,
            },
            BPS_SEARCH_ORDER,
        )
        # invalid function name
        config["bpsGenerateConfig"] = self.test_prefix + ".generate_config_1(1, param3=2"
        with self.assertRaisesRegex(ValueError, "Unparsable bpsGenerateConfig value='"):
            config.generate_config()

    def testBadFunctionName(self):
        config = BpsConfig(
            {
                "p1": 3,
                "p3": 16,
            },
            BPS_SEARCH_ORDER,
        )
        # invalid function name
        config["bpsGenerateConfig"] = self.test_prefix + ".notthere({p1}, param3={p3})"
        with self.assertRaisesRegex(ImportError, "notthere"):
            config.generate_config()

    def testBadModuleName(self):
        config = BpsConfig(
            {
                "p1": 3,
                "p3": 16,
            },
            BPS_SEARCH_ORDER,
        )
        # invalid module name
        config["bpsGenerateConfig"] = "lsst.ctrl.bps.notthere.generate_config(1, 2)"
        with self.assertRaisesRegex(ImportError, "notthere"):
            config.generate_config()

    def testBadParamName(self):
        config = BpsConfig(
            {"p1": 3, "p3": 16, "bpsGenerateConfig": self.test_prefix + ".generate_config_1(1, param5=2)"},
            BPS_SEARCH_ORDER,
        )
        with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'param5'"):
            config.generate_config()

    def testExtraParam(self):
        config = BpsConfig(
            {
                "p1": 3,
                "p3": 16,
                "bpsGenerateConfig": self.test_prefix + ".generate_config_1({p1}, 2, {p3}, 4)",
            },
            BPS_SEARCH_ORDER,
        )
        with self.assertRaisesRegex(TypeError, "positional arguments"):
            config.generate_config()

    def testWithSearchOrder(self):
        # Check that bpsGenerateConfig is replaced in search sections
        #     (e.g., pipetask) And when replacing vars in subsections
        #     config ordering is used.
        # Ditto for finalJob (which isn't a search section).
        # Checking all in single function to ensure doesn't quit early.
        self.config.generate_config()

        filename = os.path.join(TESTDIR, "data/initialize_config_truth.yaml")
        truth = BpsConfig(filename, BPS_SEARCH_ORDER, defaults={})

        self.assertEqual(self.config, truth)

    def testBpsEval(self):
        """Test replacing bpsEval when need to import module."""
        test_opt = {
            "expandEnvVars": True,
            "replaceEnvVars": True,
            "replaceVars": True,
            "curvals": {"curr_pipetask": "ptask1"},
        }
        found, value = self.config.search("genval1", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "-c val1:0.1 -c val2:3")

    def testBpsEvalBuiltin(self):
        """Test replacing bpsEval with builtin function."""
        test_opt = {
            "expandEnvVars": True,
            "replaceEnvVars": True,
            "replaceVars": True,
            "curvals": {"curr_pipetask": "ptask1"},
        }
        found, value = self.config.search("genval2", opt=test_opt)
        self.assertEqual(found, True)
        self.assertEqual(value, "-c val1:32")

    def testBpsEvalInvalid(self):
        """Test reporting not replacing bpsEval."""
        test_opt = {
            "expandEnvVars": True,
            "replaceEnvVars": True,
            "replaceVars": True,
            "curvals": {"curr_pipetask": "ptask1"},
        }

        self.config["badkey1"] = "badval1 bpsEval('sum([1,2]') blah"
        with self.assertRaisesRegex(ValueError, "Unparsable bpsEval in 'badval1 bpsEval"):
            _ = self.config.search("badkey1", opt=test_opt)


if __name__ == "__main__":
    unittest.main()
