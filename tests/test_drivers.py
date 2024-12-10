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
"""Unit tests for drivers.py."""
import logging
import os
import shutil
import tempfile
import unittest

import yaml
from lsst.ctrl.bps.drivers import _init_submission_driver, ping_driver

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestInitSubmissionDriver(unittest.TestCase):
    """Test submission."""

    def setUp(self):
        self.cwd = os.getcwd()
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @unittest.mock.patch("lsst.ctrl.bps.initialize.BPS_DEFAULTS", {})
    def testDeprecatedOutCollection(self):
        config = {
            "submitPath": "bad",
            "payload": {
                "outCollection": "bad",
                "outputRun": "bad",
            },
        }
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml") as file:
            yaml.dump(config, stream=file)
            with self.assertRaisesRegex(KeyError, "outCollection"):
                _init_submission_driver(file.name)

    @unittest.mock.patch("lsst.ctrl.bps.initialize.BPS_DEFAULTS", {})
    def testMissingOutputRun(self):
        config = {"submitPath": "bad"}
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml") as file:
            yaml.dump(config, stream=file)
            with self.assertRaisesRegex(KeyError, "outputRun"):
                _init_submission_driver(file.name)

    @unittest.mock.patch("lsst.ctrl.bps.initialize.BPS_DEFAULTS", {})
    def testMissingSubmitPath(self):
        config = {"payload": {"outputRun": "bad"}}
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml") as file:
            yaml.dump(config, stream=file)
            with self.assertRaisesRegex(KeyError, "submitPath"):
                _init_submission_driver(file.name)


class TestPingDriver(unittest.TestCase):
    """Test ping."""

    def testWmsServiceSuccess(self):
        retval = ping_driver("wms_test_utils.WmsServiceSuccess")
        self.assertEqual(retval, 0)

    def testWmsServiceFailure(self):
        with self.assertLogs(level=logging.ERROR) as cm:
            retval = ping_driver("wms_test_utils.WmsServiceFailure")
            self.assertNotEqual(retval, 0)
            self.assertEqual(cm.records[0].getMessage(), "Couldn't contact service X")

    def testWmsServiceEnvVar(self):
        with unittest.mock.patch.dict(
            os.environ, {"BPS_WMS_SERVICE_CLASS": "wms_test_utils.WmsServiceSuccess"}
        ):
            retval = ping_driver()
            self.assertEqual(retval, 0)

    @unittest.mock.patch(
        "lsst.ctrl.bps.drivers.BPS_DEFAULTS", {"wmsServiceClass": "wms_test_utils.WmsServiceDefault"}
    )
    def testWmsServiceNone(self):
        with unittest.mock.patch.dict(os.environ, {}):
            with self.assertLogs(level=logging.INFO) as cm:
                retval = ping_driver()
                self.assertEqual(retval, 0)
                self.assertEqual(cm.records[0].getMessage(), "DEFAULT None")

    def testWmsServicePassThru(self):
        with self.assertLogs(level=logging.INFO) as cm:
            retval = ping_driver("wms_test_utils.WmsServicePassThru", "EXTRA_VALUES")
            self.assertEqual(retval, 0)
            self.assertRegex(cm.output[0], "INFO.+EXTRA_VALUES")


if __name__ == "__main__":
    unittest.main()
