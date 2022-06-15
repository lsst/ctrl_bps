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
"""Unit tests for drivers.py."""
import logging
import os
import shutil
import tempfile
import unittest

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.drivers import _init_submission_driver, ping_driver

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestInitSubmissionDriver(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testDeprecatedOutCollection(self):
        with self.assertRaisesRegex(KeyError, "outCollection"):
            _init_submission_driver({"payload": {"outCollection": "bad"}})

    def testMissingOutputRun(self):
        with self.assertRaisesRegex(KeyError, "outputRun"):
            _init_submission_driver({"payload": {"inCollection": "bad"}})

    def testMissingSubmitPath(self):
        with self.assertRaisesRegex(KeyError, "submitPath"):
            _init_submission_driver({"payload": {"outputRun": "bad"}})


class TestPingDriver(unittest.TestCase):
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

    @unittest.mock.patch.dict(os.environ, {})
    def testWmsServiceNone(self):
        # Override default wms to be the test one
        with unittest.mock.patch.object(BpsConfig, "__getitem__") as mock_function:
            mock_function.return_value = "wms_test_utils.WmsServiceDefault"
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
