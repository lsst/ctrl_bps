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

from lsst.ctrl.bps import WmsRunReport, WmsStates
from lsst.ctrl.bps.bps_reports import compile_code_summary, compile_job_summary
from lsst.ctrl.bps.drivers import _init_submission_driver, ping_driver, report_driver, status_driver

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


class TestStatusDriver(unittest.TestCase):
    """Test status_driver function."""

    def testWmsServiceSuccess(self):
        with self.assertLogs(level=logging.INFO) as cm:
            retval = status_driver("wms_test_utils.WmsServiceSuccess", run_id="/dummy/path", hist_days=3)
            self.assertEqual(retval, WmsStates.SUCCEEDED.value)
            self.assertEqual(cm.records[0].getMessage(), "status: SUCCEEDED")

    def testWmsServiceFailure(self):
        with self.assertLogs(level=logging.WARNING) as cm:
            retval = status_driver("wms_test_utils.WmsServiceFailure", run_id="/dummy/path", hist_days=3)
            self.assertEqual(retval, WmsStates.FAILED.value)
            self.assertEqual(cm.records[0].getMessage(), "Dummy error message.")

    @unittest.mock.patch(
        "lsst.ctrl.bps.drivers.BPS_DEFAULTS", {"wmsServiceClass": "wms_test_utils.WmsServiceDefault"}
    )
    def testWmsServiceNone(self):
        with unittest.mock.patch.dict(os.environ, {}):
            retval = status_driver(None, run_id="/dummy/path", hist_days=3)
            self.assertEqual(retval, WmsStates.RUNNING.value)


class TestReportDriver(unittest.TestCase):
    """Test report_driver function."""

    @unittest.mock.patch(
        "lsst.ctrl.bps.drivers.BPS_DEFAULTS", new={"wmsServiceClass": "wms_test_utils.WmsServiceSuccess"}
    )
    def testWmsServiceFromDefaults(self):
        # Should not raise an exception and use default from BPS_DEFAULTS.
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            report_driver(
                wms_service=None,
                run_id=None,
                user=None,
                hist_days=0,
                pass_thru=None,
            )

    def testWmsServiceFromEnvVar(self):
        # Should not raise an exception.
        with unittest.mock.patch.dict(
            os.environ, {"BPS_WMS_SERVICE_CLASS": "wms_test_utils.WmsServiceSuccess"}
        ):
            report_driver(
                wms_service=None,
                run_id=None,
                user=None,
                hist_days=0.0,
                pass_thru=None,
            )

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testHistDefault(self, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id="123",
            user=None,
            hist_days=0.0,
            pass_thru=None,
        )

        # Verify retrieve_report was called with the default hist setting.
        _, kwargs = mock_retrieve.call_args
        self.assertAlmostEqual(kwargs["hist"], 2.0)

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testHistCustom(self, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id="123",
            user=None,
            hist_days=4.0,
            pass_thru=None,
        )

        # Verify retrieve_report was called with a custom hist setting.
        _, kwargs = mock_retrieve.call_args
        self.assertAlmostEqual(kwargs["hist"], 4.0)

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testPostprocessorsWithoutExitCodes(self, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id="123",
            user=None,
            hist_days=0.0,
            pass_thru=None,
            return_exit_codes=False,
        )

        # Verify the postprocessors list contains only one postprocessor.
        args, kwargs = mock_retrieve.call_args
        self.assertEqual(len(kwargs["postprocessors"]), 1)
        self.assertIn(compile_job_summary, kwargs["postprocessors"])

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testPostprocessorsWithExitCodes(self, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id="123",
            user=None,
            hist_days=0.0,
            pass_thru=None,
            return_exit_codes=True,
        )

        # Verify the postprocessors list contains both postprocessors.
        _, kwargs = mock_retrieve.call_args
        self.assertEqual(len(kwargs["postprocessors"]), 2)
        self.assertIn(compile_code_summary, kwargs["postprocessors"])
        self.assertIn(compile_job_summary, kwargs["postprocessors"])

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testPostprocessorsNoRunId(self, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id=None,
            user=None,
            hist_days=0.0,
            pass_thru=None,
        )

        # Verify postprocessors contains compile_job_summary
        _, kwargs = mock_retrieve.call_args
        self.assertIsNone(kwargs["postprocessors"])

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testDisplayCalledIfRuns(self, mock_display, mock_retrieve):
        mock_runs = [WmsRunReport(wms_id="1", state=WmsStates.SUCCEEDED)]
        mock_retrieve.return_value = (mock_runs, [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id=None,
            user=None,
            hist_days=0,
            pass_thru=None,
        )

        # Verify display_report was called with the runs
        mock_display.assert_called_once()
        args, kwargs = mock_display.call_args
        self.assertEqual(args[0], mock_runs)

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    def testDisplayCalledIfMessages(self, mock_display, mock_retrieve):
        mock_messages = ["Warning message 1", "Warning message 2"]
        mock_retrieve.return_value = ([], mock_messages)

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id=None,
            user=None,
            hist_days=0,
            pass_thru=None,
        )

        # Verify display_report was called with messages
        mock_display.assert_called_once()
        args, kwargs = mock_display.call_args
        self.assertEqual(args[1], mock_messages)

    @unittest.mock.patch("lsst.ctrl.bps.drivers.retrieve_report")
    @unittest.mock.patch("lsst.ctrl.bps.drivers.display_report")
    @unittest.mock.patch("builtins.print")
    def testNoRecordsFoundMessage(self, mock_print, mock_display, mock_retrieve):
        mock_retrieve.return_value = ([], [])

        report_driver(
            wms_service="wms_test_utils.WmsServiceSuccess",
            run_id="123",
            user=None,
            hist_days=1.5,
            pass_thru=None,
        )

        # Verify display_report() was NOT called.
        mock_display.assert_not_called()

        # Verify that a helpful message was printed.
        mock_print.assert_called_once()
        call_args = mock_print.call_args[0][0]
        self.assertIn("No records found", call_args)
        self.assertIn("123", call_args)


if __name__ == "__main__":
    unittest.main()
