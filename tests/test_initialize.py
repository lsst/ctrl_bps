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

"""Unit tests for initialize.py"""

import logging
import os
import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.initialize import (
    init_submission,
    out_collection_validator,
    output_run_validator,
    submit_path_validator,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestOutCollectionValidator(unittest.TestCase):
    """Checks that 'outCollection' is *not* specified in config.

    Assumes BpsConfig tests cover whether it finds values.
    """

    def testSuccess(self):
        config = BpsConfig({}, defaults={})
        out_collection_validator(config)

    def testFailure(self):
        config = BpsConfig({"outCollection": "dummy_collection"}, defaults={})
        with self.assertRaisesRegex(
            KeyError, "'outCollection' is deprecated. Replace all references to it with 'outputRun'"
        ):
            out_collection_validator(config)


class TestOutputRunValidator(unittest.TestCase):
    """Checks that 'outputRun' is specified in config.

    Assumes BpsConfig tests cover whether it finds values.
    """

    def testSuccess(self):
        config = BpsConfig({"outputRun": "dummy_run_value"}, defaults={})
        output_run_validator(config)

    def testFailure(self):
        config = BpsConfig({}, defaults={})
        with self.assertRaisesRegex(KeyError, "Must specify the output run collection using 'outputRun'"):
            output_run_validator(config)


class TestSubmitPathValidator(unittest.TestCase):
    """Check that 'submitPath' is specified in BPS config.

    Assumes BpsConfig tests cover whether it finds values.
    """

    def testSuccess(self):
        config = BpsConfig({"submitPath": "submit/dummy/path"}, defaults={})
        submit_path_validator(config)

    def testFailure(self):
        config = BpsConfig({}, defaults={})
        with self.assertRaisesRegex(
            KeyError, "Must specify the submit-side run directory using 'submitPath'"
        ):
            submit_path_validator(config)


class TestInitSubmission(unittest.TestCase):
    """Check init_submission_function."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def testBasicSuccess(self):
        filename = os.path.join(TESTDIR, "data/initialize_config.yaml")
        validators = [submit_path_validator, output_run_validator, out_collection_validator]
        config = init_submission(
            filename,
            validators=validators,
            compute_site="local",
            runWmsSubmissionChecks=False,
            wms_service="wms_test_utils.WmsServiceSuccess",
            tempDir=self.temp_dir.name,
            multVal=["a", "b", "c"],
        )
        self.assertIn(".bps_defined.timestamp", config)
        self.assertIn(".bps_defined.operator", config)
        self.assertIn(".bps_defined.uniqProcName", config)
        self.assertIn(".bps_defined.submitPath", config)
        self.assertEqual(config[".bps_cmdline.computeSite"], "local")
        self.assertEqual(config[".bps_cmdline.multVal"], "a,b,c")
        uniq_proc_name = config[".bps_defined.uniqProcName"]
        submit_path = Path(config[".bps_defined.submitPath"]).resolve()
        files = [f.name for f in submit_path.iterdir() if f.is_file()]
        self.assertIn("initialize_config.yaml", files)
        self.assertIn(f"{uniq_proc_name}_config.yaml", files)
        self.assertIn(f"{uniq_proc_name}.env.info.yaml", files)
        self.assertIn(f"{uniq_proc_name}.pkg.info.yaml", files)

        # generate_config tested elsewhere so just
        # check a couple values that shows it ran.
        self.assertEqual(config[".genall_1"], "/repo/test")
        self.assertEqual(config[".pipetask.ptask1.p3"], 32)
        self.assertEqual(config[".finalJob.gencfg_4"], 9)

    @unittest.mock.patch("lsst.ctrl.bps.initialize.BPS_DEFAULTS", {})
    def testMissingWmsServiceClass(self):
        filename = os.path.join(TESTDIR, "data/initialize_config.yaml")

        with self.assertRaisesRegex(KeyError, "Missing wmsServiceClass in bps config.  Aborting."):
            _ = init_submission(filename, runWmsSubmissionChecks=True, tempDir=self.temp_dir.name)

    @unittest.mock.patch("lsst.ctrl.bps.initialize.BPS_DEFAULTS", {})
    def testSubmissionChecksNotImplemented(self):
        filename = os.path.join(TESTDIR, "data/initialize_config.yaml")

        with self.assertLogs("lsst.ctrl.bps.initialize", level=logging.DEBUG) as cm:
            config = init_submission(
                filename,
                runWmsSubmissionChecks=True,
                tempDir=self.temp_dir.name,
                wms_service="wms_test_utils.WmsServiceSuccess",
            )
        output = " ".join(cm.output)
        self.assertIn("run_submission_checks is not implemented in wms_test_utils.WmsServiceSuccess.", output)
        self.assertIn(".bps_defined.timestamp", config)
        uniq_proc_name = config[".bps_defined.uniqProcName"]
        submit_path = Path(config[".bps_defined.submitPath"]).resolve()
        files = [f.name for f in submit_path.iterdir() if f.is_file()]
        self.assertIn("initialize_config.yaml", files)
        self.assertIn(f"{uniq_proc_name}_config.yaml", files)
        self.assertIn(f"{uniq_proc_name}.env.info.yaml", files)
        self.assertIn(f"{uniq_proc_name}.pkg.info.yaml", files)

    @unittest.mock.patch(
        "lsst.ctrl.bps.initialize.BPS_DEFAULTS", {"operator": "testuser", "uniqProcName": "uniqval"}
    )
    def testAlreadySet(self):
        """Test if operator and uniqProcName already set."""
        filename = os.path.join(TESTDIR, "data/initialize_config.yaml")

        config = init_submission(filename, runWmsSubmissionChecks=False, tempDir=self.temp_dir.name)
        self.assertEqual(config["operator"], "testuser")
        self.assertNotIn(".bps_defined.operator", config)
        self.assertEqual(config["uniqProcName"], "uniqval")
        self.assertNotIn(".bps_defined.uniqProcName", config)
