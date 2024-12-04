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

"""Unit tests for the methods in construct.py."""

import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.construct import create_custom_job, create_custom_workflow


class CreateCustomJobTestCase(unittest.TestCase):
    """Tests for creating a custom job."""

    def setUp(self):
        self.script = tempfile.NamedTemporaryFile(prefix="foo", suffix=".sh")
        self.submit_dir = tempfile.TemporaryDirectory()
        self.config = BpsConfig(
            {
                "submitPath": self.submit_dir.name,
                "computeCloud": "testcloud",
                "computeSite": "testsite",
                "customJob": {
                    "executable": self.script.name,
                    "arguments": "arg",
                },
            },
            defaults={},
        )

    def tearDown(self):
        self.script.close()
        self.submit_dir.cleanup()

    def testSuccess(self):
        script_file = self.script.name
        script_name = Path(script_file).name

        job = create_custom_job(self.config)

        self.assertEqual(job.name, script_name)
        self.assertEqual(job.label, "customJob")
        self.assertEqual(job.compute_cloud, "testcloud")
        self.assertEqual(job.compute_site, "testsite")
        self.assertEqual(job.executable.name, script_name)
        self.assertEqual(job.executable.src_uri, f"{self.submit_dir.name}/{script_name}")
        self.assertTrue(job.executable.transfer_executable)
        self.assertTrue(Path(f"{self.submit_dir.name}/{script_name}").exists())
        self.assertEqual(job.arguments, "arg")


class CreateCustomWorkflowTestSuite(unittest.TestCase):
    """Tests for creating a custom workflow."""

    def setUp(self):
        self.script = tempfile.NamedTemporaryFile(prefix="bar", suffix=".sh")
        self.submit_dir = tempfile.TemporaryDirectory()
        self.config = BpsConfig(
            {
                "submitPath": self.submit_dir.name,
                "customJob": {
                    "executable": self.script.name,
                    "arguments": "arg",
                },
                "project": "dev",
                "campaign": "test",
                "operator": "tester",
                "payloadName": "custom/workflow",
                "computeCloud": "testcloud",
                "computeSite": "testsite",
            },
            defaults={},
        )

    def tearDown(self):
        self.script.close()
        self.submit_dir.cleanup()

    def testSuccess(self):
        self.config["uniqProcName"] = self.config["submitPath"]

        workflow, config = create_custom_workflow(self.config)

        self.assertEqual(workflow.name, self.config["uniqProcName"])
        self.assertEqual(workflow.job_counts, {"customJob": 1})
        self.assertTrue(workflow.run_attrs["bps_isjob"])
        self.assertTrue(workflow.run_attrs["bps_iscustom"])
        self.assertEqual(workflow.run_attrs["bps_project"], "dev")
        self.assertEqual(workflow.run_attrs["bps_campaign"], "test")
        self.assertEqual(workflow.run_attrs["bps_operator"], "tester")
        self.assertEqual(workflow.run_attrs["bps_payload"], "custom/workflow")
        self.assertEqual(workflow.run_attrs["bps_run"], workflow.name)
        self.assertEqual(workflow.run_attrs["bps_runsite"], "testsite")

        self.assertEqual(config["workflowName"], self.config["uniqProcName"])
        self.assertEqual(config["workflowPath"], self.config["submitPath"])
