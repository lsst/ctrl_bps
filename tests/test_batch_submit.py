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
"""Unit tests for batch_submit.py."""

import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig, batch_submit


class TestCreateBatchStages(unittest.TestCase):
    """Tests for create_batch_stages function."""

    def setUp(self):
        self.common_config = {
            "bpsUseShared": True,
            "whenSaveJobQgraph": "NEVER",
            "useLazyCommands": True,
            "submitPath": "/the/path",
        }

    def testMissingBuildCmd(self):
        """Missing buildQuantumGraph jobCommand"""
        config_info = dict(self.common_config)
        config_info.update({"uniqProcName": "uniq_proc_name"})
        config = BpsConfig(config_info)
        with self.assertRaisesRegex(
            RuntimeError, "Missing executable for buildQuantumGraph.  Double check submit yaml for jobCommand"
        ):
            _ = batch_submit.create_batch_stages(config, "not_used_prefix")

    def testMissingPrepareCmd(self):
        """Missing preparePayloadWorkflow jobCommand"""
        config_info = dict(self.common_config)
        config_info.update(
            {
                "configFile": "not_used_configFile",
                "uniqProcName": "uniq_proc_name",
                "operator": "testuser",
                "payload": {"payloadName": "testPayload"},
                "buildQuantumGraph": {"jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-acquire {configFile}"},
            }
        )
        config = BpsConfig(config_info)
        with self.assertRaisesRegex(
            RuntimeError,
            "Missing executable for preparePayloadWorkflow.  Double check submit yaml for jobCommand",
        ):
            _ = batch_submit.create_batch_stages(config, "not_used_prefix")

    def testSuccess(self):
        # No saving of files
        config_info = dict(self.common_config)
        config_info.update(
            {
                "configFile": "not_used_configFile",
                "uniqProcName": "uniq_proc_name",
                "operator": "testuser",
                "payload": {"payloadName": "testPayload"},
                "buildQuantumGraph": {
                    "jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-acquire {configFile}",
                    "requestMemory": 16384,
                },
                "preparePayloadWorkflow": {
                    "jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-prepare {configFile}",
                    "requestMemory": 24576,
                },
            }
        )
        config = BpsConfig(config_info)

        with tempfile.TemporaryDirectory() as tmpdir:
            gw, config = batch_submit.create_batch_stages(config, tmpdir)
            self.assertIn("buildQuantumGraph", gw)
            job = gw.get_job("buildQuantumGraph")
            self.assertIn("batch-acquire", job.arguments)
            self.assertEqual(job.request_memory, 16384)
            self.assertIn("preparePayloadWorkflow", gw)
            job = gw.get_job("preparePayloadWorkflow")
            self.assertIn("batch-prepare", job.arguments)
            self.assertEqual(job.request_memory, 24576)

            # Check we didn't make any files
            self.assertEqual(list(Path(tmpdir).iterdir()), [])

    def testSaving(self):
        config_info = dict(self.common_config)
        config_info.update(
            {
                "configFile": "not_used_configFile",
                "uniqProcName": "uniq_proc_name",
                "operator": "testuser",
                "payload": {"payloadName": "testPayload"},
                "bpsPreCommandOpts": "--long-log --log-level=VERBOSE",
                "buildQuantumGraph": {
                    "jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-acquire {configFile}",
                    "requestMemory": 16384,
                },
                "preparePayloadWorkflow": {
                    "jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-prepare {configFile}",
                    "requestMemory": 24576,
                },
                "saveGenericWorkflow": True,
            }
        )
        config = BpsConfig(config_info)
        with tempfile.TemporaryDirectory() as tmpdir:
            gw, config = batch_submit.create_batch_stages(config, tmpdir)
            self.assertTrue((Path(tmpdir) / "bps_stages_generic_workflow.pickle").exists())


if __name__ == "__main__":
    unittest.main()
