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

import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lsst.ctrl.bps import BpsConfig, batch_submit


class TestCreateBatchStages(unittest.TestCase):
    """Tests for create_batch_stages function."""

    def testMissingBuildCmd(self):
        """Missing buildQuantumGraph jobCommand"""
        config = BpsConfig({"uniqProcName": "uniq_proc_name"})
        with self.assertRaisesRegex(
            RuntimeError, "Missing executable for buildQuantumGraph.  Double check submit yaml for jobCommand"
        ):
            _ = batch_submit.create_batch_stages(config, "not_used_prefix")

    def testMissingPrepareCmd(self):
        """Missing preparePayloadWorkflow jobCommand"""
        config = BpsConfig(
            {
                "configFile": "not_used_configFile",
                "uniqProcName": "uniq_proc_name",
                "operator": "testuser",
                "payload": {"payloadName": "testPayload"},
                "bpsPreCommandOpts": "--long-log --log-level=VERBOSE",
                "buildQuantumGraph": {"jobCommand": "${CTRL_BPS_DIR}/bin/bps batch-acquire {configFile}"},
            }
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "Missing executable for preparePayloadWorkflow.  Double check submit yaml for jobCommand",
        ):
            _ = batch_submit.create_batch_stages(config, "not_used_prefix")

    def testSuccess(self):
        # No saving of files
        config = BpsConfig(
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
            }
        )

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
        config = BpsConfig(
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
        with tempfile.TemporaryDirectory() as tmpdir:
            gw, config = batch_submit.create_batch_stages(config, tmpdir)
            self.assertTrue((Path(tmpdir) / "bps_stages_generic_workflow.pickle").exists())


class TestBatchPayloadPrepare(unittest.TestCase):
    """Tests for batch_payload_prepare function."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_info = {
            "runQgraphFile": "run.qgraph",
            "uniqProcName": "uniq_proc_name",
            "computeSite": "site1",
            "qgraphFileTemplate": "template.qgraph",
            "bps_defined": {"submitPath": self.tmpdir},
        }

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_mocks(self, mock_cluster, mock_transform, mock_prepare):
        """Configure the standard set of dependency mocks."""
        clustered_qgraph = MagicMock()
        clustered_qgraph.__len__.return_value = 3
        mock_cluster.return_value = clustered_qgraph

        generic_workflow = MagicMock()
        generic_workflow.name = "test_workflow"
        generic_workflow.job_counts = {"label1": 5}
        gwfile = MagicMock()
        generic_workflow.get_file.return_value = gwfile
        generic_workflow_config = MagicMock()
        mock_transform.return_value = (generic_workflow, generic_workflow_config)

        wms_workflow = MagicMock()
        mock_prepare.return_value = wms_workflow

        return clustered_qgraph, generic_workflow, generic_workflow_config, gwfile, wms_workflow

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testSuccessBasic(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Test success with all save flags off and no run temp space."""
        _, generic_workflow, generic_workflow_config, gwfile, wms_workflow = self._make_mocks(
            mock_cluster, mock_transform, mock_prepare
        )
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        mock_read.assert_called_once_with("run.qgraph")
        mock_cluster.assert_called_once()
        mock_transform.assert_called_once()
        mock_prepare.assert_called_once()
        # The runQgraphFile should be marked as not transferred by the WMS.
        self.assertFalse(gwfile.wms_transfer)
        # The payload workflow should be attached to the running workflow.
        wms_workflow.add_to_parent_workflow.assert_called_once_with(generic_workflow_config)
        # No files should be written with all save flags off.
        self.assertEqual(list(Path(self.tmpdir).iterdir()), [])

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testSaveClusteredQgraph(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Test saving of the clustered quantum graph."""
        clustered_qgraph, *_ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["saveClusteredQgraph"] = True
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        clustered_qgraph.save.assert_called_once()
        self.assertIn("bps_clustered_qgraph.pickle", clustered_qgraph.save.call_args[0][0])

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testSaveDotClustered(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Test writing of the dot file."""
        clustered_qgraph, *_ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["saveDot"] = True
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        clustered_qgraph.draw.assert_called_once()
        self.assertIn("bps_clustered_qgraph.dot", clustered_qgraph.draw.call_args[0][0])

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testSaveGenericWorkflow(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Test writing of the GenericWorkflow to a file."""
        _, generic_workflow, *_ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["saveGenericWorkflow"] = True
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        generic_workflow.save.assert_called_once()
        self.assertTrue((Path(self.tmpdir) / "bps_generic_workflow.pickle").exists())

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testSaveDotGeneric(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Test saving the generic workflow dot file."""
        _, generic_workflow, *_ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["saveDot"] = True
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        generic_workflow.draw.assert_called_once()
        self.assertEqual(generic_workflow.draw.call_args[0][1], "dot")
        self.assertTrue((Path(self.tmpdir) / "bps_generic_workflow.dot").exists())

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testUseRunTempSpaceFound(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """When run temp space is enabled and endpoint set, src_uri updates."""
        _, _, _, gwfile, _ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["bpsUseRunTempSpace"] = True
        self.config_info["fileDistributionEndpoint"] = "/run/temp/space"
        config = BpsConfig(self.config_info)

        batch_submit.batch_payload_prepare(config, self.tmpdir)

        self.assertEqual(gwfile.src_uri, str(Path("/run/temp/space") / "template.qgraph"))

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testUseRunTempSpaceMissingEndpoint(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """Run temp space enabled, missing endpoint should raise KeyError."""
        self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        self.config_info["bpsUseRunTempSpace"] = True
        config = BpsConfig(self.config_info)

        with self.assertRaisesRegex(KeyError, "fileDistributionEndpoint"):
            batch_submit.batch_payload_prepare(config, self.tmpdir)

    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.transform")
    @patch("lsst.ctrl.bps.batch_submit.cluster_quanta")
    @patch("lsst.ctrl.bps.batch_submit.read_quantum_graph")
    def testUseRunTempSpaceNotFound(self, mock_read, mock_cluster, mock_transform, mock_prepare):
        """When bpsUseRunTempSpace is absent, a debug message is logged."""
        _, _, _, gwfile, _ = self._make_mocks(mock_cluster, mock_transform, mock_prepare)
        config = BpsConfig(self.config_info)

        with self.assertLogs("lsst.ctrl.bps.batch_submit", level=logging.DEBUG) as cm:
            batch_submit.batch_payload_prepare(config, self.tmpdir)

        self.assertTrue(any("missing bpsUseRunTempSpace" in msg for msg in cm.output))


class TestBatchSubmit(unittest.TestCase):
    """Tests for batch_submit function."""

    def setUp(self):
        self.config_info = {"bps_defined": {"submitPath": "/the/path"}}

    @patch("lsst.ctrl.bps.batch_submit._make_id_link")
    @patch("lsst.ctrl.bps.batch_submit.submit")
    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.create_batch_stages")
    def testSuccessSubmits(self, mock_create, mock_prepare, mock_submit, mock_make_id_link):
        """Without dryRun the control workflow is prepared and submitted."""
        generic_workflow = MagicMock()
        config = BpsConfig(self.config_info)
        mock_create.return_value = (generic_workflow, config)
        wms_workflow = MagicMock()
        wms_workflow.run_id = "run123"
        mock_prepare.return_value = wms_workflow

        result = batch_submit.batch_submit(config)

        mock_create.assert_called_once()
        mock_prepare.assert_called_once()
        mock_submit.assert_called_once()
        mock_make_id_link.assert_called_once_with(config, "run123")
        self.assertIs(result, wms_workflow)

    @patch("lsst.ctrl.bps.batch_submit._make_id_link")
    @patch("lsst.ctrl.bps.batch_submit.submit")
    @patch("lsst.ctrl.bps.batch_submit.prepare")
    @patch("lsst.ctrl.bps.batch_submit.create_batch_stages")
    def testDryRun(self, mock_create, mock_prepare, mock_submit, mock_make_id_link):
        """With dryRun the workflow is not submitted but still returned."""
        generic_workflow = MagicMock()
        self.config_info["dryRun"] = True
        config = BpsConfig(self.config_info)
        mock_create.return_value = (generic_workflow, config)
        wms_workflow = MagicMock()
        wms_workflow.run_id = "run123"
        mock_prepare.return_value = wms_workflow

        result = batch_submit.batch_submit(config)

        mock_submit.assert_not_called()
        mock_make_id_link.assert_called_once_with(config, "run123")
        self.assertIs(result, wms_workflow)


if __name__ == "__main__":
    unittest.main()
