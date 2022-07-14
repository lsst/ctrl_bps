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
"""Unit tests of transform.py"""
import dataclasses
import os
import shutil
import tempfile
import unittest

from cqg_test_utils import make_test_clustered_quantum_graph
from lsst.ctrl.bps import BPS_SEARCH_ORDER, BpsConfig, GenericWorkflowJob
from lsst.ctrl.bps.transform import _get_job_values, create_generic_workflow, create_generic_workflow_config

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestCreateGenericWorkflowConfig(unittest.TestCase):
    """Tests of create_generic_workflow_config."""

    def testCreate(self):
        """Test successful creation of the config."""
        config = BpsConfig({"a": 1, "b": 2, "uniqProcName": "testCreate"})
        wf_config = create_generic_workflow_config(config, "/test/create/prefix")
        self.assertIsInstance(wf_config, BpsConfig)
        for key in config:
            self.assertEqual(wf_config[key], config[key])
        self.assertEqual(wf_config["workflowName"], "testCreate")
        self.assertEqual(wf_config["workflowPath"], "/test/create/prefix")


class TestCreateGenericWorkflow(unittest.TestCase):
    """Tests of create_generic_workflow."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)
        self.config = BpsConfig(
            {
                "runInit": True,
                "computeSite": "global",
                "runQuantumCommand": "gexe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}",
                "clusterTemplate": "{D1}_{D2}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T2", "dimensions": "D1, D2"},
                    "cl2": {"pipetasks": "T3, T4", "dimensions": "D1, D2"},
                },
                "cloud": {
                    "cloud1": {"runQuantumCommand": "c1exe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}"},
                    "cloud2": {"runQuantumCommand": "c2exe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}"},
                },
                "site": {
                    "site1": {"runQuantumCommand": "s1exe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}"},
                    "site2": {"runQuantumCommand": "s2exe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}"},
                    "global": {"runQuantumCommand": "s3exe -q {qgraphFile} --qgraph-node-id {qgraphNodeId}"},
                },
                # Needed because transform assumes they exist
                "whenSaveJobQgraph": "NEVER",
                "executionButler": {"whenCreate": "SUBMIT", "whenMerge": "ALWAYS"},
            },
            BPS_SEARCH_ORDER,
        )
        self.cqg = make_test_clustered_quantum_graph(self.config)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testCreatingGenericWorkflowGlobal(self):
        """Test creating a GenericWorkflow with global settings."""
        config = BpsConfig(self.config)
        config["computeCloud"] = "cloud1"
        config["computeSite"] = "site2"
        config["queue"] = "global_queue"
        print(config)
        workflow = create_generic_workflow(config, self.cqg, "test_gw", self.tmpdir)
        for jname in workflow:
            gwjob = workflow.get_job(jname)
            print(gwjob)
            self.assertEqual(gwjob.compute_site, "site2")
            self.assertEqual(gwjob.compute_cloud, "cloud1")
            self.assertEqual(gwjob.executable.src_uri, "s2exe")
            self.assertEqual(gwjob.queue, "global_queue")
        final = workflow.get_final()
        self.assertEqual(final.compute_site, "site2")
        self.assertEqual(final.compute_cloud, "cloud1")
        self.assertEqual(final.queue, "global_queue")

    def testCreatingQuantumGraphMixed(self):
        """Test creating a GenericWorkflow with setting overrides."""
        config = BpsConfig(self.config)
        config[".cluster.cl1.computeCloud"] = "cloud2"
        config[".cluster.cl1.computeSite"] = "notthere"
        config[".cluster.cl2.computeSite"] = "site1"
        config[".executionButler.queue"] = "special_final_queue"
        config[".executionButler.computeSite"] = "special_site"
        config[".executionButler.computeCloud"] = "special_cloud"
        workflow = create_generic_workflow(config, self.cqg, "test_gw", self.tmpdir)
        for jname in workflow:
            gwjob = workflow.get_job(jname)
            print(gwjob)
            if jname.startswith("cl1"):
                self.assertEqual(gwjob.compute_site, "notthere")
                self.assertEqual(gwjob.compute_cloud, "cloud2")
                self.assertEqual(gwjob.executable.src_uri, "c2exe")
            elif jname.startswith("cl2"):
                self.assertEqual(gwjob.compute_site, "site1")
                self.assertIsNone(gwjob.compute_cloud)
                self.assertEqual(gwjob.executable.src_uri, "s1exe")
            elif jname.startswith("pipetask"):
                self.assertEqual(gwjob.compute_site, "global")
                self.assertIsNone(gwjob.compute_cloud)
                self.assertEqual(gwjob.executable.src_uri, "s3exe")
        final = workflow.get_final()
        self.assertEqual(final.compute_site, "special_site")
        self.assertEqual(final.compute_cloud, "special_cloud")
        self.assertEqual(final.queue, "special_final_queue")


class TestGetJobValues(unittest.TestCase):
    """Tests of _get_job_values."""

    def setUp(self):
        self.default_job = GenericWorkflowJob("default_job")

    def testGettingDefaults(self):
        """Test retrieving default values."""
        config = BpsConfig({})
        job_values = _get_job_values(config, {}, None)
        self.assertTrue(
            all(
                [
                    getattr(self.default_job, field.name) == job_values[field.name]
                    for field in dataclasses.fields(self.default_job)
                ]
            )
        )

    def testEnablingMemoryScaling(self):
        """Test enabling the memory scaling mechanism."""
        config = BpsConfig({"memoryMultiplier": 2.0})
        job_values = _get_job_values(config, {}, None)
        self.assertAlmostEqual(job_values["memory_multiplier"], 2.0)
        self.assertEqual(job_values["number_of_retries"], 5)

    def testDisablingMemoryScaling(self):
        """Test disabling the memory scaling mechanism."""
        config = BpsConfig({"memoryMultiplier": 0.5})
        job_values = _get_job_values(config, {}, None)
        self.assertIsNone(job_values["memory_multiplier"])

    def testRetrievingCmdLine(self):
        """Test retrieving the command line."""
        cmd_line_key = "runQuantum"
        config = BpsConfig({cmd_line_key: "/path/to/foo bar.txt"})
        job_values = _get_job_values(config, {}, cmd_line_key)
        self.assertEqual(job_values["executable"].name, "foo")
        self.assertEqual(job_values["executable"].src_uri, "/path/to/foo")
        self.assertEqual(job_values["arguments"], "bar.txt")


if __name__ == "__main__":
    unittest.main()
