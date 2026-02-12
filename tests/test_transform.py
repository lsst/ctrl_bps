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
"""Unit tests of transform.py."""

import copy
import dataclasses
import logging
import os
import shutil
import tempfile
import unittest

from cqg_test_utils import make_test_2_cluster_cqg

from lsst.ctrl.bps import (
    BPS_DEFAULTS,
    BPS_SEARCH_ORDER,
    BpsConfig,
    GenericWorkflow,
    GenericWorkflowExec,
    GenericWorkflowJob,
)
from lsst.ctrl.bps.transform import (
    _enhance_command,
    _get_job_values,
    create_final_command,
    create_generic_workflow,
    create_generic_workflow_config,
    gather_job_environment,
)

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
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("lsst.ctrl.bps.bps_config").setLevel(logging.INFO)
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)
        filename = os.path.join(TESTDIR, "data/config_for_transform.yaml")
        self.orig_config = BpsConfig(
            filename, BPS_SEARCH_ORDER, BPS_DEFAULTS, wms_service_class_fqn="wms_test_utils.WmsServiceSuccess"
        )
        _, self.cqg = make_test_2_cluster_cqg(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testCreatingQuantumGraph(self):
        """Test creating a GenericWorkflow with setting overrides.  While
        other tests exist to check get_job_valuei,gather_job_environment,
        etc., going ahead and checking the integration of these by checking
        results instead of using mocks.
        """
        config = BpsConfig(self.orig_config)
        config[".computeSite"] = "site1"
        config[".finalJob.queue"] = "special_final_queue"
        config[".finalJob.computeSite"] = "special_site"
        config[".finalJob.computeCloud"] = "special_cloud"
        workflow = create_generic_workflow(config, self.cqg, "test_gw", self.tmpdir)
        self.assertEqual(len(workflow) - 1, len(self.cqg))  # Don't count pipetaskInit
        for jname in workflow:
            gwjob = workflow.get_job(jname)
            self.assertEqual(gwjob.compute_site, "site1", f"failed for job {gwjob}")
            self.assertIsNone(gwjob.compute_cloud, f"failed for job {gwjob}")
            if gwjob.label == "pipetaskInit":
                self.assertEqual(gwjob.executable.src_uri, "pipetask", f"failed for job {gwjob}")
            else:
                self.assertEqual(gwjob.executable.src_uri, "s1exe", f"failed for job {gwjob}")
                self.assertIn("{qgraphNodeId}", gwjob.arguments)

            base_site_env_truth = {
                "VAR2": "root_val2",
                "VAR3": "root_val3",
                "VAR4": "site1_val4",
                "VAR_PATH": "<ENV:PACKAGE_DIR>/site1_dir:<ENV:PACKAGE_DIR>/root_dir:<ENV:VAR_PATH>",
                "TEST_VAR": "one site1_val1 three",
            }

            match gwjob.label:
                case "pipetaskInit":
                    env_truth = dict(base_site_env_truth)
                    env_truth["TEST_VAR"] = "one init_val1 three"
                    self.assertEqual(gwjob.environment, env_truth)
                    self.assertEqual(gwjob.cmdvals["initPreCmdOpts"], "--log-level=DEBUG")
                    # Taking pipetaskInit-defined requestMemory.
                    self.assertEqual(gwjob.request_memory, 8096)
                case "clusterT1T2":
                    # Taking cluster-defined requestMemory.
                    self.assertEqual(gwjob.request_memory, 6144)
                    self.assertEqual(len(gwjob.cmdvals["qgraphNodeId"].split(",")), 2)
                case "clusterT3T4":
                    # Taking max of the requestMemory for quanta in cluster.
                    self.assertEqual(gwjob.request_memory, 4048)
                    self.assertEqual(len(gwjob.cmdvals["qgraphNodeId"].split(",")), 2)
                case "T2b":
                    # Taking default requestMemory from root section.
                    self.assertEqual(gwjob.request_memory, BPS_DEFAULTS["requestMemory"])
                    self.assertEqual(len(gwjob.cmdvals["qgraphNodeId"].split(",")), 1)
                case "T5":
                    # Taking default requestMemory from root section.
                    self.assertEqual(gwjob.request_memory, BPS_DEFAULTS["requestMemory"])
                    self.assertEqual(len(gwjob.cmdvals["qgraphNodeId"].split(",")), 1)
                case _:
                    # Should always have a label from above, but need to
                    # fail test if get different label.
                    self.fail(f"Invalid gwjob.label for job {gwjob}")  # pragma: no cover
        final = workflow.get_final()
        self.assertEqual(final.compute_site, "special_site", f"failed for final job {final}")
        self.assertEqual(final.compute_cloud, "special_cloud", f"failed for final job {final}")
        self.assertEqual(final.queue, "special_final_queue", f"failed for final job {final}")
        self.assertEqual(final.request_memory, BPS_DEFAULTS["finalJob"]["requestMemory"])


class TestGetJobValues(unittest.TestCase):
    """Tests of _get_job_values."""

    def setUp(self):
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("lsst.ctrl.bps.bps_config").setLevel(logging.INFO)
        self.default_job = GenericWorkflowJob("default_job", "default_label")

    def testGettingDefaults(self):
        """Test retrieving default values."""
        config = BpsConfig({})
        job_values = _get_job_values(config, {}, None)
        self.assertTrue(
            all(
                getattr(self.default_job, field.name) == job_values[field.name]
                for field in dataclasses.fields(self.default_job)
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

    @unittest.mock.patch("lsst.ctrl.bps.transform.gather_job_environment")
    def testCallGatherJobEnvironmentNoSearchOpts(self, mock_gather):
        # Test that _get_job_values passes right search options on
        # to gather_job_environment function and didn't have side-effects.
        env_truth = {"TEST_INT": "1", "TEST_BOOL": "False", "TEST_SPACES": "one two three"}
        mock_gather.return_value = dict(env_truth)
        config = BpsConfig(
            {
                "var1": "two",
                "environment": {"TEST_INT": 1, "TEST_BOOL": False, "TEST_SPACES": "one {var1} three"},
                "runQuantumCommand": "/path/to/foo bar.txt",
                "pipetask": {"isr": {"requestMemory": 8096, "environment": {"ISR_VAR": "45"}}},
            }
        )
        search_opts = {}
        config_copy = BpsConfig(config)
        search_opts_copy = dict(search_opts)

        job_values = _get_job_values(config, search_opts, None)
        env_truth = {"TEST_INT": "1", "TEST_BOOL": "False", "TEST_SPACES": "one two three"}
        self.assertEqual(env_truth, job_values["environment"])
        mock_gather.assert_called_once_with(config, search_opts)

        # And didn't have side-effects that changed vars
        self.assertEqual(config, config_copy)
        self.assertEqual(search_opts, search_opts_copy)

    @unittest.mock.patch("lsst.ctrl.bps.transform.gather_job_environment")
    def testCallGatherJobEnvironmentWithCurvals(self, mock_gather):
        # Test that _get_job_values passes right search options on
        # to gather_job_environment function and didn't have side-effects.
        env_truth = {"TEST_INT": "1", "TEST_BOOL": "False", "TEST_SPACES": "one two three", "ISR_VAR": "45"}
        mock_gather.return_value = dict(env_truth)
        config = BpsConfig(
            {
                "var1": "two",
                "environment": {"TEST_INT": 1, "TEST_BOOL": False, "TEST_SPACES": "one {var1} three"},
                "runQuantumCommand": "/path/to/foo bar.txt",
                "pipetask": {"isr": {"requestMemory": 8096, "environment": {"ISR_VAR": "45"}}},
            }
        )
        curvals = {"curr_pipetask": "isr"}
        search_opts = {"replaceVars": False, "searchobj": {"curvals": curvals}}

        # Save copies to check no side-effects
        config_copy = BpsConfig(config)
        search_opts_copy = dict(search_opts)

        job_values = _get_job_values(config, search_opts, "runQuantumCommand")
        mock_gather.assert_called_once_with(config, search_opts)

        self.assertEqual(job_values["environment"], env_truth)
        self.assertEqual(job_values["executable"].src_uri, "/path/to/foo")

        # And didn't have side-effects that changed vars
        self.assertEqual(config, config_copy)
        self.assertEqual(search_opts, search_opts_copy)


class TestGatherJobEnvironment(unittest.TestCase):
    """Tests for the gather_job_environment function."""

    def setUp(self):
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("lsst.ctrl.bps.bps_config").setLevel(logging.INFO)
        # The directories don't match real ones, but are here to test
        # environment variables in yaml environment section as well as
        # appending values across sections.
        filename = os.path.join(TESTDIR, "data/config_for_transform.yaml")
        self.orig_config = BpsConfig(
            filename, BPS_SEARCH_ORDER, BPS_DEFAULTS, wms_service_class_fqn="wms_test_utils.WmsServiceSuccess"
        )

    def testEnvironmentRootSiteCluster(self):
        search_opts = {
            "replaceVars": False,
            "curvals": {"curr_cluster": "clusterT1T2", "curr_site": "site1"},
        }
        job_env = gather_job_environment(self.orig_config, search_opts)
        truth = {
            "VAR3": "cl12_val3",
            "VAR4": "cl12_val4",
            "VAR_PATH": "<ENV:PACKAGE_DIR>/cl12_dir:<ENV:PACKAGE_DIR>/site1_dir:<ENV:PACKAGE_DIR>/root_dir"
            ":<ENV:VAR_PATH>",
            "TEST_VAR": "one cl12_val1 three",
        }
        self.assertEqual(truth, job_env)
        self.assertEqual(search_opts["replaceVars"], False)

    def testEnvironmentRootSite(self):
        # Checking that doesn't pick up env from other cluster
        search_opts = {
            "replaceVars": False,
            "curvals": {"curr_cluster": "notthere", "curr_site": "site1"},
        }
        job_env = gather_job_environment(self.orig_config, search_opts)
        truth = {
            "VAR2": "root_val2",
            "VAR3": "root_val3",
            "VAR4": "site1_val4",
            "VAR_PATH": "<ENV:PACKAGE_DIR>/site1_dir:<ENV:PACKAGE_DIR>/root_dir:<ENV:VAR_PATH>",
            "TEST_VAR": "one site1_val1 three",
        }
        self.assertEqual(truth, job_env)
        self.assertEqual(search_opts["replaceVars"], False)

    def testEnvironmentRoot(self):
        # Checking that doesn't pick up env from other cluster or site.
        # Also check that doesn't modify our search_opts by setting opposites
        # of what function uses.
        orig_search_opts = {
            "replaceVars": False,
            "replaceEnvBps2Shell": True,
            "replaceEnvShell2Bps": False,
            "expandEnvVars": True,
            "curvals": {"curr_cluster": "notthere", "curr_site": "notthere"},
        }
        search_opts = copy.deepcopy(orig_search_opts)

        job_env = gather_job_environment(self.orig_config, search_opts)
        truth = {
            "VAR2": "root_val2",
            "VAR3": "root_val3",
            "VAR_PATH": "<ENV:PACKAGE_DIR>/root_dir:<ENV:VAR_PATH>",
            "TEST_VAR": "one root_val1 three",
        }
        self.assertEqual(truth, job_env)
        self.assertEqual(orig_search_opts, search_opts)

    def testEnvironmentNoSearchOpts(self):
        search_opts = {}
        job_env = gather_job_environment(self.orig_config, search_opts)
        truth = {
            "VAR2": "root_val2",
            "VAR_PATH": "<ENV:PACKAGE_DIR>/root_dir:<ENV:VAR_PATH>",
            "VAR3": "root_val3",
            "TEST_VAR": "one root_val1 three",
        }
        self.assertEqual(truth, job_env)
        self.assertEqual(search_opts, {})

    def testSearchObj(self):
        # Test that works with searchobj, like finalJob
        search_opts = {"searchobj": self.orig_config["finalJob"], "curvals": {"curr_site": "site1"}}
        copy_final = BpsConfig(self.orig_config["finalJob"])
        job_env = gather_job_environment(self.orig_config, search_opts)
        # VAR3 and VAR4 removed in setUp
        truth = {
            "TEST_VAR": "one final_val1 three",
            "VAR2": "root_val2",
            "VAR5": "final_val5",
            "VAR_PATH": "<ENV:PACKAGE_DIR>/final_dir:<ENV:PACKAGE_DIR>/site1_dir:<ENV:PACKAGE_DIR>/root_dir"
            ":<ENV:VAR_PATH>",
        }
        self.assertEqual(truth, job_env)
        self.assertEqual(search_opts["searchobj"], copy_final)

    def testVarsInEnvironment(self):
        config = BpsConfig(
            {
                "var1": "two",
                "environment": {"TEST_INT": 1, "TEST_BOOL": False, "TEST_SPACES": "one {var1} <ENV:var3>"},
            }
        )
        job_values = _get_job_values(config, {"replaceVars": True}, None)
        truth = {"TEST_INT": "1", "TEST_BOOL": "False", "TEST_SPACES": "one two <ENV:var3>"}
        self.assertEqual(truth, job_values["environment"])


class TestCreateFinalCommand(unittest.TestCase):
    """Tests for the create_final_command function."""

    def setUp(self):
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("lsst.ctrl.bps.bps_config").setLevel(logging.INFO)
        self.tmpdir = tempfile.TemporaryDirectory()
        self.script_beginning = [
            "#!/bin/bash\n",
            "\n",
            "set -e\n",
            "set -x\n",
            "qgraphFile=$1\n",
            "butlerConfig=$2\n",
        ]

    def tearDown(self):
        self.tmpdir.cleanup()

    def testSingleCommand(self):
        """Test with single final job command."""
        config_butler = f"{self.tmpdir.name}/test_repo"
        config = BpsConfig(
            {
                "var1": "42a",
                "var2": "42b",
                "var3": "42c",
                "butlerConfig": config_butler,
                "finalJob": {"command1": "/usr/bin/echo {var1} {qgraphFile} {var2} {butlerConfig} {var3}"},
            }
        )
        gwf_exec, args = create_final_command(config, self.tmpdir.name)
        self.assertEqual(args, f"<FILE:runQgraphFile> {config_butler}")
        final_script = f"{self.tmpdir.name}/final_job.bash"
        self.assertEqual(gwf_exec.src_uri, final_script)
        with open(final_script) as infh:
            lines = infh.readlines()
        self.assertEqual(
            lines, self.script_beginning + ["/usr/bin/echo 42a ${qgraphFile} 42b ${butlerConfig} 42c\n"]
        )

    def testMultipleCommands(self):
        config_butler = f"{self.tmpdir.name}/test_repo"
        config = BpsConfig(
            {
                "var1": "42a",
                "var2": "42b",
                "var3": "42c",
                "butlerConfig": config_butler,
                "finalJob": {
                    "command1": "/usr/bin/echo {var1} {qgraphFile} {var2} {butlerConfig} {var3}",
                    "command2": "/usr/bin/uptime",
                },
            }
        )
        gwf_exec, args = create_final_command(config, self.tmpdir.name)
        self.assertEqual(args, f"<FILE:runQgraphFile> {config_butler}")
        final_script = f"{self.tmpdir.name}/final_job.bash"
        self.assertEqual(gwf_exec.src_uri, final_script)
        with open(final_script) as infh:
            lines = infh.readlines()
        self.assertEqual(
            lines,
            self.script_beginning
            + ["/usr/bin/echo 42a ${qgraphFile} 42b ${butlerConfig} 42c\n", "/usr/bin/uptime\n"],
        )

    def testZeroCommands(self):
        config_butler = f"{self.tmpdir.name}/test_repo"
        config = BpsConfig(
            {
                "var1": "42a",
                "var2": "42b",
                "var3": "42c",
                "butlerConfig": config_butler,
                "finalJob": {
                    "cmd1": "/usr/bin/echo {var1} {qgraphFile} {var2} {butlerConfig} {var3}",
                    "cmd2": "/usr/bin/uptime",
                },
            }
        )
        with self.assertRaisesRegex(RuntimeError, "finalJob.whenRun"):
            _, _ = create_final_command(config, self.tmpdir.name)

    def testWhiteSpaceOnlyCommand(self):
        config_butler = f"{self.tmpdir.name}/test_repo"
        config = BpsConfig(
            {
                "butlerConfig": config_butler,
                "finalJob": {"command1": "", "command2": "\t \n"},
            }
        )
        with self.assertRaisesRegex(RuntimeError, "finalJob.whenRun"):
            _, _ = create_final_command(config, self.tmpdir.name)

    def testSkipCommandUsingWhiteSpace(self):
        config_butler = f"{self.tmpdir.name}/test_repo"
        config = BpsConfig(
            {
                "var1": "42a",
                "var2": "42b",
                "var3": "42c",
                "butlerConfig": config_butler,
                "finalJob": {
                    "command1": "/usr/bin/echo {var1} {qgraphFile} {var2} {butlerConfig} {var3}",
                    "command2": "",  # test skipping a command (i.e., overriding a default)
                    "command3": "/usr/bin/uptime",
                },
            }
        )
        gwf_exec, args = create_final_command(config, self.tmpdir.name)
        self.assertEqual(args, f"<FILE:runQgraphFile> {config_butler}")
        final_script = f"{self.tmpdir.name}/final_job.bash"
        self.assertEqual(gwf_exec.src_uri, final_script)
        with open(final_script) as infh:
            lines = infh.readlines()
        self.assertEqual(
            lines,
            self.script_beginning
            + ["/usr/bin/echo 42a ${qgraphFile} 42b ${butlerConfig} 42c\n", "\n", "/usr/bin/uptime\n"],
        )


class TestEnhanceCommand(unittest.TestCase):
    """Tests of _enhance_command function."""

    def setUp(self):
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("lsst.ctrl.bps.bps_config").setLevel(logging.INFO)
        self.gw_exec = GenericWorkflowExec("test_exec", "/dummy/dir/pipetask")
        self.config = BpsConfig(
            {
                # "profile": {},
                "bpsUseShared": True,
                "whenSaveJobQgraph": "NEVER",
                "useLazyCommands": True,
                # "memoryLimit": 32768,
                "defOpts": "--long-log --log-file {submitPath}/{jobName}.{wmsAttemptNum}.json",
                "submitPath": "/the/path",
            }
        )
        self.cached_vals = {
            "label1": {
                "profile": {},
                "bpsUseShared": True,
                "whenSaveJobQgraph": "NEVER",
                "useLazyCommands": True,
                "memoryLimit": 32768,
                "key1": "val1",
            }
        }

    def testAttemptNum(self):
        # test both in arguments as well as in variables in arguments
        gwjob = GenericWorkflowJob("job1", "label1", executable=self.gw_exec)
        gw = GenericWorkflow("test1")
        gw.add_job(gwjob)

        first_args = "{defOpts} run-qbb repo test.qg --summary {submitPath}/{jobName}-summary."
        gwjob.arguments = first_args + "{wmsAttemptNum}.json"

        new_arguments = first_args + "<WMS:attemptNum>.json"
        new_opts = "--long-log --log-file /the/path/job1.<WMS:attemptNum>.json"

        _enhance_command(self.config, gw, gwjob, {})

        self.assertEqual(gwjob.arguments, new_arguments)
        self.assertEqual(gwjob.cmdvals["defOpts"], new_opts)

    def testKeyCachedCmdVal(self):
        gwjob = GenericWorkflowJob("job1", "label1", executable=self.gw_exec)
        gw = GenericWorkflow("test1")
        gw.add_job(gwjob)
        gwjob.arguments = "run-qbb repo test.qg -x {key1}"
        self.assertNotIn("key1", gwjob.cmdvals)
        _enhance_command(self.config, gw, gwjob, self.cached_vals)
        self.assertEqual(gwjob.cmdvals["key1"], "val1")

    def testS3Argument(self):
        """Make sure s3 double slashes are not getting removed."""
        gwjob = GenericWorkflowJob("job1", "label1", executable=self.gw_exec)
        gw = GenericWorkflow("test1")
        gw.add_job(gwjob)
        s3 = "s3://user1@rubin-place-users/butler-pipeline1-processing.yaml"
        gwjob.arguments = s3
        _enhance_command(self.config, gw, gwjob, {})
        self.assertEqual(gwjob.arguments, s3)


if __name__ == "__main__":
    unittest.main()
