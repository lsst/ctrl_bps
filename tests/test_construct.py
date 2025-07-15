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

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lsst.ctrl.bps import BpsConfig, GenericWorkflowFile, GenericWorkflowJob
from lsst.ctrl.bps.construct import (
    construct,
    create_custom_job,
    create_custom_workflow,
    create_input_path,
    create_job_files,
    create_output_path,
)


class ConstructTestCase(unittest.TestCase):
    """Tests for the main construct function."""

    def setUp(self):
        self.script = tempfile.NamedTemporaryFile(prefix="foo", suffix=".sh")
        self.submit_dir = tempfile.TemporaryDirectory()
        self.config = BpsConfig(
            {
                "submitPath": self.submit_dir.name,
                "uniqProcName": "test_workflow",
                "project": "test_project",
                "campaign": "test_campaign",
                "operator": "test_operator",
                "payloadName": "test_payload",
                "computeSite": "test_site",
                "customJob": {
                    "executable": self.script.name,
                    "arguments": "test_arg",
                },
            },
            defaults={},
        )

    def tearDown(self):
        self.script.close()
        self.submit_dir.cleanup()

    def testConstructSuccess(self):
        """Test that construct returns a workflow and config."""
        workflow, config = construct(self.config)

        self.assertIsNotNone(workflow)
        self.assertIsNotNone(config)
        self.assertEqual(workflow.name, "test_workflow")
        self.assertEqual(config["workflowName"], "test_workflow")


class CreateCustomWorkflowTestCase(unittest.TestCase):
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
                "computeCloud": "test_cloud",
                "computeSite": "test_site",
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
        self.assertEqual(workflow.run_attrs["bps_runsite"], "test_site")
        self.assertEqual(config["workflowName"], self.config["uniqProcName"])
        self.assertEqual(config["workflowPath"], self.config["submitPath"])

    def testEmptyInputs(self):
        """Test workflow creation when job has no inputs files."""
        with patch("lsst.ctrl.bps.construct.create_custom_job") as mock_create:
            self.config["uniqProcName"] = "test_custom"

            job = GenericWorkflowJob(name="test_job", label="test_job")
            gwfile = GenericWorkflowFile(name="test_output", src_uri="test_output")
            mock_create.return_value = (job, [], [gwfile])

            workflow, config = create_custom_workflow(self.config)

            self.assertEqual(len(workflow.get_job_inputs(job.name)), 0)
            self.assertGreater(len(workflow.get_job_outputs(job.name)), 0)

    def testEmptyOutputs(self):
        """Test workflow creation when job has no output files."""
        with patch("lsst.ctrl.bps.construct.create_custom_job") as mock_create:
            self.config["uniqProcName"] = "test_custom"

            job = GenericWorkflowJob(name="test_job", label="test_job")
            gwfile = GenericWorkflowFile(name="test_input", src_uri="test_input")
            mock_create.return_value = (job, [gwfile], [])

            workflow, config = create_custom_workflow(self.config)

            self.assertGreater(len(workflow.get_job_inputs(job.name)), 0)
            self.assertEqual(len(workflow.get_job_outputs(job.name)), 0)

    def testEmptyInputsAndOutputs(self):
        """Test workflow creation when job has no input nor output files."""
        with patch("lsst.ctrl.bps.construct.create_custom_job") as mock_create:
            self.config["uniqProcName"] = "test_custom"

            job = GenericWorkflowJob(name="test_job", label="test_job")
            mock_create.return_value = (job, [], [])

            workflow, config = create_custom_workflow(self.config)

            self.assertEqual(len(workflow.get_job_inputs(job.name)), 0)
            self.assertEqual(len(workflow.get_job_outputs(job.name)), 0)


class CreateCustomJobTestCase(unittest.TestCase):
    """Tests for creating a custom job."""

    def setUp(self):
        self.script = tempfile.NamedTemporaryFile(prefix="foo", suffix=".sh")
        self.submit_dir = tempfile.TemporaryDirectory()
        self.config = BpsConfig(
            {
                "submitPath": self.submit_dir.name,
                "computeCloud": "test_cloud",
                "computeSite": "test_site",
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

    def testJobCreationNoFilesSuccess(self):
        """Test successful creation of a custom job."""
        script_file = self.script.name
        script_name = Path(script_file).name

        job, inputs, outputs = create_custom_job(self.config)

        self.assertEqual(job.name, script_name)
        self.assertEqual(job.label, "customJob")
        self.assertEqual(job.compute_cloud, "test_cloud")
        self.assertEqual(job.compute_site, "test_site")
        self.assertEqual(job.executable.name, script_name)
        self.assertEqual(job.executable.src_uri, f"{self.submit_dir.name}/{script_name}")
        self.assertTrue(job.executable.transfer_executable)
        self.assertTrue(Path(f"{self.submit_dir.name}/{script_name}").exists())
        self.assertEqual(job.arguments, "arg")
        self.assertEqual(inputs, [])
        self.assertEqual(outputs, [])

    def testJobCreationWithFilesSuccess(self):
        """Test custom job creation with input and output files."""
        # Create a temporary input file
        input_file = tempfile.NamedTemporaryFile(prefix="input", suffix=".txt", delete=False)
        input_file.write(b"test input data")
        input_file.close()

        self.config[".customJob.arguments"] = "--input {input1} --output {output1}"
        self.config[".customJob.inputs.input1"] = input_file.name
        self.config[".customJob.outputs.output1"] = "output.txt"

        try:
            job, inputs, outputs = create_custom_job(self.config)

            self.assertEqual(len(inputs), 1)
            self.assertEqual(len(outputs), 1)
            self.assertEqual(inputs[0].name, "input1")
            self.assertEqual(outputs[0].name, "output1")
            self.assertIn("<FILE:input1>", job.arguments)
            self.assertIn("<FILE:output1>", job.arguments)
        finally:
            os.unlink(input_file.name)

    def testJobCreationMissingExecutable(self):
        """Test custom job creation fails with missing executable."""
        self.config[".customJob.executable"] = "/nonexistent/script.sh"

        with self.assertRaises(FileNotFoundError):
            create_custom_job(self.config)


class CreateJobFilesTestCase(unittest.TestCase):
    """Tests for create_job_files function."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.prefix = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def testJobFileCreationNoFiles(self):
        """Test create_job_files with empty file specs."""
        config = BpsConfig({"inputs": {}})
        _, filespecs = config.search("inputs")
        files = create_job_files(filespecs, self.prefix, lambda path, prefix: prefix / path.name)

        self.assertEqual(files, [])

    def testJobFileCreationWithFiles(self):
        """Test create_job_files with file specifications."""
        config = BpsConfig(
            {
                "inputs": {
                    "file1": "/path/to/file1.txt",
                    "file2": "/path/to/file2.txt",
                }
            }
        )
        _, filespecs = config.search("inputs")
        files = create_job_files(filespecs, self.prefix, lambda path, prefix: prefix / path.name)

        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].name, "file1")
        self.assertEqual(files[1].name, "file2")
        self.assertTrue(files[0].wms_transfer)
        self.assertTrue(files[1].wms_transfer)


class CreateInputPathTestCase(unittest.TestCase):
    """Tests for create_input_path function."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.prefix = Path(self.temp_dir.name)

        # Create a test input file
        self.input_file = tempfile.NamedTemporaryFile(prefix="input_", suffix=".txt", delete=False)
        self.input_file.write(b"test content")
        self.input_file.close()

    def tearDown(self):
        self.temp_dir.cleanup()

    def testInputPathCreationSuccess(self):
        """Test successful input path creation."""
        input_path = Path(self.input_file.name)
        result_path = create_input_path(input_path, self.prefix)

        expected_path = self.prefix / input_path.name
        self.assertEqual(result_path, expected_path)
        self.assertTrue(result_path.exists())

        # Verify file content was copied
        with open(result_path) as f:
            content = f.read()
        self.assertEqual(content, "test content")

    def testInputPathCreationFileIsMissing(self):
        """Test input path creation fails if file does not exist."""
        nonexistent_path = Path("/nonexistent/file.txt")

        with self.assertRaisesRegex(ValueError, "does not exist"):
            create_input_path(nonexistent_path, self.prefix)

    def testInputPathCreationFileIsDirectory(self):
        """Test input path creation fails if path is a directory."""
        dir_path = Path(self.temp_dir.name)

        with self.assertRaisesRegex(ValueError, "is a directory"):
            create_input_path(dir_path, self.prefix)

    def testInputPathCreationPermissionError(self):
        """Test input path creation with permission denied."""
        with patch("shutil.copy2", side_effect=PermissionError("Permission denied")):
            with self.assertRaises(PermissionError):
                create_input_path(Path(self.input_file.name), self.prefix)


class CreateOutputPathTestCase(unittest.TestCase):
    """Tests for create_output_path function."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.prefix = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def testOutputPathCreationRelativePathNew(self):
        """Test output path creation."""
        output_path = Path("foo/bar.txt")
        result_path = create_output_path(output_path, self.prefix)
        expected_path = self.prefix / "foo/bar.txt"

        self.assertEqual(result_path, expected_path)
        self.assertTrue(result_path.parent.exists())

    def testOutputPathCreationRelativePathParentExits(self):
        """Test output path creation when directory already exists."""
        # Create the directory first
        subdir = self.prefix / "foo"
        subdir.mkdir()

        output_path = Path("foo/bar.txt")
        result_path = create_output_path(output_path, self.prefix)
        expected_path = self.prefix / "foo/bar.txt"

        self.assertEqual(result_path, expected_path)
        self.assertTrue(result_path.parent.exists())

    def testOutputPathCreationAbsolutePath(self):
        """Test that absolute output paths are handled properly."""
        output_path = self.prefix / "foo/bar.txt"
        expected_path = output_path
        result_path = create_output_path(output_path, self.prefix)

        self.assertEqual(result_path, expected_path)
        self.assertTrue(result_path.parent.exists())
