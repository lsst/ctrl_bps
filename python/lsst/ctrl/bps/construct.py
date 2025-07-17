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

"""Driver for constructing a generic workflow running a custom job."""

__all__ = ["construct"]

import logging
import shutil
from collections.abc import Callable
from pathlib import Path

from lsst.ctrl.bps import (
    BpsConfig,
    GenericWorkflow,
    GenericWorkflowExec,
    GenericWorkflowFile,
    GenericWorkflowJob,
)
from lsst.ctrl.bps.transform import _get_job_values

_LOG = logging.getLogger(__name__)


def construct(config: BpsConfig) -> tuple[GenericWorkflow, BpsConfig]:
    """Create a workflow for running a custom job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration values to be used by submission.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow for running a standalone job.
    generic_workflow_config : `lsst.ctrl.BpsConfig`
        Configuration to accompany created generic workflow.
    """
    generic_workflow, generic_workflow_config = create_custom_workflow(config)
    return generic_workflow, generic_workflow_config


def create_custom_workflow(config: BpsConfig) -> tuple[GenericWorkflow, BpsConfig]:
    """Create a workflow that will run a custom job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow for running a custom job.
    generic_workflow_config : `lsst.ctrl.BpsConfig`
        Configuration to accompany created generic workflow.
    """
    gwjob, inputs, outputs = create_custom_job(config)

    _, name = config.search("uniqProcName", opt={"required": True})
    generic_workflow = GenericWorkflow(name)
    generic_workflow.add_job(gwjob)
    generic_workflow.run_attrs.update(
        {
            "bps_isjob": "True",
            "bps_iscustom": "True",
            "bps_project": config["project"],
            "bps_campaign": config["campaign"],
            "bps_run": generic_workflow.name,
            "bps_operator": config["operator"],
            "bps_payload": config["payloadName"],
            "bps_runsite": config["computeSite"],
        }
    )
    if inputs:
        generic_workflow.add_job_inputs(gwjob.name, inputs)
    if outputs:
        generic_workflow.add_job_outputs(gwjob.name, outputs)

    generic_workflow_config = BpsConfig(config)
    generic_workflow_config["workflowName"] = config["uniqProcName"]
    generic_workflow_config["workflowPath"] = config["submitPath"]

    return generic_workflow, generic_workflow_config


def create_custom_job(
    config: BpsConfig,
) -> tuple[GenericWorkflowJob, list[GenericWorkflowFile], list[GenericWorkflowFile]]:
    """Create a job that will run a custom command or script.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.

    Returns
    -------
    job : `lsst.ctrl.bps.GenericWorkflowJob`
        A custom job responsible for running the command.
    inputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
        List of job's input files, empty if the job has no input files.
    outputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
        List of job's output files, empty if the job has no output files.
    """
    prefix = Path(config["submitPath"])
    job_label = "customJob"

    search_opts = {"searchobj": config[job_label], "curvals": {}}
    found, value = config.search("computeSite", opt=search_opts)
    if found:
        search_opts["curvals"]["curr_site"] = value
    found, value = config.search("computeCloud", opt=search_opts)
    if found:
        search_opts["curvals"]["curr_cloud"] = value

    script_file = Path(config[f".{job_label}.executable"])
    script_name = script_file.name

    shutil.copy2(script_file, prefix)

    job = GenericWorkflowJob(name=script_name, label=job_label)
    job_values = _get_job_values(config, search_opts, "")
    for attr, value in job_values.items():
        if not getattr(job, attr):
            setattr(job, attr, value)
    job.executable = GenericWorkflowExec(
        name=script_name, src_uri=str(prefix / script_name), transfer_executable=True
    )
    _, job.arguments = config.search("arguments", opt=search_opts | {"replaceVars": False})

    inputs = []
    found, mapping = config.search("inputs", opt=search_opts)
    if found:
        inputs = create_job_files(mapping, prefix, path_creator=create_input_path)

    outputs = []
    found, mapping = config.search("outputs", opt=search_opts)
    if found:
        outputs = create_job_files(mapping, prefix, path_creator=create_output_path)

    for gwfile in inputs + outputs:
        job.arguments = job.arguments.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")

    return job, inputs, outputs


def create_job_files(
    file_specs: BpsConfig, prefix: str | Path, path_creator: Callable[[Path, Path], Path]
) -> list[GenericWorkflowFile]:
    """Create files for a job.

    Parameters
    ----------
    file_specs : `lsst.ctrl.bps.BpsConfig`
        The mapping between file keys and file paths.
    prefix : `str` | `pathlib.Path`
        The root directory to which the files will be written.
    path_creator : `Callable` [[`Path`, `Path`], `Path`]
        File category that determines actions that need to be taken during
        file creation.

    Returns
    -------
    gwfiles : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
        List of files created for the job.
    """
    prefix = Path(prefix)

    gwfiles = []
    for key, path in file_specs.items():
        src = Path(path)
        dest = path_creator(src, prefix)
        gwfiles.append(GenericWorkflowFile(name=key, src_uri=str(dest), wms_transfer=True))
    return gwfiles


def create_input_path(path: Path, prefix: Path) -> Path:
    """Process an input path.

    Parameters
    ----------
    path : `pathlib.Path`
        The input path.
    prefix : `pathlib.Path`
        The root directory to which the file will be written.

    Raises
    ------
    ValueError
        Raised if the input path does not exist or is a directory.
    """
    if path.exists():
        if path.is_dir():
            raise ValueError(f"input path '{path} is a directory, must be file")
    else:
        raise ValueError(f"input path '{path}' does not exist")
    dest = prefix / path.name
    shutil.copy2(path, dest)
    return dest


def create_output_path(path: Path, prefix: Path) -> Path:
    """Process an output path.

    Parameters
    ----------
    path : `pathlib.Path`
        The output path.
    prefix : `pathlib.Path`
        The root directory to which the file will be written.
    """
    if path.is_absolute():
        dest = path
    else:
        dest = prefix / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest
