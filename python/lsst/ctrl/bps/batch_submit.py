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

"""Driver to run submit stages as batch jobs."""

__all__ = ["batch_payload_prepare", "create_batch_stages"]

import logging
import os

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.logging import VERBOSE
from lsst.utils.timer import time_this, timeMethod

from . import (
    DEFAULT_MEM_FMT,
    DEFAULT_MEM_UNIT,
    BpsConfig,
    GenericWorkflow,
    GenericWorkflowJob,
    GenericWorkflowLazyGroup,
)
from .pre_transform import cluster_quanta, read_quantum_graph
from .prepare import prepare
from .transform import _get_job_values, transform

_LOG = logging.getLogger(__name__)


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def create_batch_stages(
    config: BpsConfig, prefix: ResourcePathExpression
) -> tuple[GenericWorkflow, BpsConfig]:
    """Create a GenericWorkflow that performs the submit stages as a workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    prefix : `lsst.resources.ResourcePathExpression`
        Root path for any output files.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow transformed from the clustered quantum graph.
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to accompany GenericWorkflow.
    """
    prefix = ResourcePath(prefix)
    generic_workflow: GenericWorkflow = GenericWorkflow(name=f"{config['uniqProcName']}_ctrl")
    cmd_line_key = "jobCommand"

    # build QuantumGraph job
    search_opt = {}
    if "buildQuantumGraph" in config:
        search_opt["searchobj"] = config.get("buildQuantumGraph")
    build_job = GenericWorkflowJob(
        name="buildQuantumGraph",
        label="buildQuantumGraph",
    )
    job_values = _get_job_values(config, search_opt, cmd_line_key)
    if not job_values["executable"]:
        raise RuntimeError(
            f"Missing executable for buildQuantumGraph.  Double check submit yaml for {cmd_line_key}"
        )
    for key, value in job_values.items():
        if key not in {"name", "label"}:
            setattr(build_job, key, value)

    generic_workflow.add_job(build_job)
    generic_workflow.run_attrs.update(
        {
            "bps_isjob": "True",
            "bps_project": config["project"],
            "bps_campaign": config["campaign"],
            "bps_run": config["uniqProcName"],
            "bps_operator": config["operator"],
            "bps_payload": config["payloadName"],
            "bps_runsite": config["computeSite"],
        }
    )

    # cluster/transform/prepare job
    search_opt = {}
    if "preparePayloadWorkflow" in config:
        search_opt["searchobj"] = config.get("preparePayloadWorkflow")
    prepare_job = GenericWorkflowLazyGroup(
        name="preparePayloadWorkflow",
        label="preparePayloadWorkflow",
    )
    job_values = _get_job_values(config, search_opt, cmd_line_key)
    if not job_values["executable"]:
        raise RuntimeError(
            f"Missing executable for preparePayloadWorkflow.  Double check submit yaml for {cmd_line_key}"
        )
    for key, value in job_values.items():
        if key not in {"name", "label"}:
            setattr(prepare_job, key, value)

    generic_workflow.add_job(prepare_job, parent_names=["buildQuantumGraph"])

    _, save_workflow = config.search("saveGenericWorkflow", opt={"default": False})
    if save_workflow:
        with prefix.join("bps_stages_generic_workflow.pickle").open("wb") as outfh:
            generic_workflow.save(outfh, "pickle")

    return generic_workflow, config


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def batch_payload_prepare(config: BpsConfig, prefix: ResourcePathExpression) -> None:
    """Create a GenericWorkflow that performs the submit stages as a workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.

    prefix : `lsst.resources.ResourcePathExpression`
        Root path for any output files.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow transformed from the clustered quantum graph.
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to accompany GenericWorkflow.
    """
    prefix = ResourcePath(prefix)
    # Read existing QuantumGraph
    qgraph_filename = prefix.join(config["qgraphFileTemplate"])
    qgraph = read_quantum_graph(qgraph_filename)
    config[".bps_defined.runQgraphFile"] = str(qgraph_filename)

    # Cluster
    _LOG.info("Starting cluster stage (grouping quanta into jobs)")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Cluster stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])

    _LOG.info("ClusteredQuantumGraph contains %d cluster(s)", len(clustered_qgraph))

    submit_path = config[".bps_defined.submitPath"]
    _, save_clustered_qgraph = config.search("saveClusteredQgraph", opt={"default": False})
    if save_clustered_qgraph:
        clustered_qgraph.save(os.path.join(submit_path, "bps_clustered_qgraph.pickle"))
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        clustered_qgraph.draw(os.path.join(submit_path, "bps_clustered_qgraph.dot"))

    # Transform
    _LOG.info("Starting transform stage (creating generic workflow)")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Transform stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
        _LOG.info("Generic workflow name '%s'", generic_workflow.name)

    num_jobs = sum(generic_workflow.job_counts.values())
    _LOG.info("GenericWorkflow contains %d job(s) (including final)", num_jobs)

    _, save_workflow = config.search("saveGenericWorkflow", opt={"default": False})
    if save_workflow:
        with open(os.path.join(submit_path, "bps_generic_workflow.pickle"), "wb") as outfh:
            generic_workflow.save(outfh, "pickle")
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        with open(os.path.join(submit_path, "bps_generic_workflow.dot"), "w") as outfh:
            generic_workflow.draw(outfh, "dot")

    # Prepare
    _LOG.info("Starting prepare stage (creating specific implementation of workflow)")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Prepare stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)

    # Add payload workflow to currently running workflow
    _LOG.info("Starting update workflow")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Workflow update completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        # Assuming submit_path for ctrl workflow is visible by this job.
        wms_workflow.add_to_parent_workflow(generic_workflow_config)
