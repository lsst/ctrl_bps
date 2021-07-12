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

"""Driver for the transformation of a QuantumGraph into a generic workflow.
"""

import logging
import os
import re

from . import BpsConfig, GenericWorkflow, GenericWorkflowJob, GenericWorkflowFile
from .bps_utils import save_qg_subgraph, WhenToSaveQuantumGraphs, create_job_quantum_graph_filename


_LOG = logging.getLogger(__name__)


def transform(config, clustered_quantum_graph, prefix):
    """Transform a ClusteredQuantumGraph to a GenericWorkflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    clustered_quantum_graph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        A clustered quantum graph to transform into a generic workflow.
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow transformed from the clustered quantum graph.
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to accompany GenericWorkflow.
    """
    if "name" in clustered_quantum_graph.graph and clustered_quantum_graph.graph["name"] is not None:
        name = clustered_quantum_graph.graph["name"]
    else:
        _, name = config.search("uniqProcName", opt={"required": True})

    generic_workflow = create_generic_workflow(config, clustered_quantum_graph, name, prefix)
    generic_workflow_config = create_generic_workflow_config(config, prefix)

    return generic_workflow, generic_workflow_config


def update_job(config, job):
    """Update given job with workflow attribute and profile values.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    job : `lsst.ctrl.bps.GenericWorkflowJob`
        Job to which the attributes and profile values should be added.
    """
    key = f".site.{job.compute_site}.profile.condor"

    if key in config:
        for key, val in config[key].items():
            if key.startswith("+"):
                job.attrs[key[1:]] = val
            else:
                job.profile[key] = val


def add_workflow_init_nodes(config, generic_workflow):
    """Add nodes to workflow graph that perform initialization steps.

    Assumes that all of the initialization should be executed prior to any
    of the current workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow to which the initialization steps should be added.
    """
    # Create a workflow graph that will have task and file nodes necessary for
    # initializing the pipeline execution
    init_workflow = create_init_workflow(config, generic_workflow.get_file("runQgraphFile"))
    _LOG.debug("init_workflow nodes = %s", init_workflow.nodes())

    # Find source nodes in workflow graph.
    workflow_sources = [n for n in generic_workflow if generic_workflow.in_degree(n) == 0]
    _LOG.debug("workflow sources = %s", workflow_sources)

    # Find sink nodes of initonly graph.
    init_sinks = [n for n in init_workflow if init_workflow.out_degree(n) == 0]
    _LOG.debug("init sinks = %s", init_sinks)

    # Add initonly nodes to Workflow graph and make new edges.
    generic_workflow.add_nodes_from(init_workflow.nodes(data=True))
    generic_workflow.add_edges_from(init_workflow.edges())
    for gwfile in init_workflow.get_files(data=True):
        generic_workflow.add_file(gwfile)
    for source in workflow_sources:
        for sink in init_sinks:
            generic_workflow.add_edge(sink, source)


def create_init_workflow(config, run_qgraph_gwfile):
    """Create workflow for running initialization job(s).

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    run_qgraph_gwfile: `lsst.ctrl.bps.GenericWorkflowFile`
        File object for the run QuantumGraph.

    Returns
    -------
    init_workflow : `lsst.ctrl.bps.GenericWorkflow`
        GenericWorkflow consisting of job(s) to initialize workflow.
    """
    _LOG.debug("creating init subgraph")
    _LOG.debug("creating init task input(s)")
    search_opt = {"curvals": {"curr_pipetask": "pipetaskInit"}, "required": False, "default": False}

    init_workflow = GenericWorkflow("init")

    # create job for executing --init-only
    gwjob = GenericWorkflowJob("pipetaskInit")
    gwjob.label = "pipetaskInit"
    gwjob.compute_site = config["computeSite"]
    search_opt["default"] = 0
    gwjob.request_cpus = int(config.search("requestCpus", opt=search_opt)[1])
    gwjob.request_memory = int(config.search("requestMemory", opt=search_opt)[1])
    gwjob.request_disk = int(config.search("requestDisk", opt=search_opt)[1])
    gwjob.request_walltime = int(config.search("requestWalltime", opt=search_opt)[1])
    update_job(config, gwjob)
    init_workflow.add_job(gwjob)

    # All outputs (config, software versions, etc) go to Butler.
    # Currently no need to add them to job.
    init_workflow.add_job_inputs(gwjob.name, run_qgraph_gwfile)
    create_command(config, init_workflow, gwjob)

    return init_workflow


def create_command(config, workflow, gwjob):
    """Create command line and vals.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains the job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job to which the command line and vals should
        be saved.
    """
    search_opt = {"curvals": {"curr_pipetask": gwjob.label},
                  "replaceVars": False,
                  "expandEnvVars": False,
                  "replaceEnvVars": True,
                  "required": False}

    # Get command line from config
    _, gwjob.cmdline = config.search("runQuantumCommand", opt=search_opt)

    # Change qgraph variable to match whether using run or per-job qgraph
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.NEVER:
        gwjob.cmdline = gwjob.cmdline.replace("{qgraphFile}", "{runQgraphFile}")
    else:
        gwjob.cmdline = gwjob.cmdline.replace("{qgraphFile}", f"{{qgraphFile_{gwjob.name}}}")

    # Replace files with special placeholders
    for gwfile in workflow.get_job_inputs(gwjob.name):
        gwjob.cmdline = gwjob.cmdline.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")
    for gwfile in workflow.get_job_outputs(gwjob.name):
        gwjob.cmdline = gwjob.cmdline.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")

    # Save dict of other values needed to complete cmdline
    # (Be careful to not replace env variables as they may
    # be different in compute job.)
    search_opt["replaceVars"] = True

    if gwjob.cmdvals is None:
        gwjob.cmdvals = {}
    for key in re.findall(r"{([^}]+)}", gwjob.cmdline):
        if key not in gwjob.cmdvals:
            _, gwjob.cmdvals[key] = config.search(key, opt=search_opt)

    # backwards compatibility
    _, use_lazy_commands = config.search("useLazyCommands", opt={"default": True})
    if not use_lazy_commands:
        _fill_command(config, workflow, gwjob)


def _fill_command(config, workflow, gwjob):
    """Replace placeholders in command line string in job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BPSConfig`
        Bps configuration.
    workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing the job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which to update command line by filling in values.
    """
    _, use_shared = config.search("useBpsShared", opt={"default": False})
    # Replace input file placeholders with paths
    for gwfile in workflow.get_job_inputs(gwjob.name):
        if use_shared:
            uri = os.path.basename(gwfile.src_uri)
        else:
            uri = gwfile.src_uri
        gwjob.cmdline = gwjob.cmdline.replace(f"<FILE:{gwfile.name}>", uri)

    # Replace output file placeholders with paths
    for gwfile in workflow.get_job_outputs(gwjob.name):
        if use_shared:
            uri = os.path.basename(gwfile.src_uri)
        else:
            uri = gwfile.src_uri
        gwjob.cmdline = gwjob.cmdline.replace(f"<FILE:{gwfile.name}>", uri)

    gwjob.cmdline = gwjob.cmdline.format(**gwjob.cmdvals)


def create_job_values_universal(config, qnodes, generic_workflow, gwjob, prefix):
    """Create job values.  Must be same value for every PipelineTask in
    cluster.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BPSConfig`
        Bps configuration.
    qnodes : `list` [`lsst.pipe.base.QuantumGraph`]
        Full run QuantumGraph.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job to which values will be added.
    prefix : `str`
        Root path for any output files.
    """
    per_job_qgraph_file = True
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.NEVER:
        per_job_qgraph_file = False

    # Verify workflow config values are same for all nodes in QuantumGraph
    # for running the Quantum and compute_site.
    job_command = None
    job_compute_site = None
    for qnode in qnodes:
        _LOG.debug("taskClass=%s", qnode.taskDef.taskClass)
        _LOG.debug("taskName=%s", qnode.taskDef.taskName)
        _LOG.debug("label=%s", qnode.taskDef.label)

        search_opt = {"curvals": {"curr_pipetask": qnode.taskDef.label}, "required": False}

        _, command = config.search("runQuantumCommand", opt=search_opt)
        if job_command is None:
            job_command = command
        elif job_command != command:
            _LOG.error("Inconsistent command to run QuantumGraph\n"
                       "Cluster %s Quantum Number %d\n"
                       "Current cluster command: %s\n"
                       "Inconsistent command: %s",
                       gwjob.name, qnode.nodeId.number, job_command, command)
            raise RuntimeError("Inconsistent run QuantumGraph command")

        _, compute_site = config.search("computeSite", opt=search_opt)
        if job_compute_site is None:
            job_compute_site = compute_site
        elif job_compute_site != compute_site:
            _LOG.error("Inconsistent compute_site\n"
                       "Cluster %s Quantum Number %d\n"
                       "Current cluster compute_site: %s\n"
                       "Inconsistent compute_site: %s",
                       gwjob.name, qnode.nodeId.number, job_compute_site, compute_site)
            raise RuntimeError("Inconsistent run QuantumGraph command")

    if per_job_qgraph_file:
        gwfile = GenericWorkflowFile(f"qgraphFile_{gwjob.name}",
                                     src_uri=create_job_quantum_graph_filename(gwjob, prefix),
                                     wms_transfer=True,
                                     job_access_remote=True,
                                     job_shared=True)
    else:
        gwfile = generic_workflow.get_file("runQgraphFile")
        gwjob.cmdvals = {"qgraphNodeId": ",".join(sorted([f"{qnode.nodeId.number}" for qnode in qnodes])),
                         "qgraphId": qnodes[0].nodeId.buildId}

    generic_workflow.add_job_inputs(gwjob.name, gwfile)

    gwjob.cmdline = job_command
    create_command(config, generic_workflow, gwjob)
    if job_compute_site is not None:
        gwjob.compute_site = job_compute_site
    update_job(config, gwjob)


def create_job_values_aggregate(config, qnodes, gwjob, pipetask_labels):
    """Create job values that are aggregate of values from PipelineTasks
    in QuantumGraph.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    qnodes : `list` [`lsst.pipe.base.QuantumGraph`]
        Full run QuantumGraph.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job in which to store the aggregate values.
    pipetask_labels : `list` [`str`]
        PipelineTask labels used in generating quanta summary tag.
    """
    label_counts = dict.fromkeys(pipetask_labels, 0)

    gwjob.request_cpus = 0
    gwjob.request_memory = 0
    gwjob.request_disk = 0
    gwjob.request_walltime = 0

    for qnode in qnodes:
        label_counts[qnode.taskDef.label] += 1

        search_opt = {"curvals": {"curr_pipetask": qnode.taskDef.label}, "required": False, "default": 0}
        _, request_cpus = config.search("requestCpus", opt=search_opt)
        gwjob.request_cpus = max(gwjob.request_cpus, int(request_cpus))
        _, request_memory = config.search("requestMemory", opt=search_opt)
        gwjob.request_memory = max(gwjob.request_memory, int(request_memory))
        _, request_disk = config.search("requestDisk", opt=search_opt)
        gwjob.request_disk += int(request_disk)
        _, request_walltime = config.search("requestWalltime", opt=search_opt)
        gwjob.request_walltime += int(request_walltime)

    gwjob.tags["quanta_summary"] = ";".join([f"{k}:{v}" for k, v in label_counts.items() if v])


def create_generic_workflow(config, clustered_quanta_graph, name, prefix):
    """Create a generic workflow from a ClusteredQuantumGraph such that it
    has information needed for WMS (e.g., command lines).

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    clustered_quanta_graph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        ClusteredQuantumGraph for running a specific pipeline on a specific
        payload.
    name : `str`
        Name for the workflow (typically unique).
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow for the given ClusteredQuantumGraph + config.
    """
    generic_workflow = GenericWorkflow(name)

    # Save full run QuantumGraph for use by jobs
    generic_workflow.add_file(GenericWorkflowFile("runQgraphFile",
                                                  src_uri=config["run_qgraph_file"],
                                                  wms_transfer=True,
                                                  job_access_remote=True,
                                                  job_shared=True))

    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})

    for node_name, data in clustered_quanta_graph.nodes(data=True):
        _LOG.debug("clustered_quanta_graph: node_name=%s, len(cluster)=%s, label=%s, ids=%s", node_name,
                   len(data["qgraph_node_ids"]), data["label"], data["qgraph_node_ids"][:4])
        gwjob = GenericWorkflowJob(node_name)
        if "tags" in data:
            gwjob.tags = data["tags"]
        if "label" in data:
            gwjob.label = data["label"]
        generic_workflow.add_job(gwjob)

        qgraph = clustered_quanta_graph.graph["qgraph"]
        qnodes = []
        for node_id in data["qgraph_node_ids"]:
            qnodes.append(qgraph.getQuantumNodeByNodeId(node_id))
        pipetask_labels = [task.label for task in qgraph.iterTaskGraph()]
        create_job_values_universal(config, qnodes, generic_workflow, gwjob, prefix)
        create_job_values_aggregate(config, qnodes, gwjob, pipetask_labels)

        if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.TRANSFORM:
            save_qg_subgraph(qgraph, generic_workflow.get_file(f"qgraph_{gwjob.name}").src_uri,
                             data["qgraph_node_ids"])

    # Create job dependencies.
    for node_name in clustered_quanta_graph.nodes():
        for child in clustered_quanta_graph.successors(node_name):
            generic_workflow.add_job_relationships(node_name, child)

    # Add initial workflow
    if config.get("runInit", "{default: False}"):
        add_workflow_init_nodes(config, generic_workflow)
    add_workflow_attributes(config, generic_workflow)

    return generic_workflow


def add_workflow_attributes(config, generic_workflow):
    """Add workflow-level attributes to given GenericWorkflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow to which attributes should be added.
    """
    # Save run quanta summary and other workflow attributes to GenericWorkflow.
    run_quanta_counts = {}
    for job_name in generic_workflow:
        job = generic_workflow.get_job(job_name)
        if job.tags is not None and "quanta_summary" in job.tags:
            for job_summary_part in job.tags["quanta_summary"].split(";"):
                (label, cnt) = job_summary_part.split(":")
                if label not in run_quanta_counts:
                    run_quanta_counts[label] = 0
                run_quanta_counts[label] += int(cnt)

    run_quanta_summary = []
    for label in run_quanta_counts:
        run_quanta_summary.append("%s:%d" % (label, run_quanta_counts[label]))

    generic_workflow.run_attrs.update({"bps_run_summary": ";".join(run_quanta_summary),
                                       "bps_isjob": "True",
                                       "bps_project": config["project"],
                                       "bps_campaign": config["campaign"],
                                       "bps_run": generic_workflow.name,
                                       "bps_operator": config["operator"],
                                       "bps_payload": config["payloadName"],
                                       "bps_runsite": "TODO"})


def create_generic_workflow_config(config, prefix):
    """Create generic workflow configuration.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration accompanying the GenericWorkflow.
    """
    generic_workflow_config = BpsConfig(config)
    generic_workflow_config["workflowName"] = config["uniqProcName"]
    generic_workflow_config["workflowPath"] = prefix
    return generic_workflow_config
