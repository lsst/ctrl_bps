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

"""Driver for the transformation of a QuantumGraph into a
generic workflow.
"""

import logging
import os

from .bps_config import BpsConfig
from .generic_workflow import GenericWorkflow, GenericWorkflowJob, GenericWorkflowFile
from .bps_utils import save_qg_subgraph, WhenToSaveQuantumGraphs, create_job_quantum_graph_filename


_LOG = logging.getLogger(__name__)


def transform(config, clustered_quantum_graph, prefix):
    """Transform a ClusteredQuantumGraph to a GenericWorkflow.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        BPS configuration.
    clustered_quantum_graph : `~lsst.ctrl.bps.clustered_quantum_graph.ClusteredQuantumGraph`
        A clustered quantum graph to transform into a generic workflow.
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        The generic workflow transformed from the clustered quantum graph.
    """
    if "name" in clustered_quantum_graph.graph and clustered_quantum_graph.graph["name"] is not None:
        name = clustered_quantum_graph.graph["name"]
    else:
        _, name = config.search("uniqProcName", opt={"required": True})

    generic_workflow = create_generic_workflow(config, clustered_quantum_graph, name, prefix)
    generic_workflow_config = create_generic_workflow_config(config, prefix)

    # Save QuantumGraphs.
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.TRANSFORM:
        for job_name in generic_workflow.nodes():
            job = generic_workflow.get_job(job_name)
            if job.qgraph_node_ids is not None:
                save_qg_subgraph(clustered_quantum_graph.graph["qgraph"],
                                 create_job_quantum_graph_filename(job, prefix),
                                 job.qgraph_node_ids)

    return generic_workflow, generic_workflow_config


def group_clusters_into_jobs(clustered_quanta_graph, name):
    """Group clusters of quanta into compute jobs.

    Parameters
    ----------
    clustered_quanta_graph : `~lsst.ctrl.bps.clustered_quantum_graph.ClusteredQuantumGraph`
        Graph where each node is a QuantumGraph of quanta that should be run
        inside single python execution.
    name : `str`
        Name of GenericWorkflow (typically unique by conventions).

    Returns
    -------
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        Skeleton of the generic workflow (job placeholders and dependencies)
    """
    generic_workflow = GenericWorkflow(name)

    for node_name, data in clustered_quanta_graph.nodes(data=True):
        _LOG.debug("clustered_quanta_graph: node_name=%s, len(cluster)=%s, label=%s, ids=%s", node_name,
                   len(data["qgraph_node_ids"]), data["label"], data["qgraph_node_ids"][:4])
        job = GenericWorkflowJob(node_name)
        job.qgraph_node_ids = data["qgraph_node_ids"]
        if "tags" in data:
            job.tags = data["tags"]
        if "label" in data:
            job.label = data["label"]
        generic_workflow.add_job(job)

    # Create job dependencies.
    for node_name in clustered_quanta_graph.nodes():
        children = clustered_quanta_graph.successors(node_name)
        for child in children:
            generic_workflow.add_job_relationships(node_name, child)

    return generic_workflow


def update_job(config, job):
    """Update given job with workflow attribute and profile values.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration.
    job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
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
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration.
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        Generic workflow to which the initialization steps should be added.
    """
    # Create a workflow graph that will have task and file nodes necessary for
    # initializing the pipeline execution
    init_workflow = create_init_workflow(config)
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
    generic_workflow._files.update(init_workflow._files)
    for source in workflow_sources:
        for sink in init_sinks:
            generic_workflow.add_edge(sink, source)


def create_init_workflow(config):
    """Create workflow for running initialization job(s).

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration.

    Returns
    -------
    init_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        GenericWorkflow consisting of job(s) to initialize workflow
    """
    _LOG.debug("creating init subgraph")
    _LOG.debug("creating init task input(s)")
    search_opt = {"curvals": {"curr_pipetask": "pipetaskInit"}, "required": False, "default": False}
    _, use_shared = config.search("bpsUseShared", opt=search_opt)
    gwfile = GenericWorkflowFile(os.path.basename(config["run_qgraph_file"]),
                                 wms_transfer=not use_shared,
                                 src_uri=config["run_qgraph_file"])

    init_workflow = GenericWorkflow("init")

    # create job for executing --init-only
    job = GenericWorkflowJob("pipetaskInit")
    job.label = "pipetaskInit"
    job.compute_site = config["computeSite"]
    search_opt["default"] = 0
    job.request_cpus = int(config.search("requestCpus", opt=search_opt)[1])
    job.request_memory = int(config.search("requestMemory", opt=search_opt)[1])
    job.request_disk = int(config.search("requestDisk", opt=search_opt)[1])
    job.request_walltime = int(config.search("requestWalltime", opt=search_opt)[1])
    update_job(config, job)
    create_command(config, job, gwfile)
    init_workflow.add_job(job)

    # All outputs (config, software versions, etc) go to Butler.
    # Currently no need to add them to job.
    init_workflow.add_job_inputs(job.name, gwfile)

    return init_workflow


def create_command(config, gwjob, gwfile):
    """Update command line string in job.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        Bps configuration.
    gwjob : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
        Job for which to create command line.
    gwfile : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
        File that will contain the QuantumGraph.
    """
    search_opt = {"curvals": {"curr_pipetask": gwjob.label}, "required": False}

    if gwfile.wms_transfer:
        search_opt["curvals"]["qgraphFile"] = os.path.basename(gwfile.src_uri)
    else:
        search_opt["curvals"]["qgraphFile"] = gwfile.src_uri

    if gwjob.qgraph_node_ids:
        search_opt["curvals"]["qgraphId"] = gwjob.qgraph_node_ids[0].buildId
        search_opt["curvals"]["qgraphNodeId"] = ",".join([f"{nid.number}" for nid in gwjob.qgraph_node_ids])

    _, gwjob.cmdline = config.search("runQuantumCommand", opt=search_opt)


def create_job_values_universal(config, qgraph, generic_workflow, prefix):
    """Create job values.  Must be same value for every PipelineTask in
    QuantumGraph.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        Bps configuration.
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Full run QuantumGraph.
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        Generic workflow in which job values will be added.
    prefix : `str`
        Root path for any output files.
    """
    per_job_qgraph_file = True
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})

    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.NEVER:
        per_job_qgraph_file = False
        run_qgraph_gwfile = GenericWorkflowFile(os.path.basename(config["run_qgraph_file"]),
                                                src_uri=config["run_qgraph_file"])

    # Verify workflow config values are same for all nodes in QuantumGraph
    # for running the Quantum and compute_site.
    for job_name, data in generic_workflow.nodes(data=True):
        generic_workflow_job = data["job"]
        job_command = None
        job_compute_site = None
        job_use_shared = None   # Cannot set default or can get conflict on first Quantum.
        for node_id in generic_workflow_job.qgraph_node_ids:
            qnode = qgraph.getQuantumNodeByNodeId(node_id)
            task_def = qnode.taskDef
            _LOG.debug("config=%s", task_def.config)
            _LOG.debug("taskClass=%s", task_def.taskClass)
            _LOG.debug("taskName=%s", task_def.taskName)
            _LOG.debug("label=%s", task_def.label)

            search_opt = {"curvals": {"curr_pipetask": task_def.label}, "required": False}

            _, command = config.search("runQuantumCommand", opt=search_opt)
            if job_command is None:
                job_command = command
            elif job_command != command:
                _LOG.error("Inconsistent command to run QuantumGraph\n"
                           "Cluster %s Quantum Number %d\n"
                           "Current cluster command: %s\n"
                           "Inconsistent command: %s",
                           job_name, qnode.nodeId.number, job_command, command)
                raise RuntimeError("Inconsistent run QuantumGraph command")

            _, compute_site = config.search("computeSite", opt=search_opt)
            if job_compute_site is None:
                job_compute_site = compute_site
            elif job_compute_site != compute_site:
                _LOG.error("Inconsistent compute_site\n"
                           "Cluster %s Quantum Number %d\n"
                           "Current cluster compute_site: %s\n"
                           "Inconsistent compute_site: %s",
                           job_name, qnode.nodeId.number, job_compute_site, compute_site)
                raise RuntimeError("Inconsistent run QuantumGraph command")

            _, use_shared = config.search("bpsUseShared", opt=search_opt)
            if job_use_shared is None:
                job_use_shared = use_shared
            elif job_use_shared != use_shared:
                _LOG.error("Inconsistent bpsUseShared\n"
                           "Cluster %s Quantum Number %d\n"
                           "Current cluster bpsUseShared: %s\n"
                           "Inconsistent bpsUseShared: %s",
                           job_name, qnode.nodeId.number, job_use_shared, use_shared)
                raise RuntimeError("Inconsistent bpsUseShared value within cluster.")

        if per_job_qgraph_file:
            data["qgraph_file"] = create_job_quantum_graph_filename(generic_workflow_job, prefix)
            gwfile = GenericWorkflowFile(os.path.basename(data["qgraph_file"]),
                                         src_uri=data["qgraph_file"])
        else:
            data["qgraph_file"] = run_qgraph_gwfile.src_uri
            gwfile = run_qgraph_gwfile

        # Tell WMS whether to transfer QuantumGraph file.
        gwfile.wms_transfer = not job_use_shared

        generic_workflow.add_job_inputs(job_name, gwfile)

        generic_workflow_job.cmdline = job_command
        create_command(config, generic_workflow_job, gwfile)
        if job_compute_site is not None:
            generic_workflow_job.compute_site = job_compute_site
        update_job(config, generic_workflow_job)


def create_job_values_aggregate(config, qgraph, generic_workflow):
    """Create job values that are aggregate of values from PipelineTasks
    in QuantumGraph.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        Bps configuration.
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Full run QuantumGraph.
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        Generic workflow in which job values will be added.
    """
    for _, data in generic_workflow.nodes(data=True):
        # Verify workflow config values are same for all nodes in QuantumGraph
        # for running the Quantum and compute_site
        job = data["job"]

        pipeline_labels = [task.label for task in qgraph.iterTaskGraph()]
        label_counts = dict.fromkeys(pipeline_labels, 0)

        job.request_cpus = 0
        job.request_memory = 0
        job.request_disk = 0
        job.request_walltime = 0

        for node_id in job.qgraph_node_ids:  # Assumes ordering.
            qnode = qgraph.getQuantumNodeByNodeId(node_id)
            label_counts[qnode.taskDef.label] += 1

            search_opt = {"curvals": {"curr_pipetask": qnode.taskDef.label}, "required": False, "default": 0}
            _, request_cpus = config.search("requestCpus", opt=search_opt)
            job.request_cpus = max(job.request_cpus, int(request_cpus))
            _, request_memory = config.search("requestMemory", opt=search_opt)
            job.request_memory = max(job.request_memory, int(request_memory))
            _, request_disk = config.search("requestDisk", opt=search_opt)
            job.request_disk += int(request_disk)
            _, request_walltime = config.search("requestWalltime", opt=search_opt)
            job.request_walltime += int(request_walltime)

        job.quanta_summary = ";".join([f"{k}:{v}" for k, v in label_counts.items() if v])


def create_generic_workflow(config, clustered_quanta_graph, name, prefix):
    """Create a generic workflow from a ClusteredQuantumGraph such that it
    has information needed for WMS (e.g., command lines).

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        BPS configuration.
    clustered_quanta_graph : `~lsst.ctrl.bps.clustered_quantum_graph.ClusteredQuantumGraph`
        ClusteredQuantumGraph for running a specific pipeline on a specific
        payload.
    name : `str`
        Name for the workflow (typically unique).
    prefix : `str`
        Root path for any output files.
    """
    generic_workflow = group_clusters_into_jobs(clustered_quanta_graph, name)
    create_job_values_universal(config, clustered_quanta_graph.graph["qgraph"], generic_workflow, prefix)
    create_job_values_aggregate(config, clustered_quanta_graph.graph["qgraph"], generic_workflow)

    if config.get("runInit", "{default: False}"):
        add_workflow_init_nodes(config, generic_workflow)
    add_workflow_attributes(config, generic_workflow)
    return generic_workflow


def add_workflow_attributes(config, generic_workflow):
    """Add workflow-level attributes to given GenericWorkflow.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        Bps configuration.
    generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
        Generic workflow to which attributes should be added.
    """
    # Save run quanta summary and other workflow attributes to GenericWorkflow.
    run_quanta_counts = {}
    for job_name in generic_workflow:
        job = generic_workflow.get_job(job_name)
        if job.quanta_summary:
            for job_summary_part in job.quanta_summary.split(";"):
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
    config : `~lsst.ctrl.bps.bps_config.BPSConfig`
        Bps configuration.
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    generic_workflow_config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        Configuration accompanying the GenericWorkflow.
    """

    generic_workflow_config = BpsConfig(config)
    generic_workflow_config["workflowName"] = config["uniqProcName"]
    generic_workflow_config["workflowPath"] = prefix
    return generic_workflow_config
