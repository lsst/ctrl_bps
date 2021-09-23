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
import math
import os
import re
import time
import dataclasses

from . import (
    DEFAULT_MEM_RETRIES,
    BpsConfig,
    GenericWorkflow,
    GenericWorkflowJob,
    GenericWorkflowFile,
    GenericWorkflowExec,
)
from .bps_utils import (
    save_qg_subgraph,
    WhenToSaveQuantumGraphs,
    create_job_quantum_graph_filename,
    _create_execution_butler
)

# All available job attributes.
_ATTRS_ALL = set([field.name for field in dataclasses.fields(GenericWorkflowJob)])

# Job attributes that need to be set to their maximal value in the cluster.
_ATTRS_MAX = {
    "memory_multiplier",
    "number_of_retries",
    "request_cpus",
    "request_memory",
}

# Job attributes that need to be set to sum of their values in the cluster.
_ATTRS_SUM = {
    "request_disk",
    "request_walltime",
}

# Job attributes do not fall into a specific category
_ATTRS_MISC = {
    "name",
    "label",
    "tags",
    "cmdline",
    "cmdvals",
    "environment",
    "pre_cmdline",
    "post_cmdline",
    "profile",
    "attrs",
}

# Attributes that need to be the same for each quanta in the cluster.
_ATTRS_UNIVERSAL = _ATTRS_ALL - (_ATTRS_MAX | _ATTRS_MISC | _ATTRS_SUM)

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

    _, when_create = config.search(".executionButler.whenCreate")
    if when_create.upper() == "TRANSFORM":
        _LOG.info("Creating execution butler")
        stime = time.time()
        _, execution_butler_dir = config.search(".bps_defined.executionButlerDir")
        _create_execution_butler(config, config["runQgraphFile"], execution_butler_dir, prefix)
        _LOG.info("Creating execution butler took %.2f seconds", time.time() - stime)

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


def add_workflow_init_nodes(config, qgraph, generic_workflow):
    """Add nodes to workflow graph that perform initialization steps.

    Assumes that all of the initialization should be executed prior to any
    of the current workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    qgraph : `lsst.pipe.base.graph.QuantumGraph`
        The quantum graph the generic workflow represents.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow to which the initialization steps should be added.
    """
    # Create a workflow graph that will have task and file nodes necessary for
    # initializing the pipeline execution
    init_workflow = create_init_workflow(config, qgraph, generic_workflow.get_file("runQgraphFile"))
    _LOG.debug("init_workflow nodes = %s", init_workflow.nodes())
    generic_workflow.add_workflow_source(init_workflow)
    old_run_summary = generic_workflow.run_attrs.get("bps_run_summary", "")
    init_summary = init_workflow.run_attrs.get("bps_run_summary", "")
    generic_workflow.run_attrs["bps_run_summary"] = ';'.join(x for x in [init_summary, old_run_summary] if x)


def create_init_workflow(config, qgraph, qgraph_gwfile):
    """Create workflow for running initialization job(s).

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    qgraph : `lsst.pipe.base.graph.QuantumGraph`
        The quantum graph the generic workflow represents.
    qgraph_gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
        File object for the full run QuantumGraph file.

    Returns
    -------
    init_workflow : `lsst.ctrl.bps.GenericWorkflow`
        GenericWorkflow consisting of job(s) to initialize workflow.
    """
    _LOG.debug("creating init subgraph")
    _LOG.debug("creating init task input(s)")
    search_opt = {"curvals": {"curr_pipetask": "pipetaskInit"},
                  "replaceVars": False,
                  "expandEnvVars": False,
                  "replaceEnvVars": True,
                  "required": False}

    init_workflow = GenericWorkflow("init")
    init_workflow.add_file(qgraph_gwfile)

    # create job for executing --init-only
    gwjob = GenericWorkflowJob("pipetaskInit")
    gwjob.label = "pipetaskInit"

    job_values = _get_job_values(config, search_opt, "runQuantumCommand")

    # Adjust job attributes values if necessary.
    attrs = {"universal": _ATTRS_UNIVERSAL, "max": _ATTRS_MAX, "sum": _ATTRS_SUM}
    _handle_job_values(attrs, job_values, gwjob)

    # Pick a node id for each task (not quantum!) to avoid reading the entire
    # quantum graph during the initialization stage.
    node_ids = []
    for task in qgraph.iterTaskGraph():
        task_def = qgraph.findTaskDefByLabel(task.label)
        node = next(iter(qgraph.getNodesForTask(task_def)))
        node_ids.append(node.nodeId)
    gwjob.cmdvals["qgraphId"] = qgraph.graphID
    gwjob.cmdvals["qgraphNodeId"] = ",".join(sorted([f"{node_id.number}" for node_id in node_ids]))

    # Save summary of Quanta in job.
    gwjob.tags["quanta_summary"] = "pipetaskInit:1"

    # Update job with workflow attribute and profile values.
    update_job(config, gwjob)

    init_workflow.add_job(gwjob)
    butler_gwfile = _get_butler_gwfile(config, config["submitPath"])
    init_workflow.add_job_inputs(gwjob.name, [qgraph_gwfile, butler_gwfile])
    init_workflow.run_attrs["bps_run_summary"] = gwjob.tags["quanta_summary"]
    _enhance_command(config, init_workflow, gwjob)

    return init_workflow


def _enhance_command(config, generic_workflow, gwjob):
    """Enhance command line with env and file placeholders
    and gather command line values.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains the job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job to which the updated executable, arguments,
        and values should be saved.
    """
    search_opt = {"curvals": {"curr_pipetask": gwjob.label},
                  "replaceVars": False,
                  "expandEnvVars": False,
                  "replaceEnvVars": True,
                  "required": False}

    # Change qgraph variable to match whether using run or per-job qgraph
    # Note: these are lookup keys, not actual physical filenames.
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.NEVER:
        gwjob.arguments = gwjob.arguments.replace("{qgraphFile}", "{runQgraphFile}")
    elif gwjob.name == "pipetaskInit":
        gwjob.arguments = gwjob.arguments.replace("{qgraphFile}", "{runQgraphFile}")
    else:    # Needed unique file keys for per-job QuantumGraphs
        gwjob.arguments = gwjob.arguments.replace("{qgraphFile}", f"{{qgraphFile_{gwjob.name}}}")

    # Replace files with special placeholders
    for gwfile in generic_workflow.get_job_inputs(gwjob.name):
        gwjob.arguments = gwjob.arguments.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")
    for gwfile in generic_workflow.get_job_outputs(gwjob.name):
        gwjob.arguments = gwjob.arguments.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")

    # Save dict of other values needed to complete command line.
    # (Be careful to not replace env variables as they may
    # be different in compute job.)
    search_opt["replaceVars"] = True

    for key in re.findall(r"{([^}]+)}", gwjob.arguments):
        if key not in gwjob.cmdvals:
            _, gwjob.cmdvals[key] = config.search(key, opt=search_opt)

    # backwards compatibility
    _, use_lazy_commands = config.search("useLazyCommands", opt={"default": True})
    if not use_lazy_commands:
        gwjob.arguments = _fill_arguments(config, generic_workflow, gwjob.arguments, gwjob.cmdvals)


def _fill_arguments(config, generic_workflow, arguments, cmdvals):
    """Replace placeholders in command line string in job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing the job.
    arguments : `str`
        String containing placeholders.
    cmdvals : `dict` [`str`, `Any`]
        Any command line values that can be used to replace placeholders.

    Returns
    -------
    arguments : `str`
        Command line with FILE and ENV placeholders replaced.
    """
    # Replace file placeholders
    _, use_shared = config.search("bpsUseShared", opt={"default": False})
    for file_key in re.findall(r"<FILE:([^>]+)>", arguments):
        gwfile = generic_workflow.get_file(file_key)
        if gwfile.wms_transfer and not use_shared or not gwfile.job_shared:
            uri = os.path.basename(gwfile.src_uri)
        else:
            uri = gwfile.src_uri
        arguments = arguments.replace(f"<FILE:{file_key}>", uri)

    # Replace env placeholder with submit-side values
    arguments = re.sub(r"<ENV:([^>]+)>", r"$\1", arguments)
    arguments = os.path.expandvars(arguments)

    # Replace remaining vars
    arguments = arguments.format(**cmdvals)

    return arguments


def _get_butler_gwfile(config, prefix):
    """Get butler location to be used by job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    prefix : `str`
        Root path for any output files.

    Returns
    -------
    gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
        Representation of butler location.
    """
    _, when_create = config.search(".executionButler.whenCreate")
    if when_create.upper() == "NEVER":
        _, butler_config = config.search("butlerConfig")
        wms_transfer = False
        job_access_remote = True
        job_shared = True
    else:
        _, butler_config = config.search(".bps_defined.executionButlerDir")
        butler_config = os.path.join(prefix, butler_config)
        wms_transfer = True
        job_access_remote = False
        job_shared = False

    gwfile = GenericWorkflowFile("butlerConfig",
                                 src_uri=butler_config,
                                 wms_transfer=wms_transfer,
                                 job_access_remote=job_access_remote,
                                 job_shared=job_shared)

    return gwfile


def _get_qgraph_gwfile(config, gwjob, run_qgraph_file, prefix):
    """Get qgraph location to be used by job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which determining QuantumGraph file.
    run_qgraph_file : `lsst.ctrl.bps.GenericWorkflowFile`
        File representation of the full run QuantumGraph.
    prefix : `str`
        Path prefix for any files written.

    Returns
    -------
    gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
        Representation of butler location (may not include filename).
    """
    per_job_qgraph_file = True
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.NEVER:
        per_job_qgraph_file = False

    qgraph_gwfile = None
    if per_job_qgraph_file:
        qgraph_gwfile = GenericWorkflowFile(f"qgraphFile_{gwjob.name}",
                                            src_uri=create_job_quantum_graph_filename(config, gwjob, prefix),
                                            wms_transfer=True,
                                            job_access_remote=True,
                                            job_shared=True)
    else:
        qgraph_gwfile = run_qgraph_file

    return qgraph_gwfile


def _get_job_values(config, search_opt, cmd_line_key):
    """Gather generic workflow job values from the bps config.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    search_opt : `dict` [`str`, `Any`]
        Search options to be used when searching config.
    cmd_line_key : `str` or None
        Which command line key to search for (e.g., "runQuantumCommand").

    Returns
    -------
    job_values : `dict` [ `str`, `Any` ]`
        A mapping between job attributes and their values.
    """
    special_values = ['name', 'label', 'cmdline', 'pre_cmdline', 'post_cmdline']

    job_values = {}
    for field in dataclasses.fields(GenericWorkflowJob):
        if field.name not in special_values:
            # Variable names in yaml are camel case instead of snake case.
            yaml_name = re.sub(r"_(\S)", lambda match: match.group(1).upper(), field.name)
            found, value = config.search(yaml_name, opt=search_opt)
            if not found and '_' in field.name:
                # Just in case someone used snake case:
                found, value = config.search(field.name, opt=search_opt)
            if found:
                job_values[field.name] = value
            else:
                job_values[field.name] = None

    # If the automatic memory scaling is enabled (i.e. the memory multiplier
    # is set and it is a positive number greater than 1.0), adjust number
    # of retries when necessary.  If the memory multiplier is invalid, disable
    # automatic memory scaling.
    if job_values["memory_multiplier"] is not None:
        if math.ceil(float(job_values["memory_multiplier"])) > 1:
            if job_values["number_of_retries"] is None:
                job_values["number_of_retries"] = DEFAULT_MEM_RETRIES
        else:
            job_values["memory_multiplier"] = None

    if cmd_line_key:
        found, cmdline = config.search(cmd_line_key, opt=search_opt)
        # Make sure cmdline isn't None as that could be sent in as a
        # default value in search_opt.
        if found and cmdline:
            cmd, args = cmdline.split(" ", 1)
            job_values["executable"] = GenericWorkflowExec(os.path.basename(cmd), cmd, False)
            if args:
                job_values["arguments"] = args

    return job_values


def _handle_job_values(attributes, quantum_job_values, gwjob):
    """Set the job attributes in the cluster to their correct values.

    Parameters
    ----------
    attributes: `dict` [`str`, Iterable [`str`]]
        Job attributes grouped by category.  Supported categories are:
        * ``universal``: job attributes that need to be the same in the cluster,
        * ``max``: job attributes that need to be set to their maximal values
          in the cluster,
        * ``sum``: job attributes that need to be sum of their values
          in the cluster.
    quantum_job_values : `dict` [`str`, Any]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the universal values.
    """
    dispatcher = {
        'universal': _handle_job_values_universal,
        'max': _handle_job_values_max,
        'sum': _handle_job_values_sum,
    }
    for type_, func in dispatcher.items():
        attrs = attributes[type_]
        func(attrs, quantum_job_values, gwjob)


def _handle_job_values_universal(attributes, quantum_job_values, gwjob):
    """Handle job attributes that must have the same value for every quantum
    in the cluster.

    Parameters
    ----------
    attributes : Iterable [`str`]
        A list of field names which must have a single value for the entire
        cluster.
    quantum_job_values : `dict` [`str`, Any]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the universal values.
    """
    for attr in attributes:
        current_value = getattr(gwjob, attr)
        try:
            quantum_value = quantum_job_values[attr]
        except KeyError:
            continue
        else:
            if not current_value:
                setattr(gwjob, attr, quantum_value)
            elif current_value != quantum_value:
                _LOG.error("Inconsistent value for %s in Cluster %s Quantum Number %s\n"
                           "Current cluster value: %s\n"
                           "Quantum value: %s",
                           attr, gwjob.name, quantum_job_values.get("qgraphNodeId", "MISSING"), current_value,
                           quantum_value)
                raise RuntimeError(f"Inconsistent value for {attr} in cluster {gwjob.name}.")


def _handle_job_values_max(attributes, quantum_job_values, gwjob):
    """Handle job attributes that should be set to their maximum value in
    the in cluster.

    Parameters
    ----------
    attributes : Iterable [`str`]
        The names of job attributes which needs to be set to their maximum
        value in the entire cluster.
    quantum_job_values : `dict` [`str`, `Any`]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the aggregate values.
    """
    for attr in attributes:
        current_value = getattr(gwjob, attr)
        try:
            quantum_value = quantum_job_values[attr]
        except KeyError:
            continue
        else:
            needs_update = False
            if current_value is None:
                if quantum_value is not None:
                    needs_update = True
            else:
                if quantum_value is not None and current_value < quantum_value:
                    needs_update = True
            if needs_update:
                setattr(gwjob, attr, quantum_value)

                # When updating memory requirements for a job, check if memory
                # autoscaling is enabled. If it is, always use the memory
                # multiplier and the number of retries which comes with the
                # quantum.
                #
                # Note that as a result, the quantum with the biggest memory
                # requirements will determine whether the memory autoscaling
                # will be enabled (or disabled) depending on the value of its
                # memory multiplier.
                if attr == "request_memory":
                    gwjob.memory_multiplier = quantum_job_values["memory_multiplier"]
                    if gwjob.memory_multiplier is not None:
                        gwjob.number_of_retries = quantum_job_values["number_of_retries"]


def _handle_job_values_sum(attributes, quantum_job_values, gwjob):
    """Handle job attributes that are the sum of their values in the cluster.

    Parameters
    ----------
    attributes : Iterable [`str`]
        The names of job attributes which need to be a sum of the respective
        attributes in the entire cluster.
    quantum_job_values : `dict` [`str`, `Any`]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the aggregate values.
    """
    for attr in attributes:
        current_value = getattr(gwjob, attr)
        if not current_value:
            setattr(gwjob, attr, quantum_job_values[attr])
        else:
            setattr(gwjob, attr, current_value + quantum_job_values[attr])


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
    # Determine whether saving per-job QuantumGraph files in the loop.
    save_per_job_qgraph = False
    _, when_save = config.search("whenSaveJobQgraph", {"default": WhenToSaveQuantumGraphs.TRANSFORM.name})
    if WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.TRANSFORM:
        save_per_job_qgraph = True

    generic_workflow = GenericWorkflow(name)

    # Save full run QuantumGraph for use by jobs
    generic_workflow.add_file(GenericWorkflowFile("runQgraphFile",
                                                  src_uri=config["runQgraphFile"],
                                                  wms_transfer=True,
                                                  job_access_remote=True,
                                                  job_shared=True))

    qgraph = clustered_quanta_graph.graph["qgraph"]
    task_labels = [task.label for task in qgraph.iterTaskGraph()]
    run_label_counts = dict.fromkeys(task_labels, 0)
    for node_name, data in clustered_quanta_graph.nodes(data=True):
        _LOG.debug("clustered_quanta_graph: node_name=%s, len(cluster)=%s, label=%s, ids=%s", node_name,
                   len(data["qgraph_node_ids"]), data["label"], data["qgraph_node_ids"][:4])
        gwjob = GenericWorkflowJob(node_name)
        if "tags" in data:
            gwjob.tags = data["tags"]
        if "label" in data:
            gwjob.label = data["label"]

        # Getting labels in pipeline order.
        label_counts = dict.fromkeys(task_labels, 0)

        # Get job info either common or aggregate for all Quanta in cluster.
        for node_id in data["qgraph_node_ids"]:
            qnode = qgraph.getQuantumNodeByNodeId(node_id)
            label_counts[qnode.taskDef.label] += 1

            search_opt = {"curvals": {"curr_pipetask": qnode.taskDef.label},
                          "replaceVars": False,
                          "expandEnvVars": False,
                          "replaceEnvVars": True,
                          "required": False}
            quantum_job_values = _get_job_values(config, search_opt, "runQuantumCommand")

            attrs = {"universal": _ATTRS_UNIVERSAL, "max": _ATTRS_MAX, "sum": _ATTRS_SUM}
            _handle_job_values(attrs, quantum_job_values, gwjob)

        # Save summary of Quanta in job.
        gwjob.tags["quanta_summary"] = ";".join([f"{k}:{v}" for k, v in label_counts.items() if v])
        # Save job quanta counts to run
        for key in task_labels:
            run_label_counts[key] += label_counts[key]

        # Update job with workflow attribute and profile values.
        update_job(config, gwjob)
        qgraph_gwfile = _get_qgraph_gwfile(config, gwjob, generic_workflow.get_file("runQgraphFile"),
                                           config["submitPath"])
        butler_gwfile = _get_butler_gwfile(config, config["submitPath"])

        generic_workflow.add_job(gwjob)
        generic_workflow.add_job_inputs(gwjob.name, [qgraph_gwfile, butler_gwfile])

        gwjob.cmdvals["qgraphId"] = data["qgraph_node_ids"][0].buildId
        gwjob.cmdvals["qgraphNodeId"] = ",".join(sorted([f"{node_id.number}" for node_id in
                                                         data["qgraph_node_ids"]]))
        _enhance_command(config, generic_workflow, gwjob)

        # If writing per-job QuantumGraph files during TRANSFORM stage,
        # write it now while in memory.
        if save_per_job_qgraph:
            save_qg_subgraph(qgraph, qgraph_gwfile.src_uri, data["qgraph_node_ids"])

    # Save run's Quanta summary
    run_summary = ";".join([f"{k}:{v}" for k, v in run_label_counts.items()])
    generic_workflow.run_attrs["bps_run_summary"] = run_summary

    # Create job dependencies.
    for node_name in clustered_quanta_graph.nodes():
        for child in clustered_quanta_graph.successors(node_name):
            generic_workflow.add_job_relationships(node_name, child)

    # Add initial workflow.
    if config.get("runInit", "{default: False}"):
        add_workflow_init_nodes(config, qgraph, generic_workflow)

    generic_workflow.run_attrs.update({"bps_isjob": "True",
                                       "bps_project": config["project"],
                                       "bps_campaign": config["campaign"],
                                       "bps_run": generic_workflow.name,
                                       "bps_operator": config["operator"],
                                       "bps_payload": config["payloadName"],
                                       "bps_runsite": config["computeSite"]})

    # Add final job
    add_final_job(config, generic_workflow, prefix)

    return generic_workflow


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


def add_final_job(config, generic_workflow, prefix):
    """Add final workflow job depending upon configuration.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow to which attributes should be added.
    prefix : `str`
        Directory in which to output final script.
    """
    _, when_create = config.search(".executionButler.whenCreate")
    _, when_merge = config.search(".executionButler.whenMerge")

    search_opt = {"searchobj": config[".executionButler"], "default": None}
    if when_create.upper() != "NEVER" and when_merge.upper() != "NEVER":
        # create gwjob
        gwjob = GenericWorkflowJob("mergeExecutionButler")
        gwjob.label = "mergeExecutionButler"

        job_values = _get_job_values(config, search_opt, None)
        for field in dataclasses.fields(GenericWorkflowJob):
            if not getattr(gwjob, field.name) and job_values.get(field.name, None):
                setattr(gwjob, field.name, job_values[field.name])

        update_job(config, gwjob)

        # Create script and add command line to job.
        gwjob.executable, gwjob.arguments = _create_final_command(config, prefix)

        # Determine inputs from command line.
        for file_key in re.findall(r"<FILE:([^>]+)>", gwjob.arguments):
            gwfile = generic_workflow.get_file(file_key)
            generic_workflow.add_job_inputs(gwjob.name, gwfile)

        _enhance_command(config, generic_workflow, gwjob)

        # Put transfer repo job in appropriate location in workflow.
        if when_merge.upper() == "ALWAYS":
            # add as special final job
            generic_workflow.add_final(gwjob)
        elif when_merge.upper() == "SUCCESS":
            # add as regular sink node
            add_final_job_as_sink(generic_workflow, gwjob)
        else:
            raise ValueError(f"Invalid value for executionButler.when_merge {when_merge}")

        generic_workflow.run_attrs["bps_run_summary"] += ";mergeExecutionButler:1"


def _create_final_command(config, prefix):
    """Create the command and shell script for the final job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    prefix : `str`
        Directory in which to output final script.

    Returns
    -------
    executable : `lsst.ctrl.bps.GenericWorkflowExec`
        Executable object for the final script.
    arguments : `str`
        Command line needed to call the final script.
    """
    search_opt = {'replaceVars': False, 'replaceEnvVars': False, 'expandEnvVars': False}

    script_file = os.path.join(prefix, "final_job.bash")
    with open(script_file, "w") as fh:
        print("#!/bin/bash\n", file=fh)
        print("set -e", file=fh)
        print("set -x", file=fh)

        print("butlerConfig=$1", file=fh)
        print("executionButlerDir=$2", file=fh)

        i = 1
        found, command = config.search(f".executionButler.command{i}", opt=search_opt)
        while found:
            # Temporarily replace any env vars so formatter doesn't try to
            # replace them.
            command = re.sub(r"\${([^}]+)}", r"<BPSTMP:\1>", command)

            # executionButlerDir and butlerConfig will be args to script and
            # set to env vars
            command = command.replace("{executionButlerDir}", "<BPSTMP:executionButlerDir>")
            command = command.replace("{butlerConfig}", "<BPSTMP:butlerConfig>")

            # Replace all other vars in command string
            search_opt["replaceVars"] = True
            command = config.formatter.format(command, config, search_opt)
            search_opt["replaceVars"] = False

            # Replace any temporary env place holders.
            command = re.sub(r"<BPSTMP:([^>]+)>", r"${\1}", command)

            print(command, file=fh)
            i += 1
            found, command = config.search(f".executionButler.command{i}", opt=search_opt)
    os.chmod(script_file, 0o755)
    executable = GenericWorkflowExec(os.path.basename(script_file), script_file, True)

    _, orig_butler = config.search("butlerConfig")
    # The execution butler was saved as butlerConfig in the workflow.
    return executable, f"{orig_butler} <FILE:butlerConfig>"


def add_final_job_as_sink(generic_workflow, final_job):
    """Add final job as the single sink for the workflow.

    Parameters
    ----------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow to which attributes should be added.
    final_job : `lsst.ctrl.bps.GenericWorkflowJob`
        Job to add as new sink node depending upon all previous sink nodes.
    """
    # Find sink nodes of generic workflow graph.
    gw_sinks = [n for n in generic_workflow if generic_workflow.out_degree(n) == 0]
    _LOG.debug("gw_sinks = %s", gw_sinks)

    generic_workflow.add_job(final_job)
    generic_workflow.add_job_relationships(gw_sinks, final_job.name)
