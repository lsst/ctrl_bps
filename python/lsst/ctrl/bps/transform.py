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
import time
import dataclasses

from . import BpsConfig, GenericWorkflow, GenericWorkflowJob, GenericWorkflowFile
from .bps_utils import (save_qg_subgraph, WhenToSaveQuantumGraphs, create_job_quantum_graph_filename,
                        _create_execution_butler)

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
    generic_workflow.add_workflow_source(init_workflow)


def create_init_workflow(config, qgraph_gwfile):
    """Create workflow for running initialization job(s).

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
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

    # Handle universal values.
    _handle_job_values_universal(job_values, gwjob)

    # Handle aggregate values.
    _handle_job_values_aggregate(job_values, gwjob)

    # Save summary of Quanta in job.
    gwjob.tags["quanta_summary"] = "pipetaskInit:1"

    # Update job with workflow attribute and profile values.
    update_job(config, gwjob)

    init_workflow.add_job(gwjob)
    butler_gwfile = _get_butler_gwfile(config, config["submitPath"])
    init_workflow.add_job_inputs(gwjob.name, [qgraph_gwfile, butler_gwfile])
    gwjob.cmdline, gwjob.cmdvals = _enhance_command(config, init_workflow, gwjob, gwjob.cmdline)

    return init_workflow


def _enhance_command(config, generic_workflow, gwjob, cmdline):
    """Enhance command line with env and file placeholders
    and gather command line values.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains the job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job to which the command line and vals should be
        saved.
    cmdline : `str`
        Original command line with placeholders.

    Returns
    -------
    cmdline : `str`
        Command line for given job.
    cmdvals : `dict` [`str`, `Any`]
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
        cmdline = cmdline.replace("{qgraphFile}", "{runQgraphFile}")
    elif gwjob.name == "pipetaskInit":
        cmdline = cmdline.replace("{qgraphFile}", "{runQgraphFile}")
    else:    # Needed unique file keys for per-job QuantumGraphs
        cmdline = cmdline.replace("{qgraphFile}", f"{{qgraphFile_{gwjob.name}}}")

    # Replace files with special placeholders
    for gwfile in generic_workflow.get_job_inputs(gwjob.name):
        cmdline = cmdline.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")
    for gwfile in generic_workflow.get_job_outputs(gwjob.name):
        cmdline = cmdline.replace(f"{{{gwfile.name}}}", f"<FILE:{gwfile.name}>")

    # Save dict of other values needed to complete cmdline
    # (Be careful to not replace env variables as they may
    # be different in compute job.)
    search_opt["replaceVars"] = True

    cmdvals = {}
    for key in re.findall(r"{([^}]+)}", cmdline):
        if key not in cmdvals:
            _, cmdvals[key] = config.search(key, opt=search_opt)

    # backwards compatibility
    use_lazy_commands = config.search("useLazyCommands", opt={"default": True})
    if not use_lazy_commands:
        cmdline = _fill_command(config, generic_workflow, cmdline, cmdvals)

    return cmdline, cmdvals


def _fill_command(config, generic_workflow, cmdline, cmdvals):
    """Replace placeholders in command line string in job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Bps configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing the job.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which to update command line by filling in values.

    Returns
    -------
    cmdline : `str`
        Command line with FILE and ENV placeholders replaced.
    """
    # Replace file placeholders
    _, use_shared = config.search("useBpsShared", opt={"default": False})
    for file_key in re.findall(r"<FILE:([^>]+)>", cmdline):
        gwfile = generic_workflow.get_file(file_key)
        if use_shared:
            uri = os.path.basename(gwfile.src_uri)
        else:
            uri = gwfile.src_uri
        cmdline = cmdline.replace(f"<FILE:{file_key}>", uri)

    # Replace env placeholder with submit-side values
    cmdline = re.sub(r"<ENV:([^>]+)>", r"$\1", cmdline)
    cmdline = os.path.expandvars(cmdline)

    # Replace remaining vars
    cmdline = cmdline.format(**cmdvals)

    return cmdline


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
    cmd_line_key : `str`
        Which command line key to search for (e.g., "runQuantumCommand").

    Returns
    -------
    gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
        Representation of butler location (may not include filename).
    """
    special_values = ['name', 'label', 'cmdline', 'pre_cmdline', 'post_cmdline']

    job_values = {}
    for field in dataclasses.fields(GenericWorkflowJob):
        if field not in special_values:
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

    found, cmdline = config.search(cmd_line_key, opt=search_opt)
    if found:
        job_values["cmdline"] = cmdline

    return job_values


def _handle_job_values_universal(quantum_job_values, gwjob):
    """Handle job values that must be same value for every PipelineTask in
    cluster.

    Parameters
    ----------
    quantum_job_values : `dict` [`str`, `Any`]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the universal values.
    """
    universal_values = ['cmdline', 'compute_site']
    for key in universal_values:
        current_value = getattr(gwjob, key)
        if not current_value:
            setattr(gwjob, key, quantum_job_values[key])
        elif current_value != quantum_job_values[key]:
            _LOG.error("Inconsistent value for %s in "
                       "Cluster %s Quantum Number %s\n"
                       "Current cluster value: %s\n"
                       "Quantum value: %s",
                       key, gwjob.name, quantum_job_values.get("qgraphNodeId", "MISSING"), current_value,
                       quantum_job_values[key])
            raise RuntimeError(f"Inconsistent value for {key} in cluster {gwjob.name}.")


def _handle_job_values_aggregate(quantum_job_values, gwjob):
    """Handle job values that are aggregate of values from PipelineTasks
    in QuantumGraph.

    Parameters
    ----------
    quantum_job_values : `dict` [`str`, `Any`]
        Job values for running single Quantum.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job in which to store the aggregate values.
    """
    values_max = ['request_cpus', 'request_memory']
    values_sum = ['request_disk', 'request_walltime']

    for key in values_max:
        current_value = getattr(gwjob, key)
        if not current_value:
            setattr(gwjob, key, quantum_job_values[key])
        else:
            setattr(gwjob, key, max(getattr(gwjob, key), quantum_job_values[key]))

    for key in values_sum:
        current_value = getattr(gwjob, key)
        if not current_value:
            setattr(gwjob, key, quantum_job_values[key])
        else:
            setattr(gwjob, key, getattr(gwjob, key) + quantum_job_values[key])


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
    for node_name, data in clustered_quanta_graph.nodes(data=True):
        _LOG.debug("clustered_quanta_graph: node_name=%s, len(cluster)=%s, label=%s, ids=%s", node_name,
                   len(data["qgraph_node_ids"]), data["label"], data["qgraph_node_ids"][:4])
        gwjob = GenericWorkflowJob(node_name)
        if "tags" in data:
            gwjob.tags = data["tags"]
        if "label" in data:
            gwjob.label = data["label"]
        generic_workflow.add_job(gwjob)
        # Getting labels in pipeline order.
        label_counts = dict.fromkeys([task.label for task in qgraph.iterTaskGraph()], 0)

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

            # Handle universal values.
            _handle_job_values_universal(quantum_job_values, gwjob)

            # Handle aggregate values.
            _handle_job_values_aggregate(quantum_job_values, gwjob)

        # Save summary of Quanta in job.
        gwjob.tags["quanta_summary"] = ";".join([f"{k}:{v}" for k, v in label_counts.items() if v])

        # Update job with workflow attribute and profile values.
        update_job(config, gwjob)
        qgraph_gwfile = _get_qgraph_gwfile(config, gwjob, generic_workflow.get_file("runQgraphFile"),
                                           config["submitPath"])
        butler_gwfile = _get_butler_gwfile(config, config["submitPath"])
        generic_workflow.add_job_inputs(gwjob.name, [qgraph_gwfile, butler_gwfile])

        gwjob.cmdline, gwjob.cmdvals = _enhance_command(config, generic_workflow, gwjob, gwjob.cmdline)
        gwjob.cmdvals["qgraphId"] = data["qgraph_node_ids"][0].buildId
        gwjob.cmdvals["qgraphNodeId"] = ",".join(sorted([f"{node_id.number}" for node_id in
                                                         data["qgraph_node_ids"]]))

        # If writing per-job QuantumGraph files during TRANSFORM stage,
        # write it now while in memory.
        if save_per_job_qgraph:
            save_qg_subgraph(qgraph, qgraph_gwfile.src_uri, data["qgraph_node_ids"])

    # Create job dependencies.
    for node_name in clustered_quanta_graph.nodes():
        for child in clustered_quanta_graph.successors(node_name):
            generic_workflow.add_job_relationships(node_name, child)

    # Add initial workflow.
    if config.get("runInit", "{default: False}"):
        add_workflow_init_nodes(config, generic_workflow)
    add_workflow_attributes(config, generic_workflow)

    # Add final job
    add_final_job(config, generic_workflow, prefix)

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

        job_values = _get_job_values(config, search_opt, "mergeCommand")
        for field in dataclasses.fields(GenericWorkflowJob):
            if not getattr(gwjob, field.name):
                setattr(gwjob, field.name, job_values[field.name])

        update_job(config, gwjob)

        # Create script and add command line to job.
        gwjob.cmdline = _create_final_command(config, prefix)
        gwjob.transfer_executable = True

        # Determine inputs from cmdline
        for file_key in re.findall(r"<FILE:([^>]+)>", gwjob.cmdline):
            gwfile = generic_workflow.get_file(file_key)
            generic_workflow.add_job_inputs(gwjob.name, gwfile)

        gwjob.cmdline, gwjob.cmdvals = _enhance_command(config, generic_workflow, gwjob, gwjob.cmdline)

        # Put transfer repo job in appropriate location in workflow.
        if when_merge.upper() == "ALWAYS":
            # add as special final job
            generic_workflow.add_final(gwjob)
        elif when_merge.upper() == "SUCCESS":
            # add as regular sink node
            add_final_job_as_sink(generic_workflow, gwjob)
        else:
            raise ValueError(f"Invalid value for executionButler.when_merge {when_merge}")


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
    cmdline : `str`
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
    _, orig_butler = config.search("butlerConfig")
    # The execution butler was saved as butlerConfig in the workflow.
    return f"{script_file} {orig_butler} <FILE:butlerConfig>"


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
