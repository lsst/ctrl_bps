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

""" Placeholder HTCondor DAGMan API.  There is new work on a python DAGMan
API from HTCondor.  However, at this time, it tries to make things easier by
assuming DAG is easily broken into levels where there are 1-1 or all-to-all
relationships to nodes in next level.  LSST workflows are more complicated.
"""

__all__ = ["DagStatus", "JobStatus", "RestrictedDict", "HTCJob", "HTCDag", "htc_escape",
           "htc_write_attribs", "htc_write_condor_file", "htc_version", "htc_submit_dag",
           "condor_q", "condor_history", "read_dag_status", "MISSING_ID",
           "summary_from_dag", "read_dag_log", "read_node_status", "read_dag_nodes_log",
           "htc_check_dagman_output", "pegasus_name_to_label"]

import itertools
import os
import re
import json
import logging
from collections import defaultdict
from collections.abc import MutableMapping
from enum import IntEnum
from pathlib import Path
import pprint
import subprocess

import networkx
import classad
import htcondor

# from ...wms_service import WmsRunReport, WmsJobReport, WmsStates

_LOG = logging.getLogger(__name__)


MISSING_ID = -99999


class DagStatus(IntEnum):
    """HTCondor DAGMan's statuses for a DAG.
    """
    OK = 0
    ERROR = 1       # an error condition different than those listed here
    FAILED = 2      # one or more nodes in the DAG have failed
    ABORTED = 3     # the DAG has been aborted by an ABORT-DAG-ON specification
    REMOVED = 4     # the DAG has been removed by condor_rm
    CYCLE = 5       # a cycle was found in the DAG
    SUSPENDED = 6   # the DAG has been suspended (see section 2.10.8)


class JobStatus(IntEnum):
    """HTCondor's statuses for jobs.
    """
    UNEXPANDED = 0	            # Unexpanded
    IDLE = 1	                # Idle
    RUNNING = 2	                # Running
    REMOVED = 3	                # Removed
    COMPLETED = 4	            # Completed
    HELD = 5	                # Held
    TRANSFERRING_OUTPUT = 6     # Transferring_Output
    SUSPENDED = 7	            # Suspended


class NodeStatus(IntEnum):
    """HTCondor's statuses for DAGman nodes.
    """
    # (STATUS_NOT_READY): At least one parent has not yet finished or the node is a FINAL node.
    NOT_READY = 0

    # (STATUS_READY): All parents have finished, but the node is not yet running.
    READY = 1

    # (STATUS_PRERUN): The node’s PRE script is running.
    PRERUN = 2

    # (STATUS_SUBMITTED): The node’s HTCondor job(s) are in the queue.
    #                     StatusDetails = "not_idle" -> running.
    #                     JobProcsHeld = 1-> hold.
    #                     JobProcsQueued = 1 -> idle.
    SUBMITTED = 3

    # (STATUS_POSTRUN): The node’s POST script is running.
    POSTRUN = 4

    # (STATUS_DONE): The node has completed successfully.
    DONE = 5

    # (STATUS_ERROR): The node has failed. StatusDetails has info (e.g., ULOG_JOB_ABORTED for deleted job).
    ERROR = 6


HTC_QUOTE_KEYS = {"environment"}
HTC_VALID_JOB_KEYS = {
    "universe",
    "executable",
    "arguments",
    "environment",
    "log",
    "error",
    "output",
    "should_transfer_files",
    "when_to_transfer_output",
    "getenv",
    "notification",
    "transfer_executable",
    "transfer_input_files",
    "request_cpus",
    "request_memory",
    "request_disk",
    "requirements",
}
HTC_VALID_JOB_DAG_KEYS = {"pre", "post", "executable"}


class RestrictedDict(MutableMapping):
    """A dictionary that only allows certain keys.

    Parameters
    ----------
    valid_keys : `Container`
        Strings that are valid keys.
    init_data : `dict` or `RestrictedDict`, optional
        Initial data.

    Raises
    ------
    KeyError
        If invalid key(s) in init_data.
    """
    def __init__(self, valid_keys, init_data=()):
        self.valid_keys = valid_keys
        self.data = {}
        self.update(init_data)

    def __getitem__(self, key):
        """Returns value for given key if exists.

        Parameters
        ----------
        key : `str`
            Identifier for value to return.

        Returns
        -------
        value : `Any`
            Value associated with given key.

        Raises
        ------
        KeyError
            If key doesn't exist.
        """
        return self.data[key]

    def __delitem__(self, key):
        """Deletes value for given key if exists.

        Parameters
        ----------
        key : `str`
            Identifier for value to delete.

        Raises
        ------
        KeyError
            If key doesn't exist.
        """
        del self.data[key]

    def __setitem__(self, key, value):
        """Stores key,value in internal dict only if key is valid

        Parameters
        ----------
        key : `str`
            Identifier to associate with given value.
        value : `Any`
            Value to store.

        Raises
        ------
        KeyError
            If key is invalid.
        """
        if key not in self.valid_keys:
            raise KeyError(f"Invalid key {key}")
        self.data[key] = value

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data)


def htc_escape(value):
    """Escape characters in given value based upon HTCondor syntax.

    Parameter
    ----------
    value : `Any`
        Value that needs to have characters escaped if string.

    Returns
    -------
    new_value : `Any`
        Given value with characters escaped appropriate for HTCondor if string.
    """
    if isinstance(value, str):
        newval = value.replace('"', '""').replace("'", "''").replace("&quot;", '"')
    else:
        newval = value

    return newval


def htc_write_attribs(stream, attrs):
    """Write job attributes in HTCondor format to writeable stream.

    Parameters
    ----------
    stream : `TextIOBase`
        Output text stream (typically an open file)
    attrs : `dict`
        HTCondor job attributes (dictionary of attribute key, value)
    """
    for key, value in attrs.items():
        # Make sure strings are syntactically correct for HTCondor.
        if isinstance(value, str):
            pval = f'"{htc_escape(value)}"'
        else:
            pval = value

        print(f"+{key} = {pval}", file=stream)


def htc_write_condor_file(filename, job_name, job, job_attrs):
    """Main function to write an HTCondor submit file.

    Parameters
    ----------
    filename : `str`
        Filename for the HTCondor submit file
    job_name : `str`
        Job name to use in submit file
    job : `RestrictedDict`
        Submit script information.
    job_attrs : `dict`
        Job attributes.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as fh:
        for key, value in job.items():
            if key in HTC_QUOTE_KEYS:
                print(f'{key}="{htc_escape(value)}"', file=fh)
            else:
                print(f"{key}={value}", file=fh)
        for key in ["output", "error", "log"]:
            if key not in job:
                filename = f"{job_name}.$(Cluster).${key[:3]}"
                print(f"{key}={filename}", file=fh)

        if job_attrs is not None:
            htc_write_attribs(fh, job_attrs)
        print("queue", file=fh)


def htc_version():
    """Return the version given by the HTCondor API.

    Returns
    -------
    version : `str`
        HTCondor version as easily comparable string.

    Raises
    -------
    RuntimeError
        Raised if fail to parse htcondor API string.
    """
    # Example string returned by htcondor.version:
    #    $CondorVersion: 8.8.6 Nov 13 2019 BuildID: 489199 PackageID: 8.8.6-1 $
    version_info = re.match(r"\$CondorVersion: (\d+).(\d+).(\d+)", htcondor.version())
    if version_info is None:
        raise RuntimeError("Problems parsing condor version")
    return f"{int(version_info.group(1)):04}.{int(version_info.group(2)):04}.{int(version_info.group(3)):04}"


def htc_submit_dag(htc_dag, submit_options=None):
    """Create DAG submission and submit

    Parameters
    ----------
    htc_dag : `HtcDag`
        DAG to submit to HTCondor
    submit_options : `dict`
        Extra options for condor_submit_dag
    """
    ver = htc_version()
    if ver >= "8.9.3":
        sub = htcondor.Submit.from_dag(htc_dag.graph["dag_filename"], submit_options)
    else:
        sub = _htc_submit_dag_old(htc_dag.graph["dag_filename"], submit_options)

    # add attributes to dag submission
    for key, value in htc_dag.graph["attr"].items():
        sub[f"+{key}"] = f'"{htc_escape(value)}"'

    # submit DAG to HTCondor's schedd
    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        htc_dag.run_id = sub.queue(txn)


def _htc_submit_dag_old(dag_filename, submit_options=None):
    """Call condor_submit_dag on given dag description file.
    (Use until using condor version with htcondor.Submit.from_dag)

    Parameters
    ----------
    dag_filename : `str`
        Name of file containing HTCondor DAG commands.
    submit_options : `dict`, optional
        Contains extra options for command line (Value of None means flag).

    Returns
    -------
    sub : `htcondor.Submit`
        htcondor.Submit object created for submitting the DAG
    """

    # Run command line condor_submit_dag command.
    cmd = "condor_submit_dag -f -no_submit -notification never -autorescue 0 -UseDagDir -no_recurse "

    if submit_options is not None:
        for opt, val in submit_options.items():
            cmd += f" -{opt} {val or ''}"
    cmd += f"{dag_filename}"

    process = subprocess.Popen(
        cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8"
    )
    process.wait()

    if process.returncode != 0:
        print(f"Exit code: {process.returncode}")
        print(process.communicate()[0])
        raise RuntimeError("Problems running condor_submit_dag")

    # Read in the created submit file in order to create submit object.
    sublines = {}
    with open(dag_filename + ".condor.sub", "r") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("#") and not line == "queue":
                (key, val) = re.split(r"\s*=\s*", line, 1)
                # Avoid UserWarning: the line 'copy_to_spool = False' was
                #       unused by Submit object. Is it a typo?
                if key != "copy_to_spool":
                    sublines[key] = val

    sub = htcondor.Submit(sublines)
    return sub


def _htc_write_job_commands(stream, name, jobs):
    """Output the DAGMan job lines for single job in DAG.

    Parameters
    ----------
    stream : `TextIOBase`
        Writeable text stream (typically an opened file).
    name : `str`
        Job name.
    jobs : `RestrictedDict`
        DAG job keys and values.
    """
    if "pre" in jobs:
        print(f"SCRIPT {jobs['pre'].get('defer', '')} PRE {name}"
              f"{jobs['pre']['executable']} {jobs['pre'].get('arguments', '')}", file=stream)

    if "post" in jobs:
        print(f"SCRIPT {jobs['post'].get('defer', '')} PRE {name}"
              f"{jobs['post']['executable']} {jobs['post'].get('arguments', '')}", file=stream)

    if "vars" in jobs:
        for key, value in jobs["vars"]:
            print(f'VARS {name} {key}="{htc_escape(value)}"', file=stream)

    if "pre_skip" in jobs:
        print(f"PRE_SKIP {name} {jobs['pre_skip']}", file=stream)

    if "retry" in jobs:
        print(f"RETRY {name} {jobs['retry']} ", end='', file=stream)
        if "retry_unless_exit" in jobs:
            print(f"UNLESS-EXIT {jobs['retry_unless_exit']}", end='', file=stream)
        print("\n", file=stream)

    if "abort_dag_on" in jobs:
        print(f"ABORT-DAG-ON {name} {jobs['abort_dag_on']['node_exit']}"
              f" RETURN {jobs['abort_dag_on']['abort_exit']}", file=stream)


class HTCJob:
    """HTCondor job for use in building DAG.

    Parameters
    ----------
    name : `str`
        Name of the job
    label : `str`
        Label that can used for grouping or lookup.
    initcmds : `RestrictedDict`
        Initial job commands for submit file.
    initdagcmds : `RestrictedDict`
        Initial commands for job inside DAG.
    initattrs : `dict`
        Initial dictionary of job attributes.
    """
    def __init__(self, name, label=None, initcmds=(), initdagcmds=(), initattrs=None):
        self.name = name
        self.label = label
        self.cmds = RestrictedDict(HTC_VALID_JOB_KEYS, initcmds)
        self.dagcmds = RestrictedDict(HTC_VALID_JOB_DAG_KEYS, initdagcmds)
        self.attrs = initattrs
        self.filename = None
        self.subfile = None

    def __str__(self):
        return self.name

    def add_job_cmds(self, new_commands):
        """Add commands to Job (overwrite existing).

        Parameters
        ----------
        new_commands : `dict`
            Submit file commands to be added to Job.
        """
        self.cmds.update(new_commands)

    def add_dag_cmds(self, new_commands):
        """Add DAG commands to Job (overwrite existing).

        Parameters
        ----------
        new_commands : `dict`
            DAG file commands to be added to Job
        """
        self.dagcmds.update(new_commands)

    def add_job_attrs(self, new_attrs):
        """Add attributes to Job (overwrite existing).

        Parameters
        ----------
        new_attrs : `dict`
            Attributes to be added to Job
        """
        if self.attrs is None:
            self.attrs = {}
        self.attrs.update(new_attrs)

    def write_submit_file(self, submit_path, job_subdir=""):
        """Write job description to submit file.

        Parameters
        ----------
        submit_path : `str`
            Prefix path for the submit file.
        job_subdir : `str`, optional
            Template for job subdir.
        """
        self.subfile = f"{self.name}.sub"
        job_subdir = job_subdir.format(self=self)
        if job_subdir:
            self.subfile = os.path.join(job_subdir, self.subfile)
        htc_write_condor_file(os.path.join(submit_path, self.subfile), self.name, self.cmds, self.attrs)

    def write_dag_commands(self, stream):
        """Write DAG commands for single job to output stream.

        Parameters
        ----------
        stream : `IO` or `str`
            Output Stream
        """
        print(f"JOB {self.name} {self.subfile}", file=stream)
        _htc_write_job_commands(stream, self.name, self.dagcmds)

    def dump(self, fh):
        """Dump job information to output stream.

        Parameters
        ----------
        fh : `TextIOBase`
            Output stream
        """
        printer = pprint.PrettyPrinter(indent=4, stream=fh)
        printer.pprint(self.name)
        printer.pprint(self.cmds)
        printer.pprint(self.attrs)


class HTCDag(networkx.DiGraph):
    """HTCondor DAG.

    Parameters
    ----------
    data : networkx.DiGraph.data
        Initial graph.
    name : `str`
        Name for DAG.
    """
    def __init__(self, data=None, name=""):
        super().__init__(data=data, name=name)

        self.graph["attr"] = dict()
        self.graph["run_id"] = None
        self.graph["submit_path"] = None

    def __str__(self):
        """Represent basic DAG info as string.

        Returns
        -------
        info : `str`
           String containing basic DAG info.
        """
        return f"{self.graph['name']} {len(self)}"

    def add_attribs(self, attribs=None):
        """Add attributes to the DAG.

        Parameters
        ----------
        attribs : `dict`
            DAG attributes
        """
        if attribs is not None:
            self.graph["attr"].update(attribs)

    def add_job(self, job, parent_names=None, child_names=None):
        """Add an HTCJob to the HTCDag.

        Parameters
        ----------
        job : `HTCJob`
            HTCJob to add to the HTCDag
        parent_names : `Iterable` of `str`, optional
            Names of parent jobs
        child_names : `Iterable` of `str`, optional
            Names of child jobs
        """
        assert isinstance(job, HTCJob)
        self.add_node(job.name, data=job)

        if parent_names is not None:
            self.add_job_relationships(parent_names, job.name)

        if child_names is not None:
            self.add_job_relationships(child_names, job.name)

    def add_job_relationships(self, parents, children):
        """Add DAG edge between parents and children jobs.

        Parameters
        ----------
        parents : list of `str`
            Contains parent job name(s).
        children : list of `str`
            Contains children job name(s).
        """
        self.add_edges_from(itertools.product(parents, children))

    def del_job(self, job_name):
        """Delete the job from the DAG.

        Parameters
        ----------
        job_name : `str`
            Name of job in DAG to delete
        """
        # Reconnect edges around node to delete
        parents = self.predecessors(job_name)
        children = self.successors(job_name)
        self.add_edges_from(itertools.product(parents, children))

        # Delete job node (which deletes its edges).
        self.remove_node(job_name)

    def write(self, submit_path, job_subdir=""):
        """Write DAG to a file.

        Parameters
        ----------
        submit_path : `str`
            Prefix path for dag filename to be combined with DAG name.
        job_subdir : `str`, optional
            Template for job subdir.
        """
        self.graph["submit_path"] = submit_path
        self.graph["dag_filename"] = os.path.join(submit_path, f"{self.graph['name']}.dag")
        os.makedirs(submit_path, exist_ok=True)
        with open(self.graph["dag_filename"], "w") as fh:
            for _, nodeval in self.nodes().items():
                job = nodeval["data"]
                job.write_submit_file(submit_path, job_subdir)
                job.write_dag_commands(fh)
            for edge in self.edges():
                print(f"PARENT {edge[0]} CHILD {edge[1]}", file=fh)
            print(f"DOT {self.name}.dot", file=fh)
            print(f"NODE_STATUS_FILE {self.name}.node_status", file=fh)

    def dump(self, fh):
        """Dump DAG info to output stream.

        Parameters
        ----------
        fh : `IO` or `str`
            Where to dump DAG info as text.
        """
        for key, value in self.graph:
            print(f"{key}={value}", file=fh)
        for name, data in self.nodes().items():
            print(f"{name}:", file=fh)
            data.dump(fh)
        for edge in self.edges():
            print(f"PARENT {edge[0]} CHILD {edge[1]}", file=fh)

    def write_dot(self, filename):
        """Write a dot version of the DAG.

        Parameters
        ----------
        filename : `str`
            dot filename
        """
        pos = networkx.nx_agraph.graphviz_layout(self)
        networkx.draw(self, pos=pos)
        networkx.drawing.nx_pydot.write_dot(self, filename)


def condor_q(constraint=None, schedd=None):
    """Query HTCondor for current jobs.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    schedd : `htcondor.Schedd`, optional
        HTCondor scheduler which to query for job information

    Returns
    -------
    jobads : `dict`
        Mapping HTCondor job id to job information
    """
    if not schedd:
        schedd = htcondor.Schedd()
    try:
        joblist = schedd.query(constraint=constraint)
    except RuntimeError as ex:
        raise RuntimeError(f"Problem querying the Schedd.  (Constraint='{constraint}')") from ex

    # convert list to dictionary
    jobads = {}
    for jobinfo in joblist:
        del jobinfo["Environment"]
        del jobinfo["Env"]

        jobads[f"{jobinfo['ClusterId']}.{jobinfo['ProcId']}"] = dict(jobinfo)

    return jobads


def condor_history(constraint=None, schedd=None):
    """Get information about completed jobs from HTCondor history.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    schedd : `htcondor.Schedd`, optional
        HTCondor scheduler which to query for job information

    Returns
    -------
    jobads : `dict`
        Mapping HTCondor job id to job information
    """
    if not schedd:
        schedd = htcondor.Schedd()
    _LOG.debug("condor_history constraint = %s", constraint)
    joblist = schedd.history(constraint, projection=[], match=-1)

    # convert list to dictionary
    jobads = {}
    for jobinfo in joblist:
        del jobinfo["Environment"]
        del jobinfo["Env"]
        jobads[f"{jobinfo['ClusterId']}.{jobinfo['ProcId']}"] = dict(jobinfo)

    _LOG.debug("condor_history returned %d jobs", len(jobads))
    return jobads


def summary_from_dag(dir_name):
    """Build bps_run_summary string from dag file.

    Parameters
    ----------
    dir_name : `str`
        Path that includes dag file for a run.

    Returns
    -------
    summary : `str`
        Semi-colon separated list of job labels and counts.
        (Same format as saved in dag classad.)
    job_name_to_label : `dict` of `str`
        Mapping of job names to job labels
    """
    dag = next(Path(dir_name).glob("*.dag"))

    # Later code depends upon insertion order
    counts = defaultdict(int)
    job_name_to_pipetask = {}
    try:
        with open(dag, "r") as fh:
            for line in fh:
                if line.startswith("JOB"):
                    m = re.match(r"JOB ([^\s]+) jobs/([^/]+)/", line)
                    if m:
                        label = m.group(2)
                        if label == "init":
                            label = "pipetaskInit"
                        job_name_to_pipetask[m.group(1)] = label
                        counts[label] += 1
                    else:   # Check if Pegasus submission
                        m = re.match(r"JOB ([^\s]+) ([^\s]+)", line)
                        if m:
                            label = pegasus_name_to_label(m.group(1))
                            job_name_to_pipetask[m.group(1)] = label
                            counts[label] += 1
                        else:
                            _LOG.warning("Parse DAG: unmatched job line: %s", line)
    except (OSError, PermissionError, StopIteration):
        pass

    summary = ";".join([f"{name}:{counts[name]}" for name in counts])
    _LOG.debug("summary_from_dag: %s %s", summary, job_name_to_pipetask)
    return summary, job_name_to_pipetask


def pegasus_name_to_label(name):
    """Convert pegasus job name to a label for the report.

    Parameters
    ----------
    name : `str`
        Name of job.

    Returns
    -------
    label : `str`
        Label for job.
    """
    label = "UNK"
    if name.startswith("create_dir") or name.startswith("stage_in") or name.startswith("stage_out"):
        label = "pegasus"
    else:
        m = re.match(r"pipetask_(\d+_)?([^_]+)", name)
        if m:
            label = m.group(2)
            if label == "init":
                label = "pipetaskInit"

    return label


def read_dag_status(wms_path):
    """Read the node status file for DAG summary information

    Parameters
    ----------
    path : `str`
        Path that includes node status file for a run.

    Returns
    -------
    dag_classad : `dict`
        DAG summary information.
    """
    dag_classad = {}

    # while this is probably more up to date than dag classad, only read from file if need to.
    try:
        try:
            node_stat_file = next(Path(wms_path).glob("*.node_status"))
            _LOG.debug("Reading Node Status File %s", node_stat_file)
            with open(node_stat_file, "r") as infh:
                dag_classad = classad.parseNext(infh)  # pylint: disable=E1101
        except StopIteration:
            pass

        if not dag_classad:
            # Pegasus check here
            try:
                metrics_file = next(Path(wms_path).glob("*.dag.metrics"))
                with open(metrics_file, "r") as infh:
                    metrics = json.load(infh)
                dag_classad["NodesTotal"] = metrics.get("jobs", 0)
                dag_classad["NodesFailed"] = metrics.get("jobs_failed", 0)
                dag_classad["NodesDone"] = metrics.get("jobs_succeeded", 0)
                dag_classad["pegasus_version"] = metrics.get("planner_version", "")
            except StopIteration:
                try:
                    metrics_file = next(Path(wms_path).glob("*.metrics"))
                    with open(metrics_file, "r") as infh:
                        metrics = json.load(infh)
                    dag_classad["NodesTotal"] = metrics["wf_metrics"]["total_jobs"]
                    dag_classad["pegasus_version"] = metrics.get("version", "")
                except StopIteration:
                    pass
    except (OSError, PermissionError):
        pass

    _LOG.debug("read_dag_status: %s", dag_classad)
    return dict(dag_classad)


def read_node_status(wms_path):
    """Read entire node status file.

    Parameters
    ----------
    wms_path : `str`
        Path that includes node status file for a run.

    Returns
    -------
    dag_classad : `dict` or `classad.ClassAd`
        DAG summary information.
    """
    # Get jobid info from other places to fill in gaps in info from node_status
    _, job_name_to_pipetask = summary_from_dag(wms_path)
    wms_workflow_id, loginfo = read_dag_log(wms_path)
    loginfo = read_dag_nodes_log(wms_path)
    _LOG.debug("loginfo = %s", loginfo)
    job_name_to_id = {}
    for jid, jinfo in loginfo.items():
        if "LogNotes" in jinfo:
            m = re.match(r"DAG Node: ([^\s]+)", jinfo["LogNotes"])
            if m:
                job_name_to_id[m.group(1)] = jid
                jinfo["DAGNodeName"] = m.group(1)

    try:
        node_status = next(Path(wms_path).glob("*.node_status"))
    except StopIteration:
        return loginfo

    jobs = {}
    fake_id = -1.0   # For nodes that do not yet have a job id, give fake one
    try:
        with open(node_status, "r") as fh:
            ads = classad.parseAds(fh)

            for jclassad in ads:
                if jclassad["Type"] == "DagStatus":
                    # skip DAG summary
                    pass
                elif "Node" not in jclassad:
                    if jclassad["Type"] != "StatusEnd":
                        _LOG.debug("Key 'Node' not in classad: %s", jclassad)
                    break
                else:
                    if jclassad["Node"] in job_name_to_pipetask:
                        try:
                            label = job_name_to_pipetask[jclassad["Node"]]
                        except KeyError:
                            _LOG.error("%s not in %s", jclassad["Node"], job_name_to_pipetask.keys())
                            raise
                    elif "_" in jclassad["Node"]:
                        label = jclassad["Node"].split("_")[1]
                    else:
                        label = jclassad["Node"]

                    # Make job info as if came from condor_q
                    if jclassad["Node"] in job_name_to_id:
                        job_id = job_name_to_id[jclassad["Node"]]
                    else:
                        job_id = str(fake_id)
                        fake_id -= 1

                    job = dict(jclassad)
                    job["ClusterId"] = int(float(job_id))
                    job["DAGManJobID"] = wms_workflow_id
                    job["DAGNodeName"] = jclassad["Node"]
                    job["bps_job_label"] = label

                    jobs[str(job_id)] = job
    except (OSError, PermissionError):
        pass

    return jobs


def read_dag_log(wms_path):
    """Read job information from the DAGMan log file.

    Parameters
    ----------
    wms_path : `str`
        Path containing the DAGMan log file.

    Returns
    -------
    info : `dict`
        HTCondor job information read from the log file mapped to HTCondor
        job id.

    Raises
    ------
    StopIteration
        If cannot find dagman log file in given wms_path.
    """
    wms_workflow_id = 0
    dag_info = {}

    path = Path(wms_path)
    if path.exists():
        filename = next(path.glob("*.dag.dagman.log"))
        _LOG.debug("dag node log filename: %s", filename)

        info = {}
        job_event_log = htcondor.JobEventLog(str(filename))
        for event in job_event_log.events(stop_after=0):
            id_ = f"{event['Cluster']}.{event['Proc']}"
            if id_ not in info:
                info[id_] = {}
                wms_workflow_id = id_   # taking last job id in case of restarts
            info[id_].update(event)
            info[id_][f"{event.type.name.lower()}_time"] = event["EventTime"]

        # only save latest DAG job
        dag_info = {wms_workflow_id: info[wms_workflow_id]}
        for job in dag_info.values():
            _tweak_log_info(filename, job)

    return wms_workflow_id, dag_info


def read_dag_nodes_log(wms_path):
    """Read job information from the DAGMan nodes log file.

    Parameters
    ----------
    wms_path : `str`
        Path containing the DAGMan nodes log file.

    Returns
    -------
    info : `dict`
        HTCondor job information read from the log file mapped to HTCondor
        job id.
    """
    filename = next(Path(wms_path).glob("*.dag.nodes.log"))
    _LOG.debug("dag node log filename: %s", filename)
    job_event_log = htcondor.JobEventLog(str(filename))
    info = {}
    for event in job_event_log.events(stop_after=0):
        id_ = f"{event['Cluster']}.{event['Proc']}"
        if id_ not in info:
            info[id_] = {}
        info[id_].update(event)
        info[id_][f"{event.type.name.lower()}_time"] = event["EventTime"]

    # add more condor_q-like info to info parsed from log file
    for job in info.values():
        _tweak_log_info(filename, job)
    return info


def _tweak_log_info(filename, job):
    _LOG.debug("_tweak_log_info: %s %s", filename, job)
    try:
        job["ClusterId"] = job["Cluster"]
        job["Iwd"] = str(filename.parent)
        job["Owner"] = filename.owner()
        if job["MyType"] == "ExecuteEvent":
            job["JobStatus"] = JobStatus.RUNNING
        elif job["MyType"] == "JobTerminatedEvent" or job["MyType"] == "PostScriptTerminatedEvent":
            job["JobStatus"] = JobStatus.COMPLETED
            try:
                if not job["TerminatedNormally"]:
                    if "ReturnValue" in job:
                        job["ExitCode"] = job["ReturnValue"]
                        job["ExitBySignal"] = False
                    elif "TerminatedBySignal" in job:
                        job["ExitBySignal"] = True
                        job["ExitSignal"] = job["TerminatedBySignal"]
                    else:
                        _LOG.warning("Could not determine exit status for completed job: %s", job)
            except KeyError as ex:
                _LOG.error("Could not determine exit status for job (missing %s): %s", str(ex), job)
        elif job["MyType"] == "SubmitEvent":
            job["JobStatus"] = JobStatus.IDLE
        else:
            _LOG.debug("Unknown log event type: %s", job["MyType"])
    except KeyError:
        _LOG.error("Missing key in job: %s", job)
        raise


def htc_check_dagman_output(wms_path):
    """Check the DAGman output for error messages.

    Parameter
    ----------
    wms_path : `str`
        Directory containing the DAGman output file.

    Returns
    -------
    message : `str`
        Message containing error messages from the DAGman output.  Empty string if
        no messages.
    """
    message = ""
    filename = next(Path(wms_path).glob("*.dag.dagman.out"))

    try:
        _LOG.debug("dag output filename: %s", filename)
        with open(filename, "r") as fh:
            last_submit_failed = ""
            for line in fh:
                m = re.match(r"(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) Job submit try \d+/\d+ failed", line)
                if m:
                    last_submit_failed = m.group(1)
        if last_submit_failed:
            message = f"Warn: Job submission issues (last: {last_submit_failed})"
    except (IOError, PermissionError):
        message = f"Warn: Could not read dagman output file from {wms_path}."
    return message
