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

__all__ = ["DagStatus", "JobStatus", "RestrictedDict", "HTCJob", "HTCDag", "htc_escape", "htc_write_attribs",
           "htc_write_condor_file", "htc_version", "htc_submit_dag", "htc_submit_dag_old",
           "htc_write_job_commands", "htc_job_status_to_wms_state", "htc_jobs_to_wms_report", "condor_q",
           "condor_history", "save_node_status", "read_node_status", "read_dag_node_log"]

import itertools
import glob
import os
import re
import json
import logging
from collections.abc import MutableMapping
from enum import IntEnum
import pprint
import subprocess
import networkx
import htcondor
import classad

from ...wms_service import WmsRunReport, WmsJobReport, WmsStates

_LOG = logging.getLogger()


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


HTC_QUOTE_KEYS = {"environment"}
HTC_VALID_JOB_KEYS = {
    "universe",
    "executable",
    "arguments",
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
        Value that needs to have characters escaped if string

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
        if isinstance(value, str):
            pval = f'"{htc_escape(value)}"'
        else:
            pval = value

        print(f'+{key} = {pval}', file=stream)


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
        sub = htc_submit_dag_old(htc_dag.graph["dag_filename"], submit_options)

    # add attributes to dag submission
    for key, value in htc_dag.graph["attr"].items():
        sub[f"+{key}"] = f'"{htc_escape(value)}"'

    # submit DAG to HTCondor's schedd
    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        htc_dag.run_id = sub.queue(txn)


def htc_submit_dag_old(dag_filename, submit_options=None):
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


def htc_write_job_commands(stream, name, jobs):
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
        htc_write_job_commands(stream, self.name, self.dagcmds)

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


def htc_job_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict`
        HTCondor job information.  (Need more than just status if completed.)

    Returns
    -------
    wms_state : `WmsStates`
        The equivalent WmsState to given job's status.
    """
    job_status = int(job['JobStatus'])
    wms_state = WmsStates.MISFIT

    _LOG.debug("htc_job_status_to_wms_state: job_status = %s", job_status)
    if job_status == JobStatus.IDLE:
        wms_state = WmsStates.PENDING
    elif job_status == JobStatus.RUNNING:
        wms_state = WmsStates.RUNNING
    elif job_status == JobStatus.REMOVED:
        wms_state = WmsStates.DELETED
    elif job_status == JobStatus.COMPLETED:
        if job['ExitBySignal'] or job['ExitCode'] or job.get('ExitSignal', 0) or job.get('DAG_Status', 0):
            wms_state = WmsStates.FAILED
        else:
            wms_state = WmsStates.SUCCEEDED
    elif job_status == JobStatus.HELD:
        wms_state = WmsStates.HELD

    return wms_state


def htc_jobs_to_wms_report(jobs):
    """Convert HTCondor job information to WmsRunReport information.

    Parameters
    ----------
    jobs : `dict`
        Mapping of HTCondor job id to HTCondor job info

    Returns
    -------
    runs : `dict` of `~lsst.ctrl.bps.wms_service.WmsRunReport`
        Information about runs from given job information.
    """
    runs = {}
    for job_id in sorted(jobs):
        job = jobs[job_id]
        owner = job.get("bps_operator", None)
        if not owner:
            owner = job["Owner"]
        if 'DAGManJobID' not in job:
            # Is a dagman job and is the top job in the run
            report = WmsRunReport(wms_id=job['ClusterID'],
                                  path=job['Iwd'],
                                  label=job.get('bps_job_label', "MISS"),
                                  run=job.get('bps_run', "MISS"),
                                  project=job.get('bps_project', "MISS"),
                                  campaign=job.get('bps_campaign', "MISS"),
                                  payload=job.get('bps_payload', "MISS"),
                                  operator=owner,
                                  run_summary=job.get('bps_run_summary', None),
                                  state=htc_job_status_to_wms_state(job),
                                  jobs=[],
                                  total_number_jobs=0,
                                  job_state_counts={})

            save_node_status(report, job)
            runs[job['ClusterID']] = report
        else:
            report = WmsJobReport(job['ClusterID'], job['DAGNodeName'],
                                  job.get('bps_job_label', job['DAGNodeName']),
                                  htc_job_status_to_wms_state(job))
            try:
                runs[job['DAGManJobID']].jobs.append(report)
            except KeyError:
                # Temporary fix in case history constraint quit in middle of run
                # Should change main code to first query DAG jobs then query their jobs
                missing_job = condor_history(f"(ClusterID == {job['DAGManJobID']})")
                missing_run = htc_jobs_to_wms_report(missing_job)
                runs[missing_run[0].wms_id] = missing_run[0]
                try:
                    runs[job['DAGManJobID']].jobs.append(report)
                except KeyError:
                    raise KeyError(f"Missing dagman job for job {job['ClusterID']}.  Double check "
                                   f"report constraints.") from None

    _LOG.debug(list(runs.values()))
    return list(runs.values())


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
        del jobinfo['Environment']
        jobads[jobinfo['ClusterId']] = jobinfo

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
        del jobinfo['Environment']
        jobads[jobinfo['ClusterId']] = jobinfo

    _LOG.debug("condor_history returned %d jobs", len(jobads))
    return jobads


def save_node_status(run_report, node_status):
    """Save node status information to the run_report

    Parameters
    ----------
    run_report : `~lsst.ctrl.bps.wms_service.WmsRunReport`
        Where to save the node status information.
    node_status : `dict` or `classad.ClassAd`
        HTCondor node status information.
    """
    if 'DAG_NodesReady' not in node_status:
        node_status = read_node_status(node_status['Iwd'])
        _LOG.debug("node_status from file: %s", node_status)

    run_report.job_state_counts = {
        WmsStates.UNREADY: node_status.get('DAG_NodesUnready', 0),
        WmsStates.READY: node_status.get('DAG_NodesReady', 0),
        WmsStates.HELD: node_status.get('JobProcsHeld', 0),
        WmsStates.SUCCEEDED: node_status.get('DAG_NodesDone', 0),
        WmsStates.FAILED: node_status.get('DAG_NodesFailed', 0),
        WmsStates.MISFIT: node_status.get('DAG_NodesPre', 0) + node_status.get('DAG_NodesPost', 0)}

    _LOG.debug("job_state_counts: %s", run_report.job_state_counts)

    # Fill in missing states
    for state in WmsStates:
        if state not in run_report.job_state_counts:
            run_report.job_state_counts[state] = 0

    run_report.total_number_jobs = node_status.get('DAG_NodesTotal', node_status.get('NodesTotal', 0))


def read_node_status(path):
    """Read the node status file for DAG summary information

    Parameters
    ----------
    path : `str`
        Path that includes node status file for a run.

    Returns
    -------
    dag_classad : `dict` or `classad.ClassAd`
        DAG summary information.
    """
    dag_classad = {}

    # while this is probably more up to date than dag classad, only read from file if need to.
    node_stat_files = glob.glob(os.path.join(path, "*.node_status"))
    if node_stat_files:
        _LOG.debug("Reading Node Status File %s", node_stat_files[0])
        try:
            with open(node_stat_files[0], 'r') as infh:
                dag_classad = classad.parseNext(infh)  # pylint: disable=E1101
        except OSError:
            pass

        # inside normal classads they are prefaced with DAG_
        for key in dag_classad:
            if key.startswith("Nodes"):
                dag_classad[f"DAG_{key}"] = dag_classad[key]
    else:
        _LOG.debug("Didn't find a *.node_status file in %s", path)

    if not dag_classad:
        # Pegasus check here
        try:
            metrics_files = glob.glob(os.path.join(path, "*.dag.metrics"))
            if metrics_files:
                with open(metrics_files[0], "r") as infh:
                    metrics = json.load(infh)
                dag_classad["NodesTotal"] = metrics.get("jobs", 0)
                dag_classad["NodesFailed"] = metrics.get("jobs_failed", 0)
                dag_classad["NodesDone"] = metrics.get("jobs_succeeded", 0)
        except OSError:
            pass

    return dag_classad


def read_dag_node_log(filename):
    """Read job information from the DAGMan log file.

    Parameters
    ----------
    filename : `str`
        Name of the DAGMan log file.

    Returns
    -------
    info : `dict`
        HTCondor job information read from the log file mapped to HTCondor
        job id.
    """
    if not filename.endswith(".log"):
        filename = glob.glob(os.path.join(filename, "*.dag.nodes.log"))[-1]

    _LOG.debug("dag node log filename: %s", filename)
    job_event_log = htcondor.JobEventLog(filename)
    info = {}
    for event in job_event_log.events(stop_after=0):
        id_ = f"{event['Cluster']}.{event['Proc']}"
        if id_ not in info:
            info[id_] = {}
        info[id_].update(event)
        info[id_][f"{event.type.name.lower()}_time"] = event["EventTime"]
    return info
