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

"""Class definitions for a Generic Workflow Graph.
"""

__all__ = ["GenericWorkflow", "GenericWorkflowFile", "GenericWorkflowJob", "GenericWorkflowExec"]


import dataclasses
import itertools
import logging
import pickle
from collections import Counter, defaultdict
from typing import Optional

from lsst.utils.iteration import ensure_iterable
from networkx import DiGraph, topological_sort
from networkx.algorithms.dag import is_directed_acyclic_graph

from .bps_draw import draw_networkx_dot

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class GenericWorkflowFile:
    """Information about a file that may be needed by various workflow
    management services.
    """

    name: str
    """Lookup key (logical file name) of file/directory. Must be unique
    within run.
    """

    src_uri: str or None  # don't know that need ResourcePath
    """Original location of file/directory.
    """

    wms_transfer: bool
    """Whether the WMS should ignore file or not.  Default is False.
    """

    job_access_remote: bool
    """Whether the job can remotely access file (using separately specified
    file access protocols).  Default is False.
    """

    job_shared: bool
    """Whether job requires its own copy of this file.  Default is False.
    """

    # As of python 3.7.8, can't use __slots__ + dataclass if give default
    # values, so writing own __init__.
    def __init__(
        self,
        name: str,
        src_uri: str = None,
        wms_transfer: bool = False,
        job_access_remote: bool = False,
        job_shared: bool = False,
    ):
        self.name = name
        self.src_uri = src_uri
        self.wms_transfer = wms_transfer
        self.job_access_remote = job_access_remote
        self.job_shared = job_shared

    __slots__ = ("name", "src_uri", "wms_transfer", "job_access_remote", "job_shared")

    def __hash__(self):
        return hash(self.name)


@dataclasses.dataclass
class GenericWorkflowExec:
    """Information about an executable that may be needed by various workflow
    management services.
    """

    name: str
    """Lookup key (logical file name) of executable. Must be unique
    within run.
    """

    src_uri: str or None  # don't know that need ResourcePath
    """Original location of executable.
    """

    transfer_executable: bool
    """Whether the WMS/plugin is responsible for staging executable to
    location usable by job.
    """

    # As of python 3.7.8, can't use __slots__ + dataclass if give default
    # values, so writing own __init__.
    def __init__(self, name: str, src_uri: str = None, transfer_executable: bool = False):
        self.name = name
        self.src_uri = src_uri
        self.transfer_executable = transfer_executable

    __slots__ = ("name", "src_uri", "transfer_executable")

    def __hash__(self):
        return hash(self.name)


@dataclasses.dataclass
class GenericWorkflowJob:
    """Information about a job that may be needed by various workflow
    management services.
    """

    name: str
    """Name of job.  Must be unique within workflow.
    """

    label: Optional[str]
    """Primary user-facing label for job.  Does not need to be unique
    and may be used for summary reports.
    """

    quanta_counts: Optional[Counter]
    """Counts of quanta per task label in job.
    """

    tags: Optional[dict]
    """Other key/value pairs for job that user may want to use as a filter.
    """

    executable: Optional[GenericWorkflowExec]
    """Executable for job.
    """

    arguments: Optional[str]
    """Command line arguments for job.
    """

    cmdvals: Optional[dict]
    """Values for variables in cmdline when using lazy command line creation.
    """

    memory_multiplier: Optional[float]
    """Memory growth rate between retries.
    """

    request_memory: Optional[int]  # MB
    """Max memory (in MB) that the job is expected to need.
    """

    request_memory_max: Optional[int]  # MB
    """Max memory (in MB) that the job should ever use.
    """

    request_cpus: Optional[int]  # cores
    """Max number of cpus that the job is expected to need.
    """

    request_disk: Optional[int]  # MB
    """Max amount of job scratch disk (in MB) that the job is expected to need.
    """

    request_walltime: Optional[str]  # minutes
    """Max amount of time (in seconds) that the job is expected to need.
    """

    compute_site: Optional[str]
    """Key to look up site-specific information for running the job.
    """

    accounting_group: Optional[str]
    """Name of the accounting group to use.
    """

    accounting_user: Optional[str]
    """Name of the user to use for accounting purposes.
    """

    mail_to: Optional[str]
    """Comma separated list of email addresses for emailing job status.
    """

    when_to_mail: Optional[str]
    """WMS-specific terminology for when to email job status.
    """

    number_of_retries: Optional[int]
    """Number of times to automatically retry a failed job.
    """

    retry_unless_exit: Optional[int]
    """Exit code for job that means to not automatically retry.
    """

    abort_on_value: Optional[int]
    """Job exit value for signals to abort the entire workflow.
    """

    abort_return_value: Optional[int]
    """Exit value to use when aborting the entire workflow.
    """

    priority: Optional[str]
    """Initial priority of job in WMS-format.
    """

    category: Optional[str]
    """WMS-facing label of job within single workflow (e.g., can be used for
    throttling jobs within a single workflow).
    """

    concurrency_limit: Optional[str]
    """Names of concurrency limits that the WMS plugin can appropriately
    translate to limit the number of this job across all running workflows.
    """

    queue: Optional[str]
    """Name of queue to use. Different WMS can translate this concept
    differently.
    """

    pre_cmdline: Optional[str]
    """Command line to be executed prior to executing job.
    """

    post_cmdline: Optional[str]
    """Command line to be executed after job executes.

    Should be executed regardless of exit status.
    """

    preemptible: Optional[bool]
    """The flag indicating whether the job can be preempted.
    """

    profile: Optional[dict]
    """Nested dictionary of WMS-specific key/value pairs with primary key being
    WMS key (e.g., pegasus, condor, panda).
    """

    attrs: Optional[dict]
    """Key/value pairs of job attributes (for WMS that have attributes in
    addition to commands).
    """

    environment: Optional[dict]
    """Environment variable names and values to be explicitly set inside job.
    """

    compute_cloud: Optional[str]
    """Key to look up cloud-specific information for running the job.
    """

    # As of python 3.7.8, can't use __slots__ if give default values, so
    # writing own __init__.
    def __init__(self, name: str):
        self.name = name
        self.label = None
        self.quanta_counts = Counter()
        self.tags = {}
        self.executable = None
        self.arguments = None
        self.cmdvals = {}
        self.memory_multiplier = None
        self.request_memory = None
        self.request_memory_max = None
        self.request_cpus = None
        self.request_disk = None
        self.request_walltime = None
        self.compute_site = None
        self.accounting_group = None
        self.accounting_user = None
        self.mail_to = None
        self.when_to_mail = None
        self.number_of_retries = None
        self.retry_unless_exit = None
        self.abort_on_value = None
        self.abort_return_value = None
        self.priority = None
        self.category = None
        self.concurrency_limit = None
        self.queue = None
        self.pre_cmdline = None
        self.post_cmdline = None
        self.preemptible = None
        self.profile = {}
        self.attrs = {}
        self.environment = {}
        self.compute_cloud = None

    __slots__ = (
        "name",
        "label",
        "quanta_counts",
        "tags",
        "mail_to",
        "when_to_mail",
        "executable",
        "arguments",
        "cmdvals",
        "memory_multiplier",
        "request_memory",
        "request_memory_max",
        "request_cpus",
        "request_disk",
        "request_walltime",
        "number_of_retries",
        "retry_unless_exit",
        "abort_on_value",
        "abort_return_value",
        "compute_site",
        "accounting_group",
        "accounting_user",
        "environment",
        "priority",
        "category",
        "concurrency_limit",
        "queue",
        "pre_cmdline",
        "post_cmdline",
        "preemptible",
        "profile",
        "attrs",
        "compute_cloud",
    )

    def __hash__(self):
        return hash(self.name)


class GenericWorkflow(DiGraph):
    """A generic representation of a workflow used to submit to specific
    workflow management systems.

    Parameters
    ----------
    name : `str`
        Name of generic workflow.
    incoming_graph_data : `Any`, optional
        Data used to initialized graph that is passed through to DiGraph
        constructor.  Can be any type supported by networkx.DiGraph.
    attr : `dict`
        Keyword arguments passed through to DiGraph constructor.
    """

    def __init__(self, name, incoming_graph_data=None, **attr):
        super().__init__(incoming_graph_data, **attr)
        self._name = name
        self.run_attrs = {}
        self._files = {}
        self._executables = {}
        self._inputs = {}  # mapping job.names to list of GenericWorkflowFile
        self._outputs = {}  # mapping job.names to list of GenericWorkflowFile
        self._labels = defaultdict(list)  # mapping job label to list of GenericWorkflowJob
        self.run_id = None
        self._final = None

    @property
    def name(self):
        """Retrieve name of generic workflow.

        Returns
        -------
        name : `str`
            Name of generic workflow.
        """
        return self._name

    @property
    def quanta_counts(self):
        """Count of quanta per task label (`collections.Counter`)."""
        qcounts = Counter()
        for job_name in self:
            gwjob = self.get_job(job_name)
            if gwjob.quanta_counts is not None:
                qcounts += gwjob.quanta_counts
        return qcounts

    @property
    def labels(self):
        """List of job labels (`list` [`str`], read-only)"""
        return list(self._labels.keys())

    def regenerate_labels(self):
        """Regenerate the list of job labels."""
        self._labels = defaultdict(list)
        for job_name in self:
            job = self.get_job(job_name)
            self._labels[job.label].append(job)

    @property
    def job_counts(self):
        """Count of jobs per job label (`collections.Counter`)."""
        jcounts = Counter({label: len(jobs) for label, jobs in self._labels.items()})

        # Final is separate
        final = self.get_final()
        if final:
            if isinstance(final, GenericWorkflow):
                jcounts.update(final.job_counts())
            else:
                jcounts[final.label] += 1

        return jcounts

    def __iter__(self):
        """Return iterator of job names in topologically sorted order."""
        return topological_sort(self)

    def get_files(self, data=False, transfer_only=True):
        """Retrieve files from generic workflow.

        Need API in case change way files are stored (e.g., make
        workflow a bipartite graph with jobs and files nodes).

        Parameters
        ----------
        data : `bool`, optional
            Whether to return the file data as well as the file object name.
            (The defaults is False.)
        transfer_only : `bool`, optional
            Whether to only return files for which a workflow management system
            would be responsible for transferring.

        Returns
        -------
        files : `list` [`lsst.ctrl.bps.GenericWorkflowFile`] or `list` [`str`]
            File names or objects from generic workflow meeting specifications.
        """
        files = []
        for filename, file in self._files.items():
            if not transfer_only or file.wms_transfer:
                if not data:
                    files.append(filename)
                else:
                    files.append(file)
        return files

    def add_job(self, job, parent_names=None, child_names=None):
        """Add job to generic workflow.

        Parameters
        ----------
        job : `lsst.ctrl.bps.GenericWorkflowJob`
            Job to add to the generic workflow.
        parent_names : `list` [`str`], optional
            Names of jobs that are parents of given job
        child_names : `list` [`str`], optional
            Names of jobs that are children of given job
        """
        if not isinstance(job, GenericWorkflowJob):
            raise RuntimeError(f"Invalid type for job to be added to GenericWorkflowGraph ({type(job)}).")
        if self.has_node(job.name):
            raise RuntimeError(f"Job {job.name} already exists in GenericWorkflowGraph.")
        super().add_node(job.name, job=job)
        self.add_job_relationships(parent_names, job.name)
        self.add_job_relationships(job.name, child_names)
        self.add_executable(job.executable)
        self._labels[job.label].append(job)

    def add_node(self, node_for_adding, **attr):
        """Override networkx function to call more specific add_job function.

        Parameters
        ----------
        node_for_adding : `lsst.ctrl.bps.GenericWorkflowJob`
            Job to be added to generic workflow.
        attr :
            Needed to match original networkx function, but not used.
        """
        self.add_job(node_for_adding)

    def add_job_relationships(self, parents, children):
        """Add dependencies between parent and child jobs.  All parents will
        be connected to all children.

        Parameters
        ----------
        parents : `list` [`str`]
            Parent job names.
        children : `list` [`str`]
            Children job names.
        """
        if parents is not None and children is not None:
            self.add_edges_from(itertools.product(ensure_iterable(parents), ensure_iterable(children)))

    def add_edges_from(self, ebunch_to_add, **attr):
        """Add several edges between jobs in the generic workflow.

        Parameters
        ----------
        ebunch_to_add : Iterable [`tuple`]
            Iterable of job name pairs between which a dependency should be
            saved.
        attr : keyword arguments, optional
            Data can be assigned using keyword arguments (not currently used).
        """
        for edge_to_add in ebunch_to_add:
            self.add_edge(edge_to_add[0], edge_to_add[1], **attr)

    def add_edge(self, u_of_edge: str, v_of_edge: str, **attr):
        """Add edge connecting jobs in workflow.

        Parameters
        ----------
        u_of_edge : `str`
            Name of parent job.
        v_of_edge : `str`
            Name of child job.
        attr : keyword arguments, optional
            Attributes to save with edge.
        """
        if u_of_edge not in self:
            raise RuntimeError(f"{u_of_edge} not in GenericWorkflow")
        if v_of_edge not in self:
            raise RuntimeError(f"{v_of_edge} not in GenericWorkflow")
        super().add_edge(u_of_edge, v_of_edge, **attr)

    def get_job(self, job_name: str):
        """Retrieve job by name from workflow.

        Parameters
        ----------
        job_name : `str`
            Name of job to retrieve.

        Returns
        -------
        job : `lsst.ctrl.bps.GenericWorkflowJob`
            Job matching given job_name.
        """
        return self.nodes[job_name]["job"]

    def del_job(self, job_name: str):
        """Delete job from generic workflow leaving connected graph.

        Parameters
        ----------
        job_name : `str`
            Name of job to delete from workflow.
        """
        job = self.get_job(job_name)
        self._labels[job.label].remove(job)
        # Don't leave keys around if removed last job
        if not self._labels[job.label]:
            del self._labels[job.label]

        # Connect all parent jobs to all children jobs.
        parents = self.predecessors(job_name)
        children = self.successors(job_name)
        self.add_job_relationships(parents, children)

        # Delete job node (which deletes edges).
        self.remove_node(job_name)

    def add_job_inputs(self, job_name, files):
        """Add files as inputs to specified job.

        Parameters
        ----------
        job_name : `str`
            Name of job to which inputs should be added
        files : `lsst.ctrl.bps.GenericWorkflowFile` or \
                `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            File object(s) to be added as inputs to the specified job.
        """
        self._inputs.setdefault(job_name, [])
        for file in ensure_iterable(files):
            # Save the central copy
            if file.name not in self._files:
                self._files[file.name] = file

            # Save the job reference to the file
            self._inputs[job_name].append(file)

    def get_file(self, name):
        """Retrieve a file object by name.

        Parameters
        ----------
        name : `str`
            Name of file object

        Returns
        -------
        gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
            File matching given name.
        """
        return self._files[name]

    def add_file(self, gwfile):
        """Add file object.

        Parameters
        ----------
        gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
            File object to add to workflow
        """
        if gwfile.name not in self._files:
            self._files[gwfile.name] = gwfile
        else:
            _LOG.debug("Skipped add_file for existing file %s", gwfile.name)

    def get_job_inputs(self, job_name, data=True, transfer_only=False):
        """Return the input files for the given job.

        Parameters
        ----------
        job_name : `str`
            Name of the job.
        data : `bool`, optional
            Whether to return the file data as well as the file object name.
        transfer_only : `bool`, optional
            Whether to only return files for which a workflow management system
            would be responsible for transferring.

        Returns
        -------
        inputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            Input files for the given job.  If no input files for the job,
            returns an empty list.
        """
        inputs = []
        if job_name in self._inputs:
            for gwfile in self._inputs[job_name]:
                if not transfer_only or gwfile.wms_transfer:
                    if not data:
                        inputs.append(gwfile.name)
                    else:
                        inputs.append(gwfile)
        return inputs

    def add_job_outputs(self, job_name, files):
        """Add output files to a job.

        Parameters
        ----------
        job_name : `str`
            Name of job to which the files should be added as outputs.
        files : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            File objects to be added as outputs for specified job.
        """
        self._outputs.setdefault(job_name, [])

        for file_ in ensure_iterable(files):
            # Save the central copy
            if file_.name not in self._files:
                self._files[file_.name] = file_

            # Save the job reference to the file
            self._outputs[job_name].append(file_)

    def get_job_outputs(self, job_name, data=True, transfer_only=False):
        """Return the output files for the given job.

        Parameters
        ----------
        job_name : `str`
            Name of the job.
        data : `bool`
            Whether to return the file data as well as the file object name.
            It defaults to `True` thus returning file data as well.
        transfer_only : `bool`
            Whether to only return files for which a workflow management system
            would be responsible for transferring.  It defaults to `False` thus
            returning all output files.

        Returns
        -------
        outputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            Output files for the given job. If no output files for the job,
            returns an empty list.
        """
        outputs = []

        if job_name in self._outputs:
            for file_name in self._outputs[job_name]:
                file = self._files[file_name]
                if not transfer_only or file.wms_transfer:
                    if not data:
                        outputs.append(file_name)
                    else:
                        outputs.append(self._files[file_name])
        return outputs

    def draw(self, stream, format_="dot"):
        """Output generic workflow in a visualization format.

        Parameters
        ----------
        stream : `str` or `io.BufferedIOBase`
            Stream to which the visualization should be written.
        format_ : `str`, optional
            Which visualization format to use.  It defaults to the format for
            the dot program.
        """
        draw_funcs = {"dot": draw_networkx_dot}
        if format_ in draw_funcs:
            draw_funcs[format_](self, stream)
        else:
            raise RuntimeError(f"Unknown draw format ({format_}")

    def save(self, stream, format_="pickle"):
        """Save the generic workflow in a format that is loadable.

        Parameters
        ----------
        stream : `str` or `io.BufferedIOBase`
            Stream to pass to the format-specific writer.  Accepts anything
            that the writer accepts.

        format_ : `str`, optional
            Format in which to write the data. It defaults to pickle format.
        """
        if format_ == "pickle":
            pickle.dump(self, stream)
        else:
            raise RuntimeError(f"Unknown format ({format_})")

    @classmethod
    def load(cls, stream, format_="pickle"):
        """Load a GenericWorkflow from the given stream

        Parameters
        ----------
        stream : `str` or `io.BufferedIOBase`
            Stream to pass to the format-specific loader. Accepts anything that
            the loader accepts.
        format_ : `str`, optional
            Format of data to expect when loading from stream.  It defaults
            to pickle format.

        Returns
        -------
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
            Generic workflow loaded from the given stream
        """
        if format_ == "pickle":
            return pickle.load(stream)

        raise RuntimeError(f"Unknown format ({format_})")

    def validate(self):
        """Run checks to ensure that the generic workflow graph is valid."""
        # Make sure a directed acyclic graph
        assert is_directed_acyclic_graph(self)

    def add_workflow_source(self, workflow):
        """Add given workflow as new source to this workflow.

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.GenericWorkflow`
        """
        # Find source nodes in self.
        self_sources = [n for n in self if self.in_degree(n) == 0]
        _LOG.debug("self_sources = %s", self_sources)

        # Find sink nodes of workflow.
        new_sinks = [n for n in workflow if workflow.out_degree(n) == 0]
        _LOG.debug("new sinks = %s", new_sinks)

        # Add new workflow nodes to self graph and make new edges.
        self.add_nodes_from(workflow.nodes(data=True))
        self.add_edges_from(workflow.edges())
        for source in self_sources:
            for sink in new_sinks:
                self.add_edge(sink, source)

        # Files are stored separately so copy them.
        for job_name in workflow:
            self.add_job_inputs(job_name, workflow.get_job_inputs(job_name, data=True))
            self.add_job_outputs(job_name, workflow.get_job_outputs(job_name, data=True))
            self.add_executable(workflow.get_job(job_name).executable)

        # Note: label ordering inferred from dict order
        #       so adding given source workflow first
        labels = defaultdict(list)
        for label in workflow._labels:
            labels[label] = workflow._labels[label]
        for label in self._labels:
            labels[label] = self._labels[label]
        self._labels = labels

    def add_final(self, final):
        """Add special final job/workflow to the generic workflow.

        Parameters
        ----------
        final : `lsst.ctrl.bps.GenericWorkflowJob` or \
                `lsst.ctrl.bps.GenericWorkflow`
            Information needed to execute the special final job(s), the
            job(s) to be executed after all jobs that can be executed
            have been executed regardless of exit status of any of the
            jobs.
        """
        if not isinstance(final, GenericWorkflowJob) and not isinstance(final, GenericWorkflow):
            raise TypeError("Invalid type for GenericWorkflow final ({type(final)})")

        self._final = final
        if isinstance(final, GenericWorkflowJob):
            self.add_executable(final.executable)

    def get_final(self):
        """Return job/workflow to be executed after all jobs that can be
        executed have been executed regardless of exit status of any of
        the jobs.

        Returns
        -------
        final : `lsst.ctrl.bps.GenericWorkflowJob` or \
                `lsst.ctrl.bps.GenericWorkflow`
            Information needed to execute final job(s).
        """
        return self._final

    def add_executable(self, executable):
        """Add executable to workflow's list of executables.

        Parameters
        ----------
        executable : `lsst.ctrl.bps.GenericWorkflowExec`
            Executable object to be added to workflow.
        """
        if executable is not None:
            self._executables[executable.name] = executable
        else:
            _LOG.warning("executable not specified (None); cannot add to the workflow's list of executables")

    def get_executables(self, data=False, transfer_only=True):
        """Retrieve executables from generic workflow.

        Parameters
        ----------
        data : `bool`, optional
            Whether to return the executable data as well as the exec object
            name.  (The defaults is False.)
        transfer_only : `bool`, optional
            Whether to only return executables for which transfer_executable
            is True.

        Returns
        -------
        execs : `list` [`lsst.ctrl.bps.GenericWorkflowExec`] or `list` [`str`]
            Filtered executable names or objects from generic workflow.
        """
        execs = []
        for name, executable in self._executables.items():
            if not transfer_only or executable.transfer_executable:
                if not data:
                    execs.append(name)
                else:
                    execs.append(executable)
        return execs

    def get_jobs_by_label(self, label: str):
        """Retrieve jobs by label from workflow.

        Parameters
        ----------
        label : `str`
            Label of jobs to retrieve.

        Returns
        -------
        jobs : list[`lsst.ctrl.bps.GenericWorkflowJob`]
            Jobs having given label.
        """
        return self._labels[label]
