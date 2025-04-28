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

"""Class definitions for a Generic Workflow Graph."""

__all__ = [
    "GenericWorkflow",
    "GenericWorkflowExec",
    "GenericWorkflowFile",
    "GenericWorkflowGroup",
    "GenericWorkflowJob",
    "GenericWorkflowNode",
    "GenericWorkflowNodeType",
    "GenericWorkflowNoopJob",
]


import dataclasses
import itertools
import logging
import pickle
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from enum import IntEnum, auto
from typing import IO, Any, BinaryIO, Literal, cast, overload

from networkx import DiGraph, topological_sort
from networkx.algorithms.dag import is_directed_acyclic_graph

from lsst.utils.iteration import ensure_iterable

from .bps_draw import draw_networkx_dot
from .bps_utils import subset_dimension_values

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class GenericWorkflowFile:
    """Information about a file that may be needed by various workflow
    management services.
    """

    name: str
    """Lookup key (logical file name) of file/directory. Must be unique
    within run.
    """

    src_uri: str | None = None  # don't know that need ResourcePath
    """Original location of file/directory.
    """

    wms_transfer: bool = False
    """Whether the WMS should ignore file or not.  Default is False.
    """

    job_access_remote: bool = False
    """Whether the job can remotely access file (using separately specified
    file access protocols).  Default is False.
    """

    job_shared: bool = False
    """Whether job requires its own copy of this file.  Default is False.
    """

    def __hash__(self) -> int:
        return hash(self.name)


@dataclasses.dataclass(slots=True)
class GenericWorkflowExec:
    """Information about an executable that may be needed by various workflow
    management services.
    """

    name: str
    """Lookup key (logical file name) of executable. Must be unique
    within run.
    """

    src_uri: str | None = None  # don't know that need ResourcePath
    """Original location of executable.
    """

    transfer_executable: bool = False
    """Whether the WMS/plugin is responsible for staging executable to
    location usable by job.
    """

    def __hash__(self) -> int:
        return hash(self.name)


class GenericWorkflowNodeType(IntEnum):
    """Type of valid types for nodes in the GenericWorkflow."""

    NOOP = auto()
    """Does nothing, but enforces special dependencies."""

    PAYLOAD = auto()
    """Typical workflow job."""

    GROUP = auto()
    """A special group (subdag) of jobs."""


@dataclasses.dataclass(slots=True)
class GenericWorkflowNode:
    """Base class for nodes in the GenericWorkflow."""

    name: str
    """Name of node.  Must be unique within workflow."""

    label: str
    """"Primary user-facing label for job.  Does not need to be unique and
    may be used for summary reports or to group nodes."""

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def node_type(self) -> GenericWorkflowNodeType:
        """Type of node."""
        raise NotImplementedError(f"{type(self).__name__} needs to override node_type.")


@dataclasses.dataclass(slots=True)
class GenericWorkflowNoopJob(GenericWorkflowNode):
    """Job that does no work.  Used for special dependencies."""

    @property
    def node_type(self) -> GenericWorkflowNodeType:
        """Indicate this is a noop job."""
        return GenericWorkflowNodeType.NOOP


@dataclasses.dataclass(slots=True)
class GenericWorkflowJob(GenericWorkflowNode):
    """Information about a job that may be needed by various workflow
    management services.
    """

    quanta_counts: Counter[str] = dataclasses.field(default_factory=Counter)
    """Counts of quanta per task label in job.
    """

    tags: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Other key/value pairs for job that user may want to use as a filter.
    """

    executable: GenericWorkflowExec | None = None
    """Executable for job.
    """

    arguments: str | None = None
    """Command line arguments for job.
    """

    cmdvals: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Values for variables in cmdline when using lazy command line creation.
    """

    memory_multiplier: float | None = None
    """Memory growth rate between retries.
    """

    request_memory: int | None = None  # MB
    """Max memory (in MB) that the job is expected to need.
    """

    request_memory_max: int | None = None  # MB
    """Max memory (in MB) that the job should ever use.
    """

    request_cpus: int | None = None  # cores
    """Max number of cpus that the job is expected to need.
    """

    request_disk: int | None = None  # MB
    """Max amount of job scratch disk (in MB) that the job is expected to need.
    """

    request_walltime: str | None = None  # minutes
    """Max amount of time (in seconds) that the job is expected to need.
    """

    compute_site: str | None = None
    """Key to look up site-specific information for running the job.
    """

    accounting_group: str | None = None
    """Name of the accounting group to use.
    """

    accounting_user: str | None = None
    """Name of the user to use for accounting purposes.
    """

    mail_to: str | None = None
    """Comma separated list of email addresses for emailing job status.
    """

    when_to_mail: str | None = None
    """WMS-specific terminology for when to email job status.
    """

    number_of_retries: int | None = None
    """Number of times to automatically retry a failed job.
    """

    retry_unless_exit: int | list[int] | None = None
    """Exit code(s) for job that means to not automatically retry.
    """

    abort_on_value: int | None = None
    """Job exit value for signals to abort the entire workflow.
    """

    abort_return_value: int | None = None
    """Exit value to use when aborting the entire workflow.
    """

    priority: str | None = None
    """Initial priority of job in WMS-format.
    """

    category: str | None = None
    """WMS-facing label of job within single workflow (e.g., can be used for
    throttling jobs within a single workflow).
    """

    concurrency_limit: str | None = None
    """Names of concurrency limits that the WMS plugin can appropriately
    translate to limit the number of this job across all running workflows.
    """

    queue: str | None = None
    """Name of queue to use. Different WMS can translate this concept
    differently.
    """

    pre_cmdline: str | None = None
    """Command line to be executed prior to executing job.
    """

    post_cmdline: str | None = None
    """Command line to be executed after job executes.

    Should be executed regardless of exit status.
    """

    preemptible: bool | None = None
    """The flag indicating whether the job can be preempted.
    """

    profile: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Nested dictionary of WMS-specific key/value pairs with primary key being
    WMS key (e.g., pegasus, condor, panda).
    """

    attrs: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Key/value pairs of job attributes (for WMS that have attributes in
    addition to commands).
    """

    environment: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Environment variable names and values to be explicitly set inside job.
    """

    compute_cloud: str | None = None
    """Key to look up cloud-specific information for running the job.
    """

    @property
    def node_type(self) -> GenericWorkflowNodeType:
        """Indicate this is a payload job."""
        return GenericWorkflowNodeType.PAYLOAD


class GenericWorkflow(DiGraph):
    """A generic representation of a workflow used to submit to specific
    workflow management systems.

    Parameters
    ----------
    name : `str`
        Name of generic workflow.
    incoming_graph_data : `~typing.Any`, optional
        Data used to initialized graph that is passed through to DiGraph
        constructor.  Can be any type supported by networkx.DiGraph.
    **attr : `dict`
        Keyword arguments passed through to DiGraph constructor.
    """

    def __init__(self, name: str, incoming_graph_data: Any | None = None, **attr: Any) -> None:
        super().__init__(incoming_graph_data, **attr)
        self._name = name
        self.run_attrs: dict[str, str] = {}
        self._job_labels = GenericWorkflowLabels()
        self._files: dict[str, GenericWorkflowFile] = {}
        self._executables: dict[str, GenericWorkflowExec] = {}
        self._inputs: dict[
            str, list[GenericWorkflowFile]
        ] = {}  # mapping job.names to list of GenericWorkflowFile
        self._outputs: dict[
            str, list[GenericWorkflowFile]
        ] = {}  # mapping job.names to list of GenericWorkflowFile
        self.run_id = None
        self._final: GenericWorkflowJob | GenericWorkflow | None = None

    @property
    def name(self) -> str:
        """Retrieve name of generic workflow.

        Returns
        -------
        name : `str`
            Name of generic workflow.
        """
        return self._name

    @property
    def quanta_counts(self) -> Counter[str]:
        """Count of quanta per task label (`collections.Counter`)."""
        qcounts: Counter[str] = Counter()
        for job_name in self:
            gwjob = self.get_job(job_name)
            if hasattr(gwjob, "quanta_counts"):
                qcounts += gwjob.quanta_counts
        return qcounts

    @property
    def labels(self) -> list[str]:
        """Job labels (`list` [`str`], read-only)."""
        return self._job_labels.labels

    def regenerate_labels(self) -> None:
        """Regenerate the list of job labels."""
        self._job_labels = GenericWorkflowLabels()
        for job_name in self:
            job = self.get_job(job_name)
            if job.node_type == GenericWorkflowNodeType.PAYLOAD:
                job = cast(GenericWorkflowJob, job)
                parents_labels: list[str] = []
                for parent_name in self.predecessors(job.name):
                    parent_job = self.get_job(parent_name)
                    if parent_job.node_type == GenericWorkflowNodeType.PAYLOAD:
                        # parent_job = cast(GenericWorkflowJob, parent_job)
                        parents_labels.append(parent_job.label)
                children_labels: list[str] = []
                for child_name in self.successors(job.name):
                    child_job = self.get_job(child_name)
                    if child_job.node_type == GenericWorkflowNodeType.PAYLOAD:
                        # child_job = cast(GenericWorkflowJob, child_job)
                        children_labels.append(child_job.label)
                self._job_labels.add_job(job, parents_labels, children_labels)

    @property
    def job_counts(self) -> Counter[str]:
        """Count of jobs per job label (`collections.Counter`)."""
        jcounts = self._job_labels.job_counts

        # Final is separate
        final = self.get_final()
        if final:
            if isinstance(final, GenericWorkflow):
                jcounts.update(final.job_counts)
            else:
                jcounts[final.label] += 1

        return jcounts

    def __iter__(self) -> Iterator[str]:
        """Return iterator of job names in topologically sorted order."""
        return topological_sort(self)

    @overload
    def get_files(self, data: Literal[False], transfer_only: bool = True) -> list[str]: ...

    @overload
    def get_files(self, data: Literal[True], transfer_only: bool = True) -> list[GenericWorkflowFile]: ...

    def get_files(
        self, data: bool = False, transfer_only: bool = True
    ) -> list[GenericWorkflowFile] | list[str]:
        """Retrieve files from generic workflow.

        Need API in case change way files are stored (e.g., make
        workflow a bipartite graph with jobs and files nodes).

        Parameters
        ----------
        data : `bool`, optional
            Whether to return the file data as well as the file object name
            (The default is False).
        transfer_only : `bool`, optional
            Whether to only return files for which a workflow management system
            would be responsible for transferring.

        Returns
        -------
        files : `list` [`lsst.ctrl.bps.GenericWorkflowFile`] or `list` [`str`]
            File names or objects from generic workflow meeting specifications.
        """
        files: list[Any] = []  # Any for mypy to allow the different append lines.
        for filename, file in self._files.items():
            if not transfer_only or file.wms_transfer:
                if not data:
                    files.append(filename)
                else:
                    files.append(file)
        return files

    def add_job(
        self,
        job: GenericWorkflowNode,
        parent_names: str | list[str] | None = None,
        child_names: str | list[str] | None = None,
    ) -> None:
        """Add job to generic workflow.

        Parameters
        ----------
        job : `lsst.ctrl.bps.GenericWorkflowNode`
            Job to add to the generic workflow.
        parent_names : `str` | `list` [`str`], optional
            Names of jobs that are parents of given job.
        child_names : `str` | `list` [`str`], optional
            Names of jobs that are children of given job.
        """
        _LOG.debug("job: %s (%s)", job.name, job.label)
        _LOG.debug("parent_names: %s", parent_names)
        _LOG.debug("child_names: %s", child_names)
        if not isinstance(job, GenericWorkflowNode):
            raise RuntimeError(f"Invalid type for job to be added to GenericWorkflowGraph ({type(job)}).")
        if self.has_node(job.name):
            raise RuntimeError(f"Job {job.name} already exists in GenericWorkflowGraph.")
        super().add_node(job.name, job=job)
        self.add_job_relationships(parent_names, job.name)
        self.add_job_relationships(job.name, child_names)
        if job.node_type == GenericWorkflowNodeType.PAYLOAD:
            job = cast(GenericWorkflowJob, job)
            self.add_executable(job.executable)
            self._job_labels.add_job(
                job,
                [self.get_job(p).label for p in self.predecessors(job.name)],
                [self.get_job(p).label for p in self.successors(job.name)],
            )

    def add_node(self, node_for_adding: GenericWorkflowNode, **attr: Any) -> None:
        """Override networkx function to call more specific add_job function.

        Parameters
        ----------
        node_for_adding : `lsst.ctrl.bps.GenericWorkflowJob`
            Job to be added to generic workflow.
        **attr
            Needed to match original networkx function, but not used.
        """
        self.add_job(node_for_adding)

    def add_job_relationships(
        self, parents: str | list[str] | None, children: str | list[str] | None
    ) -> None:
        """Add dependencies between parent and child jobs.  All parents will
        be connected to all children.

        Parameters
        ----------
        parents : `str` or `list` [`str`], optional
            Parent job names.
        children : `str` or `list` [`str`], optional
            Children job names.
        """
        # Allow this to be a noop if no parents or no children
        if parents is not None and children is not None:
            self.add_edges_from(itertools.product(ensure_iterable(parents), ensure_iterable(children)))
            self._job_labels.add_job_relationships(
                [self.get_job(n).label for n in ensure_iterable(parents)],
                [self.get_job(n).label for n in ensure_iterable(children)],
            )

    def add_edges_from(self, ebunch_to_add: Iterable[tuple[str, str]], **attr: Any) -> None:
        """Add several edges between jobs in the generic workflow.

        Parameters
        ----------
        ebunch_to_add : Iterable [`tuple` [`str`, `str`]]
            Iterable of job name pairs between which a dependency should be
            saved.
        **attr : keyword arguments, optional
            Data can be assigned using keyword arguments (not currently used).
        """
        for edge_to_add in ebunch_to_add:
            self.add_edge(edge_to_add[0], edge_to_add[1], **attr)

    def add_edge(self, u_of_edge: str, v_of_edge: str, **attr: Any) -> None:
        """Add edge connecting jobs in workflow.

        Parameters
        ----------
        u_of_edge : `str`
            Name of parent job.
        v_of_edge : `str`
            Name of child job.
        **attr : keyword arguments, optional
            Attributes to save with edge.
        """
        if u_of_edge not in self:
            raise RuntimeError(f"{u_of_edge} not in GenericWorkflow")
        if v_of_edge not in self:
            raise RuntimeError(f"{v_of_edge} not in GenericWorkflow")
        super().add_edge(u_of_edge, v_of_edge, **attr)

    def get_job(self, job_name: str) -> GenericWorkflowNode:
        """Retrieve job by name from workflow.

        Parameters
        ----------
        job_name : `str`
            Name of job to retrieve.

        Returns
        -------
        job : `lsst.ctrl.bps.GenericWorkflowNode`
            Job matching given job_name.
        """
        return self.nodes[job_name]["job"]

    def del_job(self, job_name: str) -> None:
        """Delete job from generic workflow leaving connected graph.

        Parameters
        ----------
        job_name : `str`
            Name of job to delete from workflow.
        """
        job = self.get_job(job_name)

        # Remove from job labels
        if isinstance(job, GenericWorkflowJob):
            self._job_labels.del_job(job)

        # Connect all parent jobs to all children jobs.
        parents = list(self.predecessors(job_name))
        children = list(self.successors(job_name))
        self.add_job_relationships(parents, children)

        # Delete job node (which deletes edges).
        self.remove_node(job_name)

    def add_job_inputs(self, job_name: str, files: GenericWorkflowFile | list[GenericWorkflowFile]) -> None:
        """Add files as inputs to specified job.

        Parameters
        ----------
        job_name : `str`
            Name of job to which inputs should be added.
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

    def get_file(self, name: str) -> GenericWorkflowFile:
        """Retrieve a file object by name.

        Parameters
        ----------
        name : `str`
            Name of file object.

        Returns
        -------
        gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
            File matching given name.
        """
        return self._files[name]

    def add_file(self, gwfile: GenericWorkflowFile) -> None:
        """Add file object.

        Parameters
        ----------
        gwfile : `lsst.ctrl.bps.GenericWorkflowFile`
            File object to add to workflow.
        """
        if gwfile.name not in self._files:
            self._files[gwfile.name] = gwfile
        else:
            _LOG.debug("Skipped add_file for existing file %s", gwfile.name)

    @overload
    def get_job_inputs(
        self, job_name: str, data: Literal[False], transfer_only: bool = False
    ) -> list[str]: ...

    @overload
    def get_job_inputs(
        self, job_name: str, data: Literal[True], transfer_only: bool = False
    ) -> list[GenericWorkflowFile]: ...

    def get_job_inputs(
        self, job_name: str, data: bool = True, transfer_only: bool = False
    ) -> list[GenericWorkflowFile] | list[str]:
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
        inputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`] or `list` [`str`]
            Input files for the given job.  If no input files for the job,
            returns an empty list.
        """
        inputs: list[Any] = []  # Any for mypy to allow the different append lines.
        if job_name in self._inputs:
            for gwfile in self._inputs[job_name]:
                if not transfer_only or gwfile.wms_transfer:
                    if not data:
                        inputs.append(gwfile.name)
                    else:
                        inputs.append(gwfile)
        return inputs

    def add_job_outputs(self, job_name: str, files: list[GenericWorkflowFile]) -> None:
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

    @overload
    def get_job_outputs(
        self, job_name: str, data: Literal[False], transfer_only: bool = False
    ) -> list[str]: ...

    @overload
    def get_job_outputs(
        self, job_name: str, data: Literal[True], transfer_only: bool = False
    ) -> list[GenericWorkflowFile]: ...

    def get_job_outputs(
        self, job_name: str, data: bool = True, transfer_only: bool = False
    ) -> list[GenericWorkflowFile] | list[str]:
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
        outputs : `list` [`lsst.ctrl.bps.GenericWorkflowFile`] or \
                  `list` [`str`]
            Output files for the given job. If no output files for the job,
            returns an empty list.
        """
        outputs: list[Any] = []  # Any for mypy to allow the different append lines.
        if not data:
            outputs = cast(list[str], outputs)
        else:
            outputs = cast(list[GenericWorkflowFile], outputs)

        if job_name in self._outputs:
            for gwfile in self._outputs[job_name]:
                if not transfer_only or gwfile.wms_transfer:
                    if not data:
                        outputs.append(gwfile.name)
                    else:
                        outputs.append(gwfile)
        return outputs

    def draw(self, stream: str | IO[str], format_: str = "dot") -> None:
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
            raise RuntimeError(f"Unknown draw format ({format_})")

    def save(self, stream: str | IO[bytes], format_: str = "pickle") -> None:
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
            stream = cast(BinaryIO, stream)
            pickle.dump(self, stream)
        else:
            raise RuntimeError(f"Unknown format ({format_})")

    @classmethod
    def load(cls, stream: str | IO[bytes], format_: str = "pickle") -> "GenericWorkflow":
        """Load a GenericWorkflow from the given stream.

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
            Generic workflow loaded from the given stream.
        """
        if format_ == "pickle":
            stream = cast(BinaryIO, stream)
            object_ = pickle.load(stream)
            assert isinstance(object_, GenericWorkflow)  # for mypy
            return object_

        raise RuntimeError(f"Unknown format ({format_})")

    def validate(self) -> None:
        """Run checks to ensure that the generic workflow graph is valid."""
        # Make sure a directed acyclic graph
        assert is_directed_acyclic_graph(self)

    def add_workflow_source(self, workflow: "GenericWorkflow") -> None:
        """Add given workflow as new source to this workflow.

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.GenericWorkflow`
           The given workflow.
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

        # Add separately stored info
        for job_name in workflow:
            job = self.get_job(job_name)
            # Add job labels
            if isinstance(job, GenericWorkflowJob):
                self._job_labels.add_job(
                    job,
                    [self.get_job(p).label for p in self.predecessors(job.name)],
                    [self.get_job(p).label for p in self.successors(job.name)],
                )
                # Executables are stored separately so copy them.
                self.add_executable(job.executable)

            # Files are stored separately so copy them.
            self.add_job_inputs(job_name, workflow.get_job_inputs(job_name, data=True))
            self.add_job_outputs(job_name, workflow.get_job_outputs(job_name, data=True))

    def add_final(self, final: "GenericWorkflowJob | GenericWorkflow") -> None:
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

    def get_final(self) -> "GenericWorkflowJob | GenericWorkflow | None":
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

    def add_executable(self, executable: GenericWorkflowExec | None) -> None:
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

    @overload
    def get_executables(self, data: Literal[False], transfer_only: bool = False) -> list[str]: ...

    @overload
    def get_executables(
        self, data: Literal[True], transfer_only: bool = False
    ) -> list[GenericWorkflowExec]: ...

    def get_executables(
        self, data: bool = False, transfer_only: bool = True
    ) -> list[GenericWorkflowExec] | list[str]:
        """Retrieve executables from generic workflow.

        Parameters
        ----------
        data : `bool`, optional
            Whether to return the executable data as well as the exec object
            name (The defaults is False).
        transfer_only : `bool`, optional
            Whether to only return executables for which transfer_executable
            is True.

        Returns
        -------
        execs : `list` [`lsst.ctrl.bps.GenericWorkflowExec`] or `list` [`str`]
            Filtered executable names or objects from generic workflow.
        """
        execs: list[Any] = []  # This and cast lines for mypy
        if not data:
            execs = cast(list[str], execs)
        else:
            execs = cast(list[GenericWorkflowExec], execs)

        for name, executable in self._executables.items():
            if not transfer_only or executable.transfer_executable:
                if not data:
                    execs.append(name)
                else:
                    execs.append(executable)
        return execs

    def get_jobs_by_label(self, label: str) -> list[GenericWorkflowJob]:
        """Retrieve jobs by label from workflow.

        Parameters
        ----------
        label : `str`
            Label of jobs to retrieve.

        Returns
        -------
        jobs : list[`lsst.ctrl.bps.GenericWorkflowNode`]
            Jobs having given label.
        """
        return self._job_labels.get_jobs_by_label(label)

    def _check_job_ordering_config(self, ordering_config: dict[str, Any]) -> dict[str, DiGraph]:
        """Check configuration related to job ordering.

        Parameters
        ----------
        ordering_config : `dict` [`str`, `~typing.Any`]
            Job ordering configuration to check.

        Returns
        -------
        group_to_label_subgraph : `dict` [`str`, `network.DiGraph`]
            Mapping of group name to a graph of the job labels in the group.
        """
        group_to_label_subgraph = {}
        job_label_to_group: dict[str, str] = {}  # Checking label appears only in one group
        for group, group_vals in ordering_config.items():
            implementation = group_vals.get("implementation", "group")
            if implementation not in ["noop", "group"]:
                raise RuntimeError(f"Invalid implementation for {group}: {implementation}")
            ordering_type = group_vals.get("ordering_type", "sort")
            if ordering_type != "sort":
                raise RuntimeError(f"Invalid ordering_type for {group}: {ordering_type}")
            if "dimensions" not in group_vals:
                raise KeyError(f"Missing dimensions entry in ordering group {group}")

            job_labels = [x.strip() for x in group_vals["labels"].split(",")]
            _LOG.debug("group %s: job_labels=%s", group, job_labels)
            unused_labels = []
            for job_label in job_labels:
                if job_label not in self._job_labels.labels:
                    unused_labels.append(job_label)
                elif job_label in job_label_to_group:
                    raise RuntimeError(
                        f"Job label {job_label} appears in more than one job ordering group "
                        f"({group} {job_label_to_group[job_label]})"
                    )
                else:
                    job_label_to_group[job_label] = group

            if unused_labels:
                _LOG.info("Workflow job labels = %s", ",".join(self._job_labels.labels))
                raise RuntimeError(
                    f"Job label(s) ({','.join(unused_labels)}) from job ordering group "
                    f"{group} does not exist in workflow.  Aborting."
                )

            label_subgraph = self._job_labels.subgraph(job_labels)
            group_to_label_subgraph[group] = label_subgraph

        return group_to_label_subgraph

    def _group_jobs_by_values(
        self, group: str, group_config: dict[str, Any], label_subgraph: DiGraph
    ) -> dict[tuple[Any, ...], list[str]]:
        """Create job mapping of special sortable dimension key
           to job name by comparing dimension values.

        Parameters
        ----------
        group : `str`
            Name of group for which creating mapping.
        group_config : `dict` [`str`, `~typing.Any`]
            Config for group for which creating mapping.
        label_subgraph : `networkx.DiGraph`
            The graph of job labels to be used in mapping.

        Returns
        -------
        dims_to_jobs : `dict` [`tuple` [`~typing.Any`, ...], `list` [`str`]]
            Mapping of dimensions to job names.
        """
        dims_to_jobs: dict[tuple[Any, ...], list[str]] = {}
        for job_label in label_subgraph:
            jobs = self.get_jobs_by_label(job_label)
            for job in jobs:
                job_dim_values = subset_dimension_values(
                    f"Job {job.name}",
                    f"order group {group}",
                    group_config["dimensions"],
                    job.tags,
                    group_config.get("equalDimensions", None),
                )
                job_dims = []
                for dim in [d.strip() for d in group_config["dimensions"].split(",")]:
                    job_dims.append(job_dim_values[dim])
                dims_job_list = dims_to_jobs.setdefault(tuple(job_dims), [])
                dims_job_list.append(job.name)
        return dims_to_jobs

    def _group_jobs_by_dependencies(
        self, group: str, group_config: dict[str, Any], label_subgraph: DiGraph
    ) -> dict[tuple[Any, ...], list[str]]:
        """Create job mapping of special sortable dimension key
           to job name by following dependencies.

        Parameters
        ----------
        group : `str`
            Name of group for which creating mapping.
        group_config : `dict` [`str`, `~typing.Any`]
            Config for group for which creating mapping.
        label_subgraph : `networkx.DiGraph`
            The graph of job labels to be used in mapping.

        Returns
        -------
        dims_to_jobs : `dict` [`tuple` [`~typing.Any`, ...], `list` [`str`]]
            Mapping of dimensions to job names.
        """
        method = group_config["findDependencyMethod"]
        dim_labels = list(topological_sort(label_subgraph))
        match method:
            case "source":
                find_potential_jobs = self.successors
            case "sink":
                find_potential_jobs = self.predecessors
                dim_labels.reverse()
            case _:
                raise RuntimeError(f"Invalid findDependencyMethod ({method})")

        jobs_seen: set[str] = set()
        dims_to_jobs: dict[tuple[Any, ...], list[str]] = {}
        for label in dim_labels:
            jobs = self.get_jobs_by_label(label)
            for job in jobs:
                if job.name not in jobs_seen:
                    jobs_seen.add(job.name)
                    job_dim_values = subset_dimension_values(
                        f"Job {job.name}",
                        f"order group {group}",
                        group_config["dimensions"],
                        job.tags,
                        group_config.get("equalDimensions", None),
                    )
                    job_dims = []
                    for dim in [d.strip() for d in group_config["dimensions"].split(",")]:
                        job_dims.append(job_dim_values[dim])
                    dims_job_list = dims_to_jobs.setdefault(tuple(job_dims), [])
                    dims_job_list.append(job.name)
                    # Use dependencies to find other quantum to add
                    # Note: in testing, using the following code was faster
                    # than using networkx descendants and ancestors functions
                    # While traversing the subgraph, nodes may appear
                    # repeatedly in potential_jobs.
                    jobs_to_use = [job]
                    while jobs_to_use:
                        job_to_use = jobs_to_use.pop()

                        potential_job_names = find_potential_jobs(job_to_use.name)
                        for potential_job_name in potential_job_names:
                            potential_job = cast(GenericWorkflowJob, self.get_job(potential_job_name))
                            if potential_job.label in label_subgraph:
                                if potential_job.name not in dims_job_list:
                                    _LOG.debug(
                                        "Adding potential job %s (%s) to group %s",
                                        potential_job.name,
                                        potential_job.label,
                                        group,
                                    )
                                    dims_job_list.append(potential_job.name)
                                    jobs_to_use.append(potential_job)
                                    jobs_seen.add(potential_job.name)
                            else:
                                _LOG.debug(
                                    "label (%s) not in ordered_tasks. Not adding potential quantum %s",
                                    potential_job.label,
                                    potential_job.name,
                                )
        return dims_to_jobs

    def _update_by_group_sort(
        self,
        group: str,
        dims_to_jobs: dict[tuple[Any, ...], list[str]],
        blocking: bool = False,
    ) -> None:
        """Update portion of workflow for special job ordering using sort.

        Parameters
        ----------
        group : `str`
            Ordering group label used for job name and messages.
        dims_to_jobs: `dict` [`tuple` [`~typing.Any`, ...], `list` [`str`]]
            Mapping of special dimension keys to workflow job names.
            The sort for the special ordering is over the keys.
        blocking: `bool`
            Whether a failure in a group blocks execution of remaining groups.
        """
        group_job_names: list[str] = []
        for dim_key, job_list in sorted(dims_to_jobs.items()):
            _LOG.debug("group %s: dim_key=%s", group, dim_key)
            group_job_name = f"group_{group}_{'=='.join([str(dk) for dk in dim_key])}"
            prev_name = group_job_names[-1] if group_job_names else None
            group_job_names.append(group_job_name)

            self._replace_subgraph_with_job_group(group_job_name, group, job_list, blocking)

            # ordering between groups
            if prev_name:
                self.add_edge(prev_name, group_job_name)

    def _replace_subgraph_with_job_group(
        self, group_name: str, group_label: str, job_list: list[str], blocking: bool
    ) -> None:
        """Update portion of workflow for special job ordering using groups.

        Parameters
        ----------
        group_name : `str`
            Ordering group name.
        group_label : `str`
            Ordering group label.
        job_list: `list` [`str`]
            List of job names to put in the group
        blocking: `bool`
            Whether a failure in a group blocks execution of remaining groups.
        """
        job_group = GenericWorkflowGroup(group_name, group_label, blocking=blocking)
        self.add_node(job_group)

        # Add jobs, files, and executables first
        # then add edges later to avoid order issues
        for job_name in job_list:
            job = cast(GenericWorkflowJob, self.get_job(job_name))
            job_group.add_job(job)
            files = self.get_job_inputs(job_name, data=True)
            job_group.add_job_inputs(job_name, files)
            files = self.get_job_outputs(job_name, data=True)
            job_group.add_job_outputs(job_name, files)
            job_group.add_executable(job.executable)

        # Can't remove edges while looping through edge view,
        # so save to remove after loops
        edges_to_remove: list[tuple[str, str]] = []
        for job_name in job_list:
            in_edges = self.in_edges(job_name)
            for u, v in in_edges:
                if u in job_list:
                    job_group.add_edge(u, v)
                else:
                    self.add_edge(u, group_name)
                    edges_to_remove.append((u, v))

            out_edges = self.out_edges(job_name)
            for u, v in out_edges:
                if v in job_list:
                    job_group.add_edge(u, v)
                else:
                    self.add_edge(group_name, v)
                    edges_to_remove.append((u, v))

        # Remove edges collected earlier
        self.remove_edges_from(edges_to_remove)

        # Remove nodes from main GenericWorkflow
        self.remove_nodes_from(job_list)

    def add_special_job_ordering(self, ordering: dict[str, Any]) -> None:
        """Add special nodes and dependencies to enforce given ordering.

        Parameters
        ----------
        ordering : `dict` [`str`, `~typing.Any`]
            Description of the job ordering to enforce.
        """
        group_to_label_subgraph = self._check_job_ordering_config(ordering)

        for group, group_vals in ordering.items():
            if "findDependencyMethod" in group_vals:
                job_grouping_func = self._group_jobs_by_dependencies
            else:
                job_grouping_func = self._group_jobs_by_values

            dims = [x.strip() for x in group_vals["dimensions"].split(",")]
            _LOG.debug("group %s: dims=%s", group, dims)
            job_groups = job_grouping_func(group, group_vals, group_to_label_subgraph[group])

            # Update the workflow
            implementation = group_vals.get("implementation", "group")
            ordering_type = group_vals.get("ordering_type", "sort")
            match (implementation, ordering_type):
                case ("noop", "sort"):
                    self._update_by_noop_sort(group, job_groups)
                case ("group", "sort"):
                    blocking = group_vals.get("blocking", False)
                    self._update_by_group_sort(group, job_groups, blocking)
                case _:
                    raise RuntimeError(
                        f"Invalid implementation, ordering_type pair for group ({implementation},"
                        f" {ordering_type})"
                    )

    def _update_by_noop_sort(self, group: str, dims_to_jobs: dict[tuple[Any, ...], list[str]]) -> None:
        """Update portion of workflow for special ordering of jobs using sort.

        Parameters
        ----------
        group : `str`
            Ordering group label used for job name and messages.
        dims_to_jobs: `dict` [`tuple`[`str`,...], `list` [`str`]]
            Mapping of special dimension keys to workflow job names.
            The sort for the special ordering is over the keys.
        """
        noop_names: list[str] = []
        prev_name: str | None = None
        for dim_key, job_list in sorted(dims_to_jobs.items()):
            _LOG.debug("group %s: dim_key=%s", group, dim_key)
            noop_name = f"noop_{group}_{'=='.join([str(dk) for dk in dim_key])}"
            if noop_names:
                prev_name = noop_names[-1]
            noop_names.append(noop_name)

            self._update_single_noop(job_list, noop_name, group, prev_name)

        # As implemented, loop adds one last NOOP job with
        # 0 children.  Remove it as not useful.
        assert self.out_degree(noop_names[-1]) == 0
        self.remove_node(noop_names[-1])

    def _update_single_noop(
        self, job_list: list[str], order_node_name: str, order_label: str, prev_order_name: str | None
    ) -> None:
        """Update the workflow around the single job making special NOOP
        jobs when necessary.

        Parameters
        ----------
        job_list : `list` [`str`]
            Current jobs involved in special ordering
        order_node_name : `str`
            Name for the order NOOP job.  If it does not exist in workflow, a
            new NOOP job with this name will be created and added.
        order_label : `str`
            Label for the order NOOP job.
        prev_order_name: `str` or None
            Name of the previous order NOOP job used to add edge as predecessor
            of current job.
        """
        if order_node_name not in self:
            _LOG.debug("Adding new ordering node %s", order_node_name)
            order_node = GenericWorkflowNoopJob(order_node_name, order_label)
            self.add_job(order_node)

        subgraph = DiGraph(self).subgraph(job_list)
        sinks = [n for n in job_list if subgraph.out_degree(n) == 0]
        for job_name in sinks:
            self.add_edge(job_name, order_node_name)

        if prev_order_name:
            sources = [n for n in job_list if subgraph.in_degree(n) == 0]
            for job_name in sources:
                _LOG.debug("Adding edge %s to %s", prev_order_name, job_name)
                self.add_edge(prev_order_name, job_name)


@dataclasses.dataclass(slots=True)
class GenericWorkflowGroup(GenericWorkflowNode, GenericWorkflow):
    """Node representing a group of jobs. Used for special dependencies.

    Parameters
    ----------
    name : `str`
        Name of node.  Must be unique within workflow.
    label : `str`
        Primary user-facing label for job.  Does not need to be unique and
        may be used for summary reports or to group nodes.
    blocking : `bool`
        Whether a failure inside group prunes executions of remaining groups.
    """

    blocking: bool = False
    """Whether a failure inside group prunes executions of remaining groups."""

    @property
    def node_type(self) -> GenericWorkflowNodeType:
        """Indicate this is a group job."""
        return GenericWorkflowNodeType.GROUP

    def __init__(self, name: str, label: str, blocking: bool = False) -> None:
        """Initialize each parent class."""
        _LOG.debug("%s %s %s", name, label, blocking)
        GenericWorkflowNode.__init__(self, name, label)
        GenericWorkflow.__init__(self, name)
        self.blocking = blocking


class GenericWorkflowLabels:
    """Label-oriented representation of the GenericWorkflowJobs."""

    def __init__(self) -> None:
        self._label_graph = DiGraph()  # Dependency graph of job labels
        self._label_to_jobs: defaultdict[str, list[GenericWorkflowJob]] = defaultdict(
            list
        )  # mapping job label to list of GenericWorkflowJob

    @property
    def labels(self) -> list[str]:
        """List of job labels (`list` [`str`], read-only)."""
        return list(topological_sort(self._label_graph))

    @property
    def job_counts(self) -> Counter[str]:
        """Count of jobs per job label (`collections.Counter`)."""
        return Counter({label: len(self._label_to_jobs[label]) for label in self.labels})

    def get_jobs_by_label(self, label: str) -> list[GenericWorkflowJob]:
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
        return self._label_to_jobs[label]

    def add_job(self, job: GenericWorkflowJob, parent_labels: list[str], child_labels: list[str]) -> None:
        """Add job's label to labels.

        Parameters
        ----------
        job : `lsst.ctrl.bps.GenericWorkflowJob`
            The job to add to the job labels.
        parent_labels : `list` [`str`]
            Parent job labels.
        child_labels : `list` [`str`]
            Children job labels.
        """
        _LOG.debug("job: %s (%s)", job.name, job.label)
        _LOG.debug("parent_labels: %s", parent_labels)
        _LOG.debug("child_labels: %s", child_labels)
        self._label_to_jobs[job.label].append(job)
        self._label_graph.add_node(job.label)
        for parent in parent_labels:
            self._label_graph.add_edge(parent, job.label)
        for child in child_labels:
            self._label_graph.add_edge(job.label, child)

    def add_job_relationships(self, parent_labels: list[str], children_labels: list[str]) -> None:
        """Add dependencies between parent and child job labels.
        All parents will be connected to all children.

        Parameters
        ----------
        parent_labels : `list` [`str`]
            Parent job labels.
        children_labels : `list` [`str`]
            Children job labels.
        """
        # Since labels, must ensure not adding edge from label to itself.
        edges = [
            e
            for e in itertools.product(ensure_iterable(parent_labels), ensure_iterable(children_labels))
            if e[0] != e[1]
        ]

        self._label_graph.add_edges_from(edges)

    def del_job(self, job: GenericWorkflowJob) -> None:
        """Delete job and its label from job labels.

        Parameters
        ----------
        job : `lsst.ctrl.bps.GenericWorkflowJob`
            The job to delete from the job labels.
        """
        self._label_to_jobs[job.label].remove(job)
        # Don't leave keys around if removed last job
        if not self._label_to_jobs[job.label]:
            del self._label_to_jobs[job.label]

            parents = self._label_graph.predecessors(job.label)
            children = self._label_graph.successors(job.label)
            self._label_graph.remove_node(job.label)
            self._label_graph.add_edges_from(
                itertools.product(ensure_iterable(parents), ensure_iterable(children))
            )

    def subgraph(self, labels: Iterable[str]) -> DiGraph:
        """Create subgraph of workflow label graph with given labels.

        Parameters
        ----------
        labels : Iterable [`str`]
            Labels to appear in subgraph.

        Returns
        -------
        subgraph : `networkx.DiGraph`
            Subgraph of workflow label graph with given labels.
        """
        return self._label_graph.subgraph(labels)
