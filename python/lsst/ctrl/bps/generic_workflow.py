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

"""Class definitions for a Generic Workflow Graph
"""

import dataclasses
import itertools
from typing import Optional

import networkx as nx

from lsst.daf.butler.core.utils import iterable
from lsst.daf.butler import DatasetRef
from lsst.pipe.base import QuantumGraph
from .bps_draw import draw_networkx_dot


@dataclasses.dataclass
class GenericWorkflowFile:
    """Information about a file that may be needed by various workflow
    management services."""
    name: str
    wms_transfer: bool
    src_uri: str or None  # don't know that need ButlerURI
    dataset_ref: DatasetRef or None
    dest_uri: str or None    # don't know that need ButlerURI
    logical_file_name: str or None

    # As of python 3.7.8, can't use __slots__ + dataclass if give default
    # values, so writing own __init__
    def __init__(self, name: str, wms_transfer: bool = False, src_uri=None,
                 dataset_ref=None, dest_uri=None, logical_file_name=None):
        self.name = name
        self.wms_transfer = wms_transfer
        self.src_uri = src_uri
        self.dataset_ref = dataset_ref
        self.dest_uri = dest_uri
        self.logical_file_name = logical_file_name

    __slots__ = ("name", "wms_transfer", "dataset_ref", "src_uri", "dest_uri", "logical_file_name")

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return f"GenericWorkflowJob(name={self.name})"


@dataclasses.dataclass
class GenericWorkflowJob:
    """Information about a job that may be needed by various workflow
    management services.
    """
    name: str
    label: Optional[str]
    tags: Optional[str]
    cmdline: Optional[str]
    request_memory: Optional[int]    # MB
    request_cpus: Optional[int]       # cores
    request_disk: Optional[int]      # MB
    request_walltime: Optional[str]  # minutes
    compute_site: Optional[str]
    mail_to: Optional[str]
    when_to_mail: Optional[str]
    number_of_retries: Optional[int]
    retry_unless_exit: Optional[int]
    abort_on_value: Optional[int]
    abort_return_value: Optional[int]
    priority: Optional[str]
    category: Optional[str]
    pre_cmdline: Optional[str]
    post_cmdline: Optional[str]
    profile: Optional[dict]
    attrs: Optional[dict]
    environment: Optional[dict]
    quantum_graph: Optional[QuantumGraph]
    qgraph_node_ids: Optional[list]
    quanta_summary: Optional[str]

    # As of python 3.7.8, can't use __slots__ if give default values, so writing own __init__
    def __init__(self, name: str):
        self.name = name
        self.label = None
        self.tags = None
        self.cmdline = None
        self.request_memory = None
        self.request_cpus = None
        self.request_disk = None
        self.request_walltime = None
        self.compute_site = None
        self.mail_to = None
        self.when_to_mail = None
        self.number_of_retries = None
        self.retry_unless_exit = None
        self.abort_on_value = None
        self.abort_return_value = None
        self.priority = None
        self.category = None
        self.pre_cmdline = None
        self.post_cmdline = None
        self.profile = {}
        self.attrs = {}
        self.environment = {}
        self.quantum_graph = None
        self.qgraph_node_ids = None
        self.quanta_summary = ""

    __slots__ = ("name", "label", "tags", "mail_to", "when_to_mail", "cmdline", "request_memory",
                 "request_cpus", "request_disk", "request_walltime", "compute_site", "environment",
                 "number_of_retries", "retry_unless_exit", "abort_on_value", "abort_return_value",
                 "priority", "category", "pre_cmdline", "post_cmdline", "profile", "attrs",
                 "quantum_graph", "qgraph_node_ids", "quanta_summary")

    def __hash__(self):
        return hash(self.name)


class GenericWorkflow(nx.DiGraph):
    """A generic representation of a workflow used to submit to specific
    workflow management systems.

    Parameters
    ----------
    name : `str`
        Name of generic workflow.
    incoming_graph_data : `Any`, optional
        Data used to initialized graph that is passed through to nx.DiGraph
        constructor.  Can be any type supported by networkx.DiGraph.
    attr : `dict`
        Keyword arguments passed through to nx.DiGraph constructor.
    """
    def __init__(self, name, incoming_graph_data=None, **attr):
        super().__init__(incoming_graph_data, **attr)
        self._name = name
        self.run_attrs = {}
        self._files = {}
        self.run_id = None

    @property
    def name(self):
        """Retrieve name of generic workflow.

        Returns
        -------
        name : `str`
            Name of generic workflow.
        """
        return self._name

    def get_files(self, data=False, transfer_only=True):
        """Retrieve files from generic workflow.
        Need API in case change way files are stored (e.g., make
        workflow a bipartite graph with jobs and files nodes).

        Parameters
        ----------
        data : `bool`, optional
            Whether to return the file data as well as the file object name.
        transfer_only : `bool`, optional
            Whether to only return files for which a workflow management system
            would be responsible for transferring.

        Returns
        -------
        files : `list` of `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
            Files from generic workflow meeting specifications.
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
        job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
            Job to add to the generic workflow.
        parent_names : `list` of `str`, optional
            Names of jobs that are parents of given job
        child_names : `list` of `str`, optional
            Names of jobs that are children of given job
        """
        if not isinstance(job, GenericWorkflowJob):
            raise RuntimeError(f"Invalid type for job to be added to GenericWorkflowGraph ({type(job)}).")
        if self.has_node(job.name):
            raise RuntimeError(f"Job {job.name} already exists in GenericWorkflowGraph.")
        super().add_node(job.name, job=job, inputs={}, outputs={})
        self.add_job_relationships(parent_names, job.name)
        self.add_job_relationships(job.name, child_names)

    def add_node(self, node_for_adding, **attr):
        """Override networkx function to call more specific add_job function.

        Parameters
        ----------
        node_for_adding : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
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
        parents : `list` of `str`
            Parent job names.
        children : `list` of `str`
            Children job names.
        """
        if parents is not None and children is not None:
            self.add_edges_from(itertools.product(iterable(parents), iterable(children)))

    def add_edges_from(self, ebunch_to_add, **attr):
        """Add several edges between jobs in the generic workflow.

        Parameters
        ----------
        ebunch_to_add : Iterable of `tuple` of `str`
            Iterable of job name pairs between which a dependency should be saved.
        attr : keyword arguments, optional
            Data can be assigned using keyword arguments (not currently used)
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
        job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
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
        # Connect all parent jobs to all children jobs.
        parents = self.predecessors(job_name)
        children = self.successors(job_name)
        self.add_job_relationships(parents, children)

        # Delete job node (which deleted edges).
        self.remove_node(job_name)

    def add_job_inputs(self, job_name: str, files):
        """Add files as inputs to specified job.

        Parameters
        ----------
        job_name : `str`
            Name of job to which inputs should be added
        files : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile` or `list`
            File object(s) to be added as inputs to the specified job.
        """
        job_inputs = self.nodes[job_name]["inputs"]
        for file in iterable(files):
            # Save the central copy
            if file.name not in self._files:
                self._files[file.name] = file

            # Save the job reference to the file
            job_inputs[file.name] = file

    def get_file(self, name):
        """Retrieve a file object by name.

        Parameters
        ----------
        name : `str`
            Name of file object

        Returns
        -------
        file_ : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
            File matching given name.
        """
        return self._files[name]

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
        inputs : `list` of `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
            Input files for the given job.
        """
        job_inputs = self.nodes[job_name]["inputs"]
        inputs = []
        for file_name in job_inputs:
            file = self._files[file_name]
            if not transfer_only or file.wms_transfer:
                if not data:
                    inputs.append(file_name)
                else:
                    inputs.append(self._files[file_name])
        return inputs

    def add_job_outputs(self, job_name, files):
        """Add output files to a job.

        Parameters
        ----------
        job_name : `str`
            Name of job to which the files should be added as outputs.
        files : `list` of `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
            File objects to be added as outputs for specified job.
        """
        job_outputs = self.nodes[job_name]["outputs"]
        for file in files:
            # Save the central copy
            if file.name not in self._files:
                self._files[file.name] = file
            # Save the job reference to the file
            job_outputs[file.name] = file

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
        outputs : `list` of `~lsst.ctrl.bps.generic_workflow.GenericWorkflowFile`
            Output files for the given job.
        """
        job_outputs = self.nodes[job_name]["outputs"]
        outputs = []
        for file_name in job_outputs:
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
            nx.write_gpickle(self, stream)
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
        generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic workflow loaded from the given stream
        """
        if format_ == "pickle":
            return nx.read_gpickle(stream)

        raise RuntimeError(f"Unknown format ({format_})")

    def validate(self):
        """Run checks to ensure this is still a valid generic workflow graph.
        """
        # Make sure a directed acyclic graph
        assert nx.algorithms.dag.is_directed_acyclic_graph(self)
