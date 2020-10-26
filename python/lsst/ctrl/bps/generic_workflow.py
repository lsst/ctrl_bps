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
from typing import Optional, Tuple, Union, Iterator, Any
import networkx as nx

from lsst.daf.butler.core.utils import iterable
from lsst.daf.butler import DatasetRef
from lsst.pipe.base import QuantumGraph
from .bps_draw import draw_networkx_dot


@dataclasses.dataclass
class GenericWorkflowFile:
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

    __slots__ = ('name', 'wms_transfer', 'dataset_ref', 'src_uri', 'dest_uri', 'logical_file_name')

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return f"GenericWorkflowJob(name={self.name})"


@dataclasses.dataclass
class GenericWorkflowJob:
    name: str
    label: Optional[str]
    cmdline: str
    request_memory: Optional[int]    # MB
    request_cpu: Optional[int]       # cores
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
    quanta_summary: Optional[str]

    # As of python 3.7.8, can't use __slots__ if give default values, so writing own __init__
    def __init__(self, name: str):
        self.name = name
        self.label = None
        self.cmdline = None
        self.request_memory = None
        self.request_cpu = None
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
        self.quanta_summary = ""

    __slots__ = ('name', 'label', 'mail_to', 'when_to_mail', 'cmdline', 'request_memory', 'request_cpu',
                 'request_disk', 'request_walltime', 'compute_site', 'environment', 'number_of_retries',
                 'retry_unless_exit', 'abort_on_value', 'abort_return_value', 'priority',
                 'category', 'pre_cmdline', 'post_cmdline', 'profile', 'attrs',
                 'quantum_graph', 'quanta_summary')

    def __hash__(self):
        return hash(self.name)


class GenericWorkflow(nx.DiGraph):
    def __init__(self, name, incoming_graph_data=None, **attr):
        super().__init__(incoming_graph_data, **attr)
        self._name = name
        self.run_attrs = {}
        self._files = {}
        self.run_id = None

    @property
    def name(self):
        return self._name

    def get_files(self, data=False, transfer_only=True):
        """
        Need API in case change way files are stored (e.g., make workflow a bipartite graph with jobs and
        files)
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
        if not isinstance(job, GenericWorkflowJob):
            raise RuntimeError(f"Invalid type for job to be added to GenericWorkflowGraph ({type(job)}).")
        if self.has_node(job.name):
            raise RuntimeError(f"Job {job.name} already exists in GenericWorkflowGraph.")
        super().add_node(job.name, job=job, inputs={}, outputs={})
        self.add_job_relationships(parent_names, job.name)
        self.add_job_relationships(job.name, child_names)

    def add_node(self, node_for_adding, **attr):
        self.add_job(node_for_adding)

    def add_job_relationships(self, parents, children):
        if parents is not None and children is not None:
            self.add_edges_from(itertools.product(iterable(parents), iterable(children)))

    def add_edges_from(self, ebunch_to_add: Union[Iterator[Tuple[Any, Any]], Iterator[Tuple[Any, ...]]],
                       **attr):
        for edge_to_add in ebunch_to_add:
            self.add_edge(edge_to_add[0], edge_to_add[1], **attr)

    def add_edge(self, u_of_edge: str, v_of_edge: str, **attr):
        if u_of_edge not in self:
            raise RuntimeError(f"{u_of_edge} not in GenericWorkflow")
        if v_of_edge not in self:
            raise RuntimeError(f"{v_of_edge} not in GenericWorkflow")
        super().add_edge(u_of_edge, v_of_edge, **attr)

    def get_job(self, job_name: str):
        return self.nodes[job_name]['job']

    def del_job(self, job_name: str):
        # Delete job from generic workflow leaving connected graph.

        # Connect all parent jobs to all children jobs.
        parents = self.predecessors(job_name)
        children = self.successors(job_name)
        self.add_job_relationships(parents, children)

        # Delete job node (which deleted edges).
        self.remove_node(job_name)

    def add_job_inputs(self, job_name: str, files):
        job_inputs = self.nodes[job_name]['inputs']
        for file in iterable(files):
            # Save the central copy
            if file.name not in self._files:
                self._files[file.name] = file

            # Save the job reference to the file
            job_inputs[file.name] = file

    def get_file(self, name):
        return self._files[name]

    def get_job_inputs(self, job_name, data=True, transfer_only=False):
        job_inputs = self.nodes[job_name]['inputs']
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
        job_outputs = self.nodes[job_name]['outputs']
        for file in files:
            # Save the central copy
            if file.name not in self._files:
                self._files[file.name] = file
            # Save the job reference to the file
            job_outputs[file.name] = file

    def get_job_outputs(self, job_name, data=True, transfer_only=False):
        job_outputs = self.nodes[job_name]['outputs']
        outputs = []
        for file_name in job_outputs:
            file = self._files[file_name]
            if not transfer_only or file.wms_transfer:
                if not data:
                    outputs.append(file_name)
                else:
                    outputs.append(self._files[file_name])
        return outputs

    def draw(self, stream, format_):
        draw_funcs = {'dot': draw_networkx_dot}
        if format_ in draw_funcs:
            draw_funcs[format_](self, stream)
        else:
            raise RuntimeError(f"Unknown draw format ({format_}")

    def save(self, stream, format_='pickle'):
        if format_ == 'pickle':
            nx.write_gpickle(self, stream)
        else:
            raise RuntimeError(f"Unknown format ({format_})")

    @classmethod
    def load(cls, stream, format_):
        """Load a GenericWorkflow from the given stream

        Parameters
        ----------
        stream : `IO` or `str`
            Stream to pass to the format-specific loader. Accepts anything that
            the loader accepts.
        format_ : `str`
            Format of data to expect when loading from stream.

        Returns
        -------
        generic_workflow : GenericWorkflow
            Generic workflow loaded from the given stream
        """
        if format_ == 'pickle':
            return nx.read_gpickle(stream)

        raise RuntimeError(f"Unknown format ({format_})")

    def validate(self):
        # Make sure a directed acyclic graph
        assert nx.algorithms.dag.is_directed_acyclic_graph(self)
