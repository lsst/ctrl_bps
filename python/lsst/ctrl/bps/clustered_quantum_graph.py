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

"""Class definitions for a Clustered QuantumGraph where a node in the graph is
a QuantumGraph.
"""

__all__ = ["QuantaCluster", "ClusteredQuantumGraph"]


import logging
import re
import pickle
from collections import Counter, defaultdict
from pathlib import Path
from networkx import DiGraph

from lsst.daf.butler import DimensionUniverse
from lsst.daf.butler.core.utils import iterable
from lsst.pipe.base import QuantumGraph, NodeId
from .bps_draw import draw_networkx_dot


_LOG = logging.getLogger(__name__)


class QuantaCluster:
    """Information about the cluster and Quanta belonging to it.

    Parameters
    ----------
    name: `str`
        Lookup key (logical file name) of file/directory. Must
        be unique within ClusteredQuantumGraph.
    label: `str`
        Value used to group clusters.
    tags : `dict` [`str`, `Any`], optional
        Arbitrary key/value pairs for the cluster.

    Raises
    ------
    ValueError
        Raised if invalid name (e.g., name contains /)
    """
    def __init__(self, name, label, tags=None):
        if '/' in name:
            raise ValueError(f"Cluster's name cannot have a / ({name})")
        self.name = name
        self.label = label
        self._qgraph_node_ids = []
        self._task_label_counts = Counter()
        self.tags = tags
        if self.tags is None:
            self.tags = {}

    @classmethod
    def from_quantum_node(cls, quantum_node, template):
        """Create single quantum cluster from given quantum node.

        Parameters
        ----------
        quantum_node : `lsst.pipe.base.QuantumNode`
            QuantumNode for which to make into a single quantum cluster.

        template : `str`
            Template for creating cluster name.

        Returns
        -------
        cluster : `QuantaCluster`
            Newly created cluster containing the given quantum.
        """
        label = quantum_node.taskDef.label
        node_id = quantum_node.nodeId
        data_id = quantum_node.quantum.dataId

        # Gather info for name template into a dictionary.
        info = data_id.byName()
        info["label"] = label
        info["node_number"] = node_id.number
        _LOG.debug("template = %s", template)
        _LOG.debug("info for template = %s", info)

        # Use dictionary plus template format string to create name. To avoid
        # key errors from generic patterns, use defaultdict.
        name = template.format_map(defaultdict(lambda: "", info))
        name = re.sub("_+", "_", name)
        _LOG.debug("template name = %s", name)

        cluster = QuantaCluster(name, label, info)
        cluster.add_quantum(quantum_node.nodeId, label)
        return cluster

    @property
    def qgraph_node_ids(self):
        """QuantumGraph NodeIds corresponding to this cluster.
        """
        _LOG.debug("_qgraph_node_ids = %s", self._qgraph_node_ids)
        return frozenset(self._qgraph_node_ids)

    @property
    def quanta_counts(self):
        """Counts of Quanta per taskDef.label in this cluster.
        """
        return Counter(self._task_label_counts)

    def add_quantum_node(self, quantum_node):
        """Add a quantumNode to this cluster.

        Parameters
        ----------
        quantum_node : `lsst.pipe.base.QuantumNode`
        """
        _LOG.debug("quantum_node = %s", quantum_node)
        _LOG.debug("quantum_node.nodeId = %s", quantum_node.nodeId)
        self.add_quantum(quantum_node.nodeId, quantum_node.taskDef.label)

    def add_quantum(self, node_id, task_label):
        """Add a quantumNode to this cluster.

        Parameters
        ----------
        node_id : `lsst.pipe.base.NodeId`
            ID for quantumNode to be added to cluster.
        task_label : `str`
            Task label for quantumNode to be added to cluster.
        """
        self._qgraph_node_ids.append(node_id)
        self._task_label_counts[task_label] += 1

    def __str__(self):
        return f"QuantaCluster(name={self.name},label={self.label},tags={self.tags}," \
               f"counts={self.quanta_counts},ids={self.qgraph_node_ids})"

    def __eq__(self, other: object) -> bool:
        # Doesn't check data equality, but only
        # name equality since those are supposed
        # to be unique.
        if isinstance(other, str):
            return self.name == other

        if isinstance(other, QuantaCluster):
            return self.name == other.name

        return False

    def __hash__(self) -> int:
        return hash(self.name)


class ClusteredQuantumGraph:
    """Graph where the data for a node is a subgraph of the full
    QuantumGraph represented by a list of node ids.

    Parameters
    ----------
    name : `str`
        Name to be given to the ClusteredQuantumGraph.
    qgraph : `lsst.pipe.base.QuantumGraph`
        The QuantumGraph to be clustered.
    qgraph_filename : `str`
        Filename for given QuantumGraph if it has already been
        serialized.

    Raises
    ------
    ValueError
        Raised if invalid name (e.g., name contains /)

    Notes
    -----
    Using lsst.pipe.base.NodeId instead of integer because the QuantumGraph
    API requires them.  Chose skipping the repeated creation of objects to
    use API over totally minimized memory usage.
    """

    def __init__(self, name, qgraph, qgraph_filename=None):
        if '/' in name:
            raise ValueError(f"name cannot have a / ({name})")
        self._name = name
        self._quantum_graph = qgraph
        self._quantum_graph_filename = qgraph_filename
        self._cluster_graph = DiGraph()

    def __str__(self):
        return f"ClusteredQuantumGraph(name={self.name}," \
               f"quantum_graph_filename={self._quantum_graph_filename}," \
               f"len(qgraph)={len(self._quantum_graph) if self._quantum_graph else None}," \
               f"len(cqgraph)={len(self._cluster_graph) if self._cluster_graph else None})"

    def __len__(self):
        """Return the number of clusters.
        """
        return len(self._cluster_graph)

    @property
    def name(self):
        """The name of the ClusteredQuantumGraph.
        """
        return self._name

    @property
    def qgraph(self):
        """The QuantumGraph associated with this Clustered
        QuantumGraph.
        """
        return self._quantum_graph

    def add_cluster(self, clusters_for_adding):
        """Add a cluster of quanta as a node in the graph.

        Parameters
        ----------
        clusters_for_adding: `QuantaCluster` or `Iterable` [`QuantaCluster`]
            The cluster to be added to the ClusteredQuantumGraph.
        """
        for cluster in iterable(clusters_for_adding):
            if not isinstance(cluster, QuantaCluster):
                raise TypeError(f"Must be type QuantaCluster (given: {type(cluster)})")

            if self._cluster_graph.has_node(cluster.name):
                raise KeyError(f"Cluster {cluster.name} already exists in ClusteredQuantumGraph")

            self._cluster_graph.add_node(cluster.name, cluster=cluster)

    def get_cluster(self, name):
        """Retrieve a cluster from the ClusteredQuantumGraph by name.

        Parameters
        ----------
        name : `str`
            Name of cluster to retrieve.

        Returns
        -------
        cluster : `QuantaCluster`
            QuantaCluster matching given name.

        Raises
        ------
        KeyError
            Raised if the ClusteredQuantumGraph does not contain
            a cluster with given name.
        """
        if name not in self._cluster_graph:
            raise KeyError(f"{self.name} does not have a cluster named {name}")

        _LOG.debug("get_cluster nodes = %s", list(self._cluster_graph.nodes))
        attr = self._cluster_graph.nodes[name]
        return attr['cluster']

    def get_quantum_node(self, id_):
        """Retrieve a QuantumNode from the ClusteredQuantumGraph by ID.

        Parameters
        ----------
        id_ : `lsst.pipe.base.NodeId` or int
            ID of the QuantumNode to retrieve.

        Returns
        -------
        quantum_node : `lsst.pipe.base.QuantumNode`
            QuantumNode matching given ID.

        Raises
        ------
        KeyError
            Raised if the ClusteredQuantumGraph does not contain
            a QuantumNode with given ID.
        """
        node_id = id_
        if isinstance(id_, int):
            node_id = NodeId(id, self._quantum_graph.graphID)
        _LOG.debug("get_quantum_node: node_id = %s", node_id)
        return self._quantum_graph.getQuantumNodeByNodeId(node_id)

    def __iter__(self):
        """Iterate over names of clusters.

        Returns
        -------
        names : `Iterator` [`str`]
            Iterator over names of clusters.
        """
        return self._cluster_graph.nodes()

    def clusters(self):
        """Iterate over clusters.

        Returns
        -------
        clusters : `Iterator` [`lsst.ctrl.bps.QuantaCluster`]
            Iterator over clusters.
        """
        return map(self.get_cluster, self._cluster_graph.nodes())

    def successors(self, name):
        """Return clusters that are successors of the cluster
        with the given name.

        Parameters
        ----------
        name : `str`
            Name of cluster for which need the successors.

        Returns
        -------
        clusters : `Iterator` [`lsst.ctrl.bps.QuantaCluster`]
            Iterator over successors of given cluster.
        """
        return map(self.get_cluster, self._cluster_graph.successors(name))

    def predecessors(self, name):
        """Return clusters that are predecessors of the cluster
        with the given name.

        Parameters
        ----------
        name : `str`
            Name of cluster for which need the predecessors.

        Returns
        -------
        clusters : `Iterator` [`lsst.ctrl.bps.QuantaCluster`]
            Iterator over predecessors of given cluster.
        """
        return map(self.get_cluster, self._cluster_graph.predecessors(name))

    def add_dependency(self, parent, child):
        """Add a directed dependency between a parent cluster and a child
           cluster.

        Parameters
        ----------
        parent : `str` or `QuantaCluster`
            Parent cluster.
        child : `str` or `QuantaCluster`
            Child cluster.

        Raises
        ------
        KeyError
            Raised if either the parent or child doesn't exist in the
            ClusteredQuantumGraph.
        """
        if not self._cluster_graph.has_node(parent):
            raise KeyError(f"{self.name} does not have a cluster named {parent}")
        if not self._cluster_graph.has_node(child):
            raise KeyError(f"{self.name} does not have a cluster named {child}")
        _LOG.debug("add_dependency: adding edge %s %s", parent, child)

        if isinstance(parent, QuantaCluster):
            pname = parent.name
        else:
            pname = parent

        if isinstance(child, QuantaCluster):
            cname = child.name
        else:
            cname = child
        self._cluster_graph.add_edge(pname, cname)

    def __contains__(self, name):
        """Check if a cluster with given name is in this ClusteredQuantumGraph.

        Parameters
        ----------
        name : `str`
            Name of cluster to check.

        Returns
        -------
        found : `bool`
            Whether a cluster with given name is in this ClusteredQuantumGraph.
        """
        return self._cluster_graph.has_node(name)

    def save(self, filename, format_=None):
        """Save the ClusteredQuantumGraph in a format that is loadable.
        The QuantumGraph is saved separately if hasn't already been
        serialized.

        Parameters
        ----------
        filename : `str`
            File to which the ClusteredQuantumGraph should be serialized.

        format_ : `str`, optional
            Format in which to write the data. It defaults to pickle format.
        """
        path = Path(filename)

        # if format is None, try extension
        if format_ is None:
            format_ = path.suffix[1:]  # suffix includes the leading period

        if format_ not in {"pickle"}:
            raise RuntimeError(f"Unknown format ({format_})")

        if not self._quantum_graph_filename:
            # Create filename based on given ClusteredQuantumGraph filename
            self._quantum_graph_filename = path.with_suffix('.qgraph')

        # If QuantumGraph file doesn't already exist, save it:
        if not Path(self._quantum_graph_filename).exists():
            self._quantum_graph.saveUri(self._quantum_graph_filename)

        if format_ == "pickle":
            # Don't save QuantumGraph in same file.
            tmp_qgraph = self._quantum_graph
            self._quantum_graph = None
            with open(filename, "wb") as fh:
                pickle.dump(self, fh)
            # Return to original state.
            self._quantum_graph = tmp_qgraph

    def draw(self, filename, format_=None):
        """Draw the ClusteredQuantumGraph in a given format.

        Parameters
        ----------
        filename : `str`
            File to which the ClusteredQuantumGraph should be serialized.

        format_ : `str`, optional
            Format in which to draw the data. It defaults to dot format.
        """
        path = Path(filename)

        # if format is None, try extension
        if format_ is None:
            format_ = path.suffix[1:]  # suffix includes the leading period

        draw_funcs = {"dot": draw_networkx_dot}
        if format_ in draw_funcs:
            draw_funcs[format_](self._cluster_graph, filename)
        else:
            raise RuntimeError(f"Unknown draw format ({format_}")

    @classmethod
    def load(cls, filename, format_=None):
        """Load a ClusteredQuantumGraph from the given file.

        Parameters
        ----------
        filename : `str`
            File from which to read the ClusteredQuantumGraph.
        format_ : `str`, optional
            Format of data to expect when loading from stream.  It defaults
            to pickle format.

        Returns
        -------
        ClusteredQuantumGraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
            ClusteredQuantumGraph workflow loaded from the given file.
            The QuantumGraph is loaded from its own file specified in
            the saved ClusteredQuantumGraph.
        """
        path = Path(filename)

        # if format is None, try extension
        if format_ is None:
            format_ = path.suffix[1:]  # suffix includes the leading period

        if format_ not in {"pickle"}:
            raise RuntimeError(f"Unknown format ({format_})")

        cgraph = None
        if format_ == "pickle":
            dim_univ = DimensionUniverse()
            with open(filename, "rb") as fh:
                cgraph = pickle.load(fh)

            # The QuantumGraph was saved separately
            cgraph._quantum_graph = QuantumGraph.loadUri(cgraph._quantum_graph_filename, dim_univ)
        return cgraph
