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

"""Class definitions for a Clustered QuantumGraph where a node
in the graph is a QuantumGraph.
"""

import networkx


class ClusteredQuantumGraph(networkx.DiGraph):
    """Graph where the data for a node is a subgraph of the full
    QuantumGraph represented by a list of NodeIds.

    Using lsst.pipe.base.NodeId instead of integer because the QuantumGraph
    API requires them.  Chose skipping the repeated creation of objects to
    use API over totally minimized memory usage.
    """

    def add_cluster(self, name, node_ids, label=None):
        """Add a cluster of quanta as a node in the graph.

        Parameters
        ----------
        name : `str`
            Node name which must be unique in the graph.
        node_ids : `list` of `~lsst.pipe.base.NodeId`
            NodeIds for QuantumGraph subset.
        label : `str`, optional
            Label for the cluster.  Can be used in grouping clusters.
        """
        self.add_node(name, qgraph_node_ids=node_ids, label=label)

    def add_node(self, node_for_adding, **attr):
        """Override add_node function to ensure that nodes are limited
        to QuantumGraphs

        Parameters
        ----------
        node_for_adding : `str` or `list` of `~lsst.pipe.base.NodeId`
            Name of cluster or cluster data (list of NodeIds).
        attr :
            Attributes to be saved with node in graph.
        """
        if isinstance(node_for_adding, list):
            name = f"{len(self) + 1:06d}"  # Create cluster name by counter.
            attr["qgraph_node_ids"] = node_for_adding
        elif "qgraph_node_ids" in attr:
            if not isinstance(attr["qgraph_node_ids"], list):
                raise RuntimeError("Invalid type for qgraph_node_ids attribute.")
            name = node_for_adding
        else:
            raise RuntimeError("Missing qgraph_node_ids attribute.")

        if "label" not in attr:
            attr["label"] = None
        super().add_node(name, **attr)

    def add_nodes_from(self, nodes_for_adding, **attr):
        # Docstring inherited from networkx.Digraph
        raise TypeError("Multiple nodes should not have same attributes.")
