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
from lsst.pipe.base import QuantumGraph


class ClusteredQuantumGraph(networkx.DiGraph):
    """Graph where node in the graph is a QuantumGraph
    """

    def add_cluster(self, name, qgraph, label=None):
        """Add a cluster of quanta as a node in the graph.

        Parameters
        ----------
        name : `str`
            Node name which must be unique in the graph.
        qgraph : `~lsst.pipe.base.QuantumGraph`
            QuantumGraph containing the quanta in the cluster.
        label : `str`
            Label for the cluster.  Can be used in grouping clusters.
        """
        self.add_node(name, qgraph=qgraph, label=label)

    def add_node(self, node_for_adding, **attr):
        """Override add_node function to ensure that nodes are limited
        to QuantumGraphs

        Parameters
        ----------
        node_for_adding : `str` or `~lsst.pipe.base.QuantumGraph`
            Name of cluster or QuantumGraph to add where name will
            be a padded node counter.
        attr :
            Attributes to be saved with node in graph
            attr.qgraph : `~lsst.pipe.base.QuantumGraph`
                QuantumGraph to be stored in graph node
        """
        if isinstance(node_for_adding, QuantumGraph):
            name = f"{len(self) + 1:06d}"  # create cluster name by counter
            attr['qgraph'] = node_for_adding
        elif 'qgraph' in attr:
            if not isinstance(attr['qgraph'], QuantumGraph):
                raise RuntimeError("Invalid type for qgraph attribute.")
            name = node_for_adding
        else:
            raise RuntimeError("Missing qgraph attribute.")

        if 'label' not in attr:
            attr['label'] = None
        super().add_node(name, **attr)

    def add_nodes_from(self, nodes_for_adding, **attr):
        # Docstring inherited from networkx.Digraph
        raise TypeError("Multiple nodes should not have same qgraph attribute.")
