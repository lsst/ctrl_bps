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

"""Functions that convert QuantumGraph into ClusteredQuantumGraph
"""

from .clustered_quantum_graph import ClusteredQuantumGraph


def single_quantum_clustering(_, qgraph, name):
    """Create clusters with only single quantum.

    Parameters
    ----------
    _ : `lsst.ctrl.bps.bps_config.BpsConfig`
        Not used in this function
    qgraph : `~lsst.pipe.base.QuantumGraph`
        QuantumGraph to break into clusters for ClusteredQuantumGraph

    Returns
    -------
    clustered_quantum : `ClusteredQuantumGraph`
        ClusteredQuantumGraph with single quantum per cluster created from
        given QuantumGraph
    """
    clustered_quantum = ClusteredQuantumGraph(name=name)

    name_pattern = "{:08d}"

    # create cluster of single quantum
    for quantum_node in qgraph:
        subgraph = qgraph.subset(quantum_node)
        label = quantum_node.taskDef.label
        clustered_quantum.add_cluster(name_pattern.format(quantum_node.nodeId.number), subgraph, label)

    # add cluster dependencies
    for quantum_node in qgraph:
        # get child nodes
        children = qgraph.determineOutputsOfQuantumNode(quantum_node)
        for child in children:
            clustered_quantum.add_edge(name_pattern.format(quantum_node.nodeId.number),
                                       name_pattern.format(child.nodeId.number))

    return clustered_quantum
