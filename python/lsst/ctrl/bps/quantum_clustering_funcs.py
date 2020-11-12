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
import re
import logging
from collections import defaultdict

from .clustered_quantum_graph import ClusteredQuantumGraph

_LOG = logging.getLogger()


def single_quantum_clustering(config, qgraph, name):
    """Create clusters with only single quantum.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration.
    qgraph : `~lsst.pipe.base.QuantumGraph`
        QuantumGraph to break into clusters for ClusteredQuantumGraph.
    name : `str`
        Name to give to ClusteredQuantumGraph.

    Returns
    -------
    clustered_quantum : `~lsst.ctrl.bps.clustered_quantum_graph.ClusteredQuantumGraph`
        ClusteredQuantumGraph with single quantum per cluster created from
        given QuantumGraph.
    """
    clustered_quantum = ClusteredQuantumGraph(name=name)

    # Save mapping of quantum nodeNumber to name so don't have to create it multiple times.
    number_to_name = {}

    # Create cluster of single quantum.
    for quantum_node in qgraph:
        subgraph = qgraph.subset(quantum_node)
        label = quantum_node.taskDef.label
        found, template = config.search('templateDataId', opt={'curvals': {'curr_pipetask': label},
                                                               'replaceVars': False})
        if found:
            template = "{node_number}_{label}_" + template
        else:
            template = "{node_number:08d}"

        # Note: Can't quite reuse lsst.daf.butler.core.fileTemplates.FileTemplate as don't
        # want to require datasetType (and run) in the template.  Use defaultdict to handle
        # the missing values in template.
        data_id = quantum_node.quantum.dataId
        info = defaultdict(lambda: '', {k: data_id.get(k) for k in data_id.graph.names})
        info['label'] = label
        info['node_number'] = quantum_node.nodeId.number
        _LOG.debug("template = %s", template)
        _LOG.debug("info for template = %s", info)
        name = template.format_map(info)
        name = re.sub('_+', '_', name)
        _LOG.debug("template name = %s", name)
        number_to_name[quantum_node.nodeId.number] = name

        clustered_quantum.add_cluster(name, subgraph, label)

    # Add cluster dependencies.
    for quantum_node in qgraph:
        # Get child nodes.
        children = qgraph.determineOutputsOfQuantumNode(quantum_node)
        for child in children:
            clustered_quantum.add_edge(number_to_name[quantum_node.nodeId.number],
                                       number_to_name[child.nodeId.number])

    return clustered_quantum
