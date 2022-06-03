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

"""Functions that convert QuantumGraph into ClusteredQuantumGraph.
"""
import logging
import re
from collections import defaultdict

from lsst.pipe.base import NodeId
from networkx import DiGraph, is_directed_acyclic_graph

from . import ClusteredQuantumGraph, QuantaCluster

_LOG = logging.getLogger(__name__)


def single_quantum_clustering(config, qgraph, name):
    """Create clusters with only single quantum.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph to break into clusters for ClusteredQuantumGraph.
    name : `str`
        Name to give to ClusteredQuantumGraph.

    Returns
    -------
    clustered_quantum : `lsst.ctrl.bps.ClusteredQuantumGraph`
        ClusteredQuantumGraph with single quantum per cluster created from
        given QuantumGraph.
    """
    cqgraph = ClusteredQuantumGraph(
        name=name,
        qgraph=qgraph,
        qgraph_filename=config[".bps_defined.runQgraphFile"],
        butler_uri=config["butlerConfig"],
    )

    # Save mapping of quantum nodeNumber to name so don't have to create it
    # multiple times.
    number_to_name = {}

    # Cache template per label for speed.
    cached_template = {}

    # Create cluster of single quantum.
    for qnode in qgraph:
        if qnode.taskDef.label not in cached_template:
            found, template_data_id = config.search(
                "templateDataId",
                opt={"curvals": {"curr_pipetask": qnode.taskDef.label}, "replaceVars": False},
            )
            if found:
                template = "{node_number}_{label}_" + template_data_id
            else:
                template = "{node_number}"
            cached_template[qnode.taskDef.label] = template

        cluster = QuantaCluster.from_quantum_node(qnode, cached_template[qnode.taskDef.label])

        # Save mapping for use when creating dependencies.
        number_to_name[qnode.nodeId] = cluster.name

        cqgraph.add_cluster(cluster)

    # Add cluster dependencies.
    for qnode in qgraph:
        # Get child nodes.
        children = qgraph.determineOutputsOfQuantumNode(qnode)
        for child in children:
            cqgraph.add_dependency(number_to_name[qnode.nodeId], number_to_name[child.nodeId])

    return cqgraph


def _check_clusters_tasks(cluster_config, taskGraph):
    """Check cluster definitions in terms of pipetask lists.

    Parameters
    ----------
    cluster_config : `lsst.ctrl.bps.BpsConfig`
        The cluster section from the BPS configuration.
    taskGraph : `lsst.pipe.base.taskGraph`
        Directed graph of tasks.

    Returns
    -------
    task_labels : `set` [`str`]
        Set of task labels from the cluster definitions.

    Raises
    -------
    RuntimeError
        Raised if task label appears in more than one cluster def or
        if there's a cycle in the cluster defs.
    """

    # Build a "clustered" task graph to check for cycle.
    task_to_cluster = {}
    task_labels = set()
    clustered_task_graph = DiGraph()

    # Create clusters based on given configuration.
    for cluster_label in cluster_config:
        _LOG.debug("cluster = %s", cluster_label)
        cluster_tasks = [pt.strip() for pt in cluster_config[cluster_label]["pipetasks"].split(",")]
        for task_label in cluster_tasks:
            if task_label in task_labels:
                raise RuntimeError(
                    f"Task label {task_label} appears in more than one cluster definition.  "
                    "Aborting submission."
                )
            task_labels.add(task_label)
            task_to_cluster[task_label] = cluster_label
            clustered_task_graph.add_node(cluster_label)

    # Create clusters for tasks not covered by clusters.
    for task in taskGraph:
        if task.label not in task_labels:
            task_to_cluster[task.label] = task.label
            clustered_task_graph.add_node(task.label)

    # Create dependencies between clusters.
    for edge in taskGraph.edges:
        if task_to_cluster[edge[0].label] != task_to_cluster[edge[1].label]:
            clustered_task_graph.add_edge(task_to_cluster[edge[0].label], task_to_cluster[edge[1].label])

    _LOG.debug("clustered_task_graph.edges = %s", [e for e in clustered_task_graph.edges])

    if not is_directed_acyclic_graph(clustered_task_graph):
        raise RuntimeError("Cluster pipetasks do not create a DAG")

    return task_labels


def dimension_clustering(config, qgraph, name):
    """Follow config instructions to make clusters based upon dimensions.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph to break into clusters for ClusteredQuantumGraph.
    name : `str`
        Name to give to ClusteredQuantumGraph.

    Returns
    -------
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        ClusteredQuantumGraph with clustering as defined in config.
    """
    cqgraph = ClusteredQuantumGraph(
        name=name,
        qgraph=qgraph,
        qgraph_filename=config[".bps_defined.runQgraphFile"],
        butler_uri=config["butlerConfig"],
    )

    # save mapping in order to create dependencies later
    quantum_to_cluster = {}

    cluster_config = config["cluster"]
    task_labels = _check_clusters_tasks(cluster_config, qgraph.taskGraph)
    for cluster_label in cluster_config:
        _LOG.debug("cluster = %s", cluster_label)
        cluster_dims = []
        if "dimensions" in cluster_config[cluster_label]:
            cluster_dims = [d.strip() for d in cluster_config[cluster_label]["dimensions"].split(",")]
        _LOG.debug("cluster_dims = %s", cluster_dims)

        found, template = cluster_config[cluster_label].search("clusterTemplate", opt={"replaceVars": False})
        if not found:
            if cluster_dims:
                template = f"{cluster_label}_" + "_".join(f"{{{dim}}}" for dim in cluster_dims)
            else:
                template = cluster_label
        _LOG.debug("template = %s", template)

        cluster_tasks = [pt.strip() for pt in cluster_config[cluster_label]["pipetasks"].split(",")]
        for task_label in cluster_tasks:
            task_labels.add(task_label)

            # Currently getQuantaForTask is currently a mapping taskDef to
            # Quanta, so quick enough to call repeatedly.
            task_def = qgraph.findTaskDefByLabel(task_label)
            if task_def is None:
                continue
            quantum_nodes = qgraph.getNodesForTask(task_def)

            equal_dims = cluster_config[cluster_label].get("equalDimensions", None)

            # Determine cluster for each node
            for qnode in quantum_nodes:
                # Gather info for cluster name template into a dictionary.
                info = {}

                missing_info = set()
                data_id_info = qnode.quantum.dataId.byName()
                for dim_name in cluster_dims:
                    _LOG.debug("dim_name = %s", dim_name)
                    if dim_name in data_id_info:
                        info[dim_name] = data_id_info[dim_name]
                    else:
                        missing_info.add(dim_name)
                if equal_dims:
                    for pair in [pt.strip() for pt in equal_dims.split(",")]:
                        dim1, dim2 = pair.strip().split(":")
                        if dim1 in cluster_dims and dim2 in data_id_info:
                            info[dim1] = data_id_info[dim2]
                            missing_info.remove(dim1)
                        elif dim2 in cluster_dims and dim1 in data_id_info:
                            info[dim2] = data_id_info[dim1]
                            missing_info.remove(dim2)

                info["label"] = cluster_label
                _LOG.debug("info for template = %s", info)

                if missing_info:
                    raise RuntimeError(
                        "Quantum %s (%s) missing dimensions %s required for cluster %s"
                        % (qnode.nodeId, data_id_info, ",".join(missing_info), cluster_label)
                    )

                # Use dictionary plus template format string to create name.
                # To avoid # key errors from generic patterns, use defaultdict.
                cluster_name = template.format_map(defaultdict(lambda: "", info))
                cluster_name = re.sub("_+", "_", cluster_name)

                # Some dimensions contain slash which must be replaced.
                cluster_name = re.sub("/", "_", cluster_name)
                _LOG.debug("cluster_name = %s", cluster_name)

                # Save mapping for use when creating dependencies.
                quantum_to_cluster[qnode.nodeId] = cluster_name

                # Add cluster to the ClusteredQuantumGraph.
                # Saving NodeId instead of number because QuantumGraph API
                # requires it for creating per-job QuantumGraphs.
                if cluster_name in cqgraph:
                    cluster = cqgraph.get_cluster(cluster_name)
                else:
                    cluster = QuantaCluster(cluster_name, cluster_label, info)
                    cqgraph.add_cluster(cluster)
                cluster.add_quantum(qnode.nodeId, task_label)

    # Assume any task not handled above is supposed to be 1 cluster = 1 quantum
    for task_def in qgraph.iterTaskGraph():
        if task_def.label not in task_labels:
            _LOG.info("Creating 1-quantum clusters for task %s", task_def.label)
            found, template_data_id = config.search(
                "templateDataId", opt={"curvals": {"curr_pipetask": task_def.label}, "replaceVars": False}
            )
            if found:
                template = "{node_number}_{label}_" + template_data_id
            else:
                template = "{node_number}"

            for qnode in qgraph.getNodesForTask(task_def):
                cluster = QuantaCluster.from_quantum_node(qnode, template)
                cqgraph.add_cluster(cluster)
                quantum_to_cluster[qnode.nodeId] = cluster.name

    # Add cluster dependencies.
    for parent in qgraph:
        # Get child nodes.
        children = qgraph.determineOutputsOfQuantumNode(parent)
        for child in children:
            try:
                if quantum_to_cluster[parent.nodeId] != quantum_to_cluster[child.nodeId]:
                    cqgraph.add_dependency(
                        quantum_to_cluster[parent.nodeId], quantum_to_cluster[child.nodeId]
                    )
            except KeyError as e:  # pragma: no cover
                # For debugging a problem internal to method
                nid = NodeId(e.args[0], qgraph.graphID)
                qnode = qgraph.getQuantumNodeByNodeId(nid)

                print(
                    f"Quanta missing when clustering: {qnode.taskDef.label}, "
                    f"{qnode.quantum.dataId.byName()}"
                )
                raise

    return cqgraph
