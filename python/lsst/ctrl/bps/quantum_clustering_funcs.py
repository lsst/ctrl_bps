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

"""Functions that convert QuantumGraph into ClusteredQuantumGraph."""

import logging
import re
from collections import defaultdict
from typing import Any
from uuid import UUID

from networkx import DiGraph, NetworkXNoCycle, find_cycle, topological_sort

from lsst.pipe.base import QuantumGraph, QuantumNode

from . import BpsConfig, ClusteredQuantumGraph, QuantaCluster

_LOG = logging.getLogger(__name__)


def single_quantum_clustering(config: BpsConfig, qgraph: QuantumGraph, name: str) -> ClusteredQuantumGraph:
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


def _check_clusters_tasks(
    cluster_config: BpsConfig, task_graph: DiGraph
) -> tuple[list[str], dict[str, list[str]]]:
    """Check cluster definitions in terms of pipetask lists.

    Parameters
    ----------
    cluster_config : `lsst.ctrl.bps.BpsConfig`
        The cluster section from the BPS configuration.
    task_graph : `networkx.DiGraph`
        Directed graph of tasks.

    Returns
    -------
    cluster_labels: `list` [`str`]
        Dependency ordered list of cluster labels (includes
        single quantum clusters).
    ordered_tasks : `dict` [`str`, `networkx.DiGraph`]
        Mapping of cluster label to task subgraph.

    Raises
    ------
    RuntimeError
        Raised if task label appears in more than one cluster def or
        if there's a cycle in the cluster defs.
    """
    # Build a PipelineTask graph of just labels because PipelineGraph
    # methods revolve around NodeKey instead of labels.
    label_graph = DiGraph()
    for node_key in task_graph:
        label_graph.add_node(node_key.name)
        for parent in task_graph.predecessors(node_key):
            label_graph.add_edge(parent.name, node_key.name)

    # Build a "clustered" task graph to check for cycle.
    task_to_cluster = {}
    used_labels = set()
    clustered_task_graph = DiGraph()
    ordered_tasks = {}  # cluster label to ordered list of task labels

    # Create clusters based on given configuration.
    for cluster_label in cluster_config:
        _LOG.debug("cluster = %s", cluster_label)
        cluster_tasks = [pt.strip() for pt in cluster_config[cluster_label]["pipetasks"].split(",")]
        cluster_tasks_in_qgraph = []
        for task_label in cluster_tasks:
            if task_label in used_labels:
                raise RuntimeError(
                    f"Task label {task_label} appears in more than one cluster definition.  "
                    "Aborting submission."
                )
            # Only check cluster defs that affect the QuantumGraph
            if label_graph.has_node(task_label):
                cluster_tasks_in_qgraph.append(task_label)
                used_labels.add(task_label)
                task_to_cluster[task_label] = cluster_label

        if cluster_tasks_in_qgraph:
            # Ensure have list of tasks in dependency order.
            quantum_subgraph = label_graph.subgraph(cluster_tasks_in_qgraph)
            ordered_tasks[cluster_label] = quantum_subgraph

            clustered_task_graph.add_node(cluster_label)

    # Create single task clusters for tasks not covered by clusters.
    for label in label_graph:
        if label not in used_labels:
            task_to_cluster[label] = label
            clustered_task_graph.add_node(label)
            ordered_tasks[label] = label_graph.subgraph([label])

    # Create dependencies between clusters.
    for edge in task_graph.edges:
        if task_to_cluster[edge[0].name] != task_to_cluster[edge[1].name]:
            clustered_task_graph.add_edge(task_to_cluster[edge[0].name], task_to_cluster[edge[1].name])

    _LOG.debug("clustered_task_graph.edges = %s", list(clustered_task_graph.edges))

    # Check if DAG:  DiGraph enforces direction, so need to check for cycles
    try:
        cycle = find_cycle(clustered_task_graph)
    except NetworkXNoCycle:
        _LOG.debug("Did not find a cycle in the clustered_task_graph")
    else:
        _LOG.error(
            "Found cycle when making clusters: %s.  Typically this means a PipelineTask needs to be added to"
            " a cluster or removed from a cluster.",
            cycle,
        )
        raise RuntimeError("Cluster pipetasks do not create a DAG")

    return list(topological_sort(clustered_task_graph)), ordered_tasks


def dimension_clustering(config: BpsConfig, qgraph: QuantumGraph, name: str) -> ClusteredQuantumGraph:
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
    )

    # save mapping in order to create dependencies later
    quantum_to_cluster: dict[UUID, str] = {}

    cluster_section = config["cluster"]
    cluster_labels, ordered_tasks = _check_clusters_tasks(
        cluster_section, qgraph.pipeline_graph.make_task_xgraph()
    )
    for cluster_label in cluster_labels:
        _LOG.debug("cluster = %s", cluster_label)
        if cluster_label in cluster_section:
            if "findDependencyMethod" in cluster_section[cluster_label]:
                add_func = add_dim_clusters_dependency
            else:
                add_func = add_dim_clusters

            add_func(
                cluster_section[cluster_label],
                cluster_label,
                qgraph,
                ordered_tasks,
                cqgraph,
                quantum_to_cluster,
            )
        else:
            add_clusters_per_quantum(config, cluster_label, qgraph, cqgraph, quantum_to_cluster)

    return cqgraph


def add_clusters_per_quantum(
    config: BpsConfig,
    label: str,
    qgraph: QuantumGraph,
    cqgraph: ClusteredQuantumGraph,
    quantum_to_cluster: dict[UUID, str],
) -> None:
    """Add 1-quantum clusters for a task to a ClusteredQuantumGraph.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    label : `str`
        The taskDef label for which to add clusters.
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph providing quanta for the clusters.
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        The ClusteredQuantumGraph to which the new 1-quantum
        clusters are added (modified in method).
    quantum_to_cluster : `dict` [ `uuid.UUID`, `str` ]
        Mapping of quantum node id to which cluster it was added
        (modified in method).
    """
    _LOG.info("Creating 1-quantum clusters for task %s", label)
    found, template_data_id = config.search(
        "templateDataId", opt={"curvals": {"curr_pipetask": label}, "replaceVars": False}
    )
    if found:
        template = "{node_number}_{label}_" + template_data_id
    else:
        template = "{node_number}"

    # Currently getQuantaForTask is currently a mapping taskDef to
    # Quanta, so quick enough to call repeatedly.
    task_def = qgraph.findTaskDefByLabel(label)
    assert task_def is not None, f"Given taskDef label ({label}) not found in QuantumGraph"  # for mypy
    quantum_nodes = qgraph.getNodesForTask(task_def)

    for qnode in quantum_nodes:
        cluster = QuantaCluster.from_quantum_node(qnode, template)
        cqgraph.add_cluster(cluster)
        quantum_to_cluster[qnode.nodeId] = cluster.name
        add_cluster_dependencies(cqgraph, cluster, quantum_to_cluster)


def add_dim_clusters(
    cluster_config: BpsConfig,
    cluster_label: str,
    qgraph: QuantumGraph,
    ordered_tasks: dict[str, DiGraph],
    cqgraph: ClusteredQuantumGraph,
    quantum_to_cluster: dict[UUID, str],
) -> None:
    """Add clusters for a cluster label to a ClusteredQuantumGraph.

    Parameters
    ----------
    cluster_config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration for specific cluster label.
    cluster_label : `str`
        Cluster label for which to add clusters.
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph providing quanta for the clusters.
    ordered_tasks : `dict` [`str`, `networkx.DiGraph`]
        Mapping of cluster label to task label subgraph.
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        The ClusteredQuantumGraph to which the new 1-quantum
        clusters are added (modified in method).
    quantum_to_cluster : `dict` [ `uuid.UUID`, `str` ]
        Mapping of quantum node id to which cluster it was added
        (modified in method).
    """
    cluster_dims = []
    if "dimensions" in cluster_config:
        cluster_dims = [d.strip() for d in cluster_config["dimensions"].split(",")]
    _LOG.debug("cluster_dims = %s", cluster_dims)
    equal_dims = cluster_config.get("equalDimensions", None)

    found, template = cluster_config.search("clusterTemplate", opt={"replaceVars": False})
    if not found:
        if cluster_dims:
            template = f"{cluster_label}_" + "_".join(f"{{{dim}}}" for dim in cluster_dims)
        else:
            template = cluster_label
    _LOG.debug("template = %s", template)

    new_clusters = []
    for task_label in topological_sort(ordered_tasks[cluster_label]):
        # Determine cluster for each node
        task_def = qgraph.findTaskDefByLabel(task_label)
        assert task_def is not None  # for mypy
        for node in qgraph.getNodesForTask(task_def):
            cluster_name, info = get_cluster_name_from_node(
                node, cluster_dims, cluster_label, template, equal_dims
            )

            # Save mapping for use when creating dependencies.
            quantum_to_cluster[node.nodeId] = cluster_name

            # Add cluster to the ClusteredQuantumGraph.
            # Saving NodeId instead of number because QuantumGraph API
            # requires it for creating per-job QuantumGraphs.
            if cluster_name in cqgraph:
                cluster = cqgraph.get_cluster(cluster_name)
            else:
                cluster = QuantaCluster(cluster_name, cluster_label, info)
                cqgraph.add_cluster(cluster)
            cluster.add_quantum(node.nodeId, task_label)
            new_clusters.append(cluster)

    for cluster in new_clusters:
        add_cluster_dependencies(cqgraph, cluster, quantum_to_cluster)


def add_cluster_dependencies(
    cqgraph: ClusteredQuantumGraph, cluster: QuantaCluster, quantum_to_cluster: dict[UUID, str]
) -> None:
    """Add dependencies for a cluster within a ClusteredQuantumGraph.

    Parameters
    ----------
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        The ClusteredQuantumGraph to which the new 1-quantum
        clusters are added (modified in method).
    cluster : `lsst.ctrl.bps.QuantaCluster`
        The cluster for which to add dependencies.
    quantum_to_cluster : `dict` [ `uuid.UUID`, `str` ]
        Mapping of quantum node id to which cluster it was added
        (modified in method).

    Raises
    ------
    KeyError
        Raised if any of the cluster's quantum node ids are missing
        from quantum_to_cluster or if their parent quantum node ids
        are missing from quantum_to_cluster.
    """
    qgraph = cqgraph.qgraph
    for node_id in cluster.qgraph_node_ids:
        cluster_node = qgraph.getQuantumNodeByNodeId(node_id)
        parents = qgraph.determineInputsToQuantumNode(cluster_node)
        for parent in parents:
            try:
                if quantum_to_cluster[parent.nodeId] != quantum_to_cluster[node_id]:
                    cqgraph.add_dependency(quantum_to_cluster[parent.nodeId], quantum_to_cluster[node_id])
            except KeyError as e:  # pragma: no cover
                # For debugging a problem internal to method
                qnode = qgraph.getQuantumNodeByNodeId(e.args[0])
                _LOG.error(
                    "Quanta missing when clustering: cluster node = %s, %s; missing = %s, %s",
                    cluster_node.taskDef.label,
                    cluster_node.quantum.dataId,
                    qnode.taskDef.label,
                    qnode.quantum.dataId,
                )
                _LOG.error(quantum_to_cluster)
                raise


def add_dim_clusters_dependency(
    cluster_config: BpsConfig,
    cluster_label: str,
    qgraph: QuantumGraph,
    ordered_tasks: dict[str, DiGraph],
    cqgraph: ClusteredQuantumGraph,
    quantum_to_cluster: dict[UUID, str],
) -> None:
    """Add clusters for a cluster label to a ClusteredQuantumGraph using
       QuantumGraph dependencies as well as dimension values to help when
       some do not have particular dimension value.

    Parameters
    ----------
    cluster_config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration for specific cluster label.
    cluster_label : `str`
        Cluster label for which to add clusters.
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph providing quanta for the clusters.
    ordered_tasks : `dict` [`str`, `networkx.DiGraph`]
        Mapping of cluster label to task label subgraph.
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        The ClusteredQuantumGraph to which the new
        clusters are added (modified in method).
    quantum_to_cluster : `dict` [ `str`, `str` ]
        Mapping of quantum node id to which cluster it was added
        (modified in method).
    """
    cluster_dims = []
    if "dimensions" in cluster_config:
        cluster_dims = [d.strip() for d in cluster_config["dimensions"].split(",")]
    _LOG.debug("cluster_dims = %s", cluster_dims)
    equal_dims = cluster_config.get("equalDimensions", None)

    found, template = cluster_config.search("clusterTemplate", opt={"replaceVars": False})
    if not found:
        if cluster_dims:
            template = f"{cluster_label}_" + "_".join(f"{{{dim}}}" for dim in cluster_dims)
        else:
            template = cluster_label
    _LOG.debug("template = %s", template)

    # Note:  Can't use just source/sink labels in case some quanta for those
    #        aren't in the QuantumGraph (e.g., a rescue QuantumGraph)
    label_search_order = list(topological_sort(ordered_tasks[cluster_label]))
    method = cluster_config["findDependencyMethod"]
    match method:
        case "source":
            find_possible_nodes = qgraph.determineOutputsOfQuantumNode
        case "sink":
            find_possible_nodes = qgraph.determineInputsToQuantumNode
            label_search_order.reverse()
        case _:
            raise RuntimeError(f"Invalid findDependencyMethod ({method})")
    _LOG.info("label_search_order = %s", label_search_order)

    new_clusters = []
    for task_label in label_search_order:
        task_def = qgraph.findTaskDefByLabel(task_label)
        assert task_def is not None  # for mypy
        for node in qgraph.getNodesForTask(task_def):
            # Since not only using source/sink labels, might look at
            # node more than once.
            if node.nodeId in quantum_to_cluster:
                continue

            cluster_name, info = get_cluster_name_from_node(
                node, cluster_dims, cluster_label, template, equal_dims
            )

            # Add cluster to the ClusteredQuantumGraph.
            # Saving NodeId instead of number because QuantumGraph API
            # requires it for creating per-job QuantumGraphs.
            if cluster_name in cqgraph:
                cluster = cqgraph.get_cluster(cluster_name)
            else:
                cluster = QuantaCluster(cluster_name, cluster_label, info)
                cqgraph.add_cluster(cluster)
                new_clusters.append(cluster)
            cluster.add_quantum(node.nodeId, task_label)

            # Save mapping for use when creating dependencies.
            quantum_to_cluster[node.nodeId] = cluster_name

            # Use dependencies to find other quantum to add
            # Note: in testing, using the following code was faster than
            # using networkx descendants and ancestors functions
            # While traversing the QuantumGraph, nodes may appear
            # repeatedly in possible_nodes.
            nodes_to_use = [node]
            while nodes_to_use:
                node_to_use = nodes_to_use.pop()
                possible_nodes = find_possible_nodes(node_to_use)
                for possible_node in possible_nodes:
                    if possible_node.taskDef.label in ordered_tasks[cluster_label]:
                        if possible_node.nodeId not in cluster.qgraph_node_ids:
                            _LOG.debug(
                                "Adding possible quantum %s (%s) to cluster %s",
                                possible_node.nodeId,
                                possible_node.taskDef.label,
                                cluster_name,
                            )
                            cluster.add_quantum(possible_node.nodeId, possible_node.taskDef.label)
                            quantum_to_cluster[possible_node.nodeId] = cluster_name
                            nodes_to_use.append(possible_node)
                    else:
                        _LOG.debug(
                            "label (%s) not in ordered_tasks. Not adding possible quantum %s",
                            possible_node.taskDef.label,
                            possible_node.nodeId,
                        )

    for cluster in new_clusters:
        add_cluster_dependencies(cqgraph, cluster, quantum_to_cluster)


def get_cluster_name_from_node(
    node: QuantumNode,
    cluster_dims: list[str],
    cluster_label: str,
    template: str,
    equal_dims: str,
) -> tuple[str, dict[str, Any]]:
    """Get the cluster name in which to add the given node.

    Parameters
    ----------
    node : `lsst.pipe.base.QuantumNode`
        QuantumNode from which to create the cluster.
    cluster_dims : `list` [ `str` ]
        List of dimension names to be used when clustering.
    cluster_label : `str`
        Cluster label.
    template : `str`
        Template for the cluster name.
    equal_dims : `str`
        Configuration describing any dimensions to be considered equal.

    Returns
    -------
    cluster_name : `str`
        Name of the cluster in which to add the given node.
    info : dict [`str`, `Any`]
        Information needed if creating a new node.
    """
    # Gather info for cluster name template into a dictionary.
    info: dict[str, Any] = {"node_number": node.nodeId}

    missing_info = set()
    assert node.quantum.dataId is not None  # for mypy
    data_id_info = dict(node.quantum.dataId.mapping)
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
            f"Quantum {node.nodeId} ({data_id_info}) missing dimensions: {','.join(missing_info)}; "
            f"required for cluster {cluster_label}"
        )

    # Use dictionary plus template format string to create name.
    # To avoid # key errors from generic patterns, use defaultdict.
    cluster_name = template.format_map(defaultdict(lambda: "", info))
    cluster_name = re.sub("_+", "_", cluster_name)

    # Some dimensions contain slash which must be replaced.
    cluster_name = re.sub("/", "_", cluster_name)
    _LOG.debug("cluster_name = %s", cluster_name)

    return cluster_name, info
