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
"""ClusteredQuantumGraph-related utilities to support ctrl_bps testing.
"""

import uuid

from lsst.ctrl.bps.quantum_clustering_funcs import dimension_clustering
from networkx import is_directed_acyclic_graph
from qg_test_utils import make_test_quantum_graph


def check_cqg(cqg, truth=None):
    """Check ClusteredQuantumGraph for correctness used by unit
    tests.

    Parameters
    ----------
    cqg : `lsst.ctrl.bps.ClusteredQuantumGraph`
        ClusteredQuantumGraph to be checked for correctness.
    truth : `dict` [`str`, `Any`], optional
        Information describing what this cluster should look like.
    """
    # Checks independent of data

    # Check no cycles, only one edge between same two nodes,
    assert is_directed_acyclic_graph(cqg._cluster_graph)

    # Check has all QGraph nodes (include message about duplicate node).
    node_ids = set()
    cl_by_label = {}
    for cluster in cqg.clusters():
        cl_by_label.setdefault(cluster.label, []).append(cluster)
        for id_ in cluster.qgraph_node_ids:
            qnode = cqg.get_quantum_node(id_)
            assert id_ not in node_ids, (
                f"Checking cluster {cluster.name}, id {id_} ({qnode.quantum.dataId.byName()}) appears more "
                "than once in CQG."
            )
            node_ids.add(id_)
    assert len(node_ids) == len(cqg._quantum_graph)

    # If given what should be there, check values.
    if truth:
        cqg_info = dump_cqg(cqg)
        compare_cqg_dicts(truth, cqg_info)


def replace_node_name(name, label, dims):
    """Replace node id in cluster name because they
    change every run and thus make testing difficult.

    Parameters
    ----------
    name : `str`
        Cluster name
    label : `str`
        Cluster label
    dims : `dict` [`str`, `Any`]
        Dimension names and values in order to make new name unique.

    Returns
    -------
    name : `str`
        New name of cluster.
    """
    try:
        name_parts = name.split("_")
        _ = uuid.UUID(name_parts[0])
        if len(name_parts) == 1:
            name = f"NODEONLY_{label}_{str(dims)}"
        else:
            name = f"NODENAME_{'_'.join(name_parts[1:])}"
    except ValueError:
        pass
    return name


def dump_cqg(cqg):
    """Represent ClusteredQuantumGraph as dictionary for testing.

    Parameters
    ----------
    cqg : `lsst.ctrl.bps.ClusteredQuantumGraph`
        ClusteredQuantumGraph to be represented as a dictionary.

    Returns
    -------
    info : `dict` [`str`, `Any`]
        Dictionary represention of ClusteredQuantumGraph.
    """
    info = {"name": cqg.name, "nodes": {}}

    orig_to_new = {}
    for cluster in cqg.clusters():
        dims = {}
        for key, value in cluster.tags.items():
            if key not in ["label", "node_number"]:
                dims[key] = value
        name = replace_node_name(cluster.name, cluster.label, dims)
        orig_to_new[cluster.name] = name
        info["nodes"][name] = {"label": cluster.label, "dims": dims, "counts": dict(cluster.quanta_counts)}

    info["edges"] = []
    for edge in cqg._cluster_graph.edges:
        info["edges"].append((orig_to_new[edge[0]], orig_to_new[edge[1]]))

    return info


def compare_cqg_dicts(truth, cqg):
    """Compare dicts representing two ClusteredQuantumGraphs.

    Parameters
    ----------
    truth : `dict` [`str`, `Any`]
        Representation of the expected ClusteredQuantumGraph.
    cqg : `dict` [`str`, `Any`]
        Representation of the calculated ClusteredQuantumGraph.

    Raises
    ------
    AssertionError
        Whenever discover discrepancy between dicts.
    """
    assert truth["name"] == cqg["name"], f"Mismatch name: truth={truth['name']}, cqg={cqg['name']}"
    assert len(truth["nodes"]) == len(
        cqg["nodes"]
    ), f"Mismatch number of nodes: truth={len(truth['nodes'])}, cqg={len(cqg['nodes'])}"
    for tkey in truth["nodes"]:
        assert tkey in cqg["nodes"], f"Could not find {tkey} in cqg"
        tnode = truth["nodes"][tkey]
        cnode = cqg["nodes"][tkey]
        assert (
            tnode["label"] == cnode["label"]
        ), f"Mismatch cluster label: truth={tnode['label']}, cqg={cnode['label']}"
        assert (
            tnode["dims"] == cnode["dims"]
        ), f"Mismatch cluster dims: truth={tnode['dims']}, cqg={cnode['dims']}"
        assert (
            tnode["counts"] == cnode["counts"]
        ), f"Mismatch cluster quanta counts: truth={tnode['counts']}, cqg={cnode['counts']}"
    assert set(truth["edges"]) == set(
        cqg["edges"]
    ), f"Mismatch edges: truth={truth['edges']}, cqg={cqg['edges']}"


def make_test_clustered_quantum_graph(config):
    qgraph = make_test_quantum_graph()
    cqg = dimension_clustering(config, qgraph, "test_cqg")
    return cqg
