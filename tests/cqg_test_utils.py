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
"""ClusteredQuantumGraph-related utilities to support ctrl_bps testing."""

import uuid

from qg_test_utils import make_test_helper

from lsst.ctrl.bps import ClusteredQuantumGraph, QuantaCluster


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
    cqg.validate()

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
        Cluster name.
    label : `str`
        Cluster label.
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
            if key == "label":
                info["tags_label"] = value
            elif key != "node_number":
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
    assert len(truth["nodes"]) == len(cqg["nodes"]), (
        f"Mismatch number of nodes: truth={len(truth['nodes'])}, cqg={len(cqg['nodes'])}"
    )
    for tkey in truth["nodes"]:
        assert tkey in cqg["nodes"], f"Could not find {tkey} in cqg"
        tnode = truth["nodes"][tkey]
        cnode = cqg["nodes"][tkey]
        assert tnode["label"] == cnode["label"], (
            f"Mismatch cluster label: truth={tnode['label']}, cqg={cnode['label']}"
        )
        assert tnode["dims"] == cnode["dims"], (
            f"Mismatch cluster dims: truth={tnode['dims']}, cqg={cnode['dims']}"
        )
        assert tnode["counts"] == cnode["counts"], (
            f"Mismatch cluster quanta counts: truth={tnode['counts']}, cqg={cnode['counts']}"
        )
    assert set(truth["edges"]) == set(cqg["edges"]), (
        f"Mismatch edges: truth={truth['edges']}, cqg={cqg['edges']}"
    )


#  T1(1,2)    T1(1,4)     T1(3,4)  T5(1,2)  T5(1,4)  T5(3,4)
#   |          |           |
#  T2(1,2)    T2(1,4)     T2(3,4)
#   |   |      |   |       |   |
#   | T2b(1,2) | T2b(1,4)  | T2b(3,4)
#   |          |           |
#  T3(1,2)    T3(1,4)     T3(3,4)
#   |          |           |
#  T4(1,2)    T4(1,4)     T4(3,4)
def make_test_clustered_quantum_graph(outdir):
    """Make a ClusteredQuantumGraph for testing.

    Parameters
    ----------
    outdir : `str`
        Root used for the quantum graph filename stored
        in the ClusteredQuantumGraph.  The quantum graph is always saved to
        this location.

    Returns
    -------
    qgraph : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
        The fake QuantumGraph created for the test
        ClusteredQuantumGraph returned separately.
    cqg : `lsst.ctrl.bps.ClusteredQuantumGraph`
        Clustered quantum graph.
    """
    helper = make_test_helper()
    qgc = helper.make_quantum_graph_builder(output_run="run").finish(attach_datastore_records=False)
    qg_filename = f"{outdir}/test_file.qg"
    qgc.write(qg_filename)
    qgraph = qgc.assemble()
    cqg = ClusteredQuantumGraph("cqg1", qgraph, qg_filename)

    # since random hash ids, create mapping for tests
    test_lookup = {}
    for task_label, quanta_for_task in qgraph.quanta_by_task.items():
        for data_coordinate, quantum_id in quanta_for_task.items():
            data_id = dict(data_coordinate.required)
            key = f"{task_label}_{data_id['D1']}_{data_id['D2']}"
            test_lookup[key] = (quantum_id, cqg.qxgraph.nodes[quantum_id])

    def get_add_quantum_args(key: str) -> tuple[uuid.UUID, str]:
        quantum_id, quantum_info = test_lookup[key]
        return quantum_id, quantum_info["task_label"]

    # T1 -> T2,T3 -> T4  Dim1 = 1, Dim2 = 2
    qc1 = QuantaCluster.from_quantum_info(*test_lookup["T1_1_2"], template="T1_1_2")
    qc23 = QuantaCluster.from_quantum_info(*test_lookup["T2_1_2"], template="T23_1_2")
    qc23.add_quantum(*get_add_quantum_args("T3_1_2"))
    qc23.label = "clusterT2T3"  # update label so doesnt look like only T2
    qc23.tags["label"] = "clusterT2T3"
    cqg.add_cluster([qc23, qc1])  # reversed to check order is corrected in tests
    cqg.add_dependency(qc1, qc23)
    qc4 = QuantaCluster.from_quantum_info(*test_lookup["T4_1_2"], template="T4_1_2")
    cqg.add_cluster(qc4)
    cqg.add_dependency(qc23, qc4)
    qc2b = QuantaCluster.from_quantum_info(*test_lookup["T2b_1_2"], template="T2b_1_2")
    cqg.add_cluster(qc2b)
    cqg.add_dependency(qc23, qc2b)

    # T1 -> T2,T3 -> T4  Dim1 = 1, Dim2 = 4
    qc1 = QuantaCluster.from_quantum_info(*test_lookup["T1_1_4"], template="T1_1_4")
    qc23 = QuantaCluster.from_quantum_info(*test_lookup["T2_1_4"], template="T23_1_4")
    qc23.add_quantum(*get_add_quantum_args("T3_1_4"))
    qc23.label = "clusterT2T3"  # update label so doesnt look like only T2
    qc23.tags["label"] = "clusterT2T3"
    cqg.add_cluster([qc23, qc1])  # reversed to check order is corrected in tests
    cqg.add_dependency(qc1, qc23)
    qc4 = QuantaCluster.from_quantum_info(*test_lookup["T4_1_4"], template="T4_1_4")
    cqg.add_cluster(qc4)
    cqg.add_dependency(qc23, qc4)
    qc2b = QuantaCluster.from_quantum_info(*test_lookup["T2b_1_4"], template="T2b_1_4")
    cqg.add_cluster(qc2b)
    cqg.add_dependency(qc23, qc2b)

    # T1 -> T2,T3 -> T4  Dim1 = 3, Dim2 = 4
    qc1 = QuantaCluster.from_quantum_info(*test_lookup["T1_3_4"], template="T1_3_4")
    qc23 = QuantaCluster.from_quantum_info(*test_lookup["T2_3_4"], template="T23_3_4")
    qc23.add_quantum(*get_add_quantum_args("T3_3_4"))
    qc23.label = "clusterT2T3"  # update label so doesnt look like only T2
    qc23.tags["label"] = "clusterT2T3"
    cqg.add_cluster([qc23, qc1])  # reversed to check order is corrected in tests
    cqg.add_dependency(qc1, qc23)
    qc4 = QuantaCluster.from_quantum_info(*test_lookup["T4_3_4"], template="T4_3_4")
    cqg.add_cluster(qc4)
    cqg.add_dependency(qc23, qc4)
    qc2b = QuantaCluster.from_quantum_info(*test_lookup["T2b_3_4"], template="T2b_3_4")
    cqg.add_cluster(qc2b)
    cqg.add_dependency(qc23, qc2b)

    # Add remaining
    cluster = QuantaCluster.from_quantum_info(*test_lookup["T5_1_2"], template="T5_1_2")
    cqg.add_cluster(cluster)
    cluster = QuantaCluster.from_quantum_info(*test_lookup["T5_1_4"], template="T5_1_4")
    cqg.add_cluster(cluster)
    cluster = QuantaCluster.from_quantum_info(*test_lookup["T5_3_4"], template="T5_3_4")
    cqg.add_cluster(cluster)

    return qgraph, cqg
