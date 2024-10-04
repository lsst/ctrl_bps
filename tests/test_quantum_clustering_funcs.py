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
"""Unit tests for the clustering methods.
"""

# Turn off "doesn't conform to snake_case naming style" because matching
# the unittest casing.
# pylint: disable=invalid-name

import os
import unittest

from cqg_test_utils import check_cqg
from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.quantum_clustering_funcs import dimension_clustering, single_quantum_clustering
from qg_test_utils import make_test_quantum_graph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestSingleQuantumClustering(unittest.TestCase):
    """Tests for single_quantum_clustering method."""

    def setUp(self):
        self.qgraph = make_test_quantum_graph()

    def tearDown(self):
        pass

    def testClustering(self):
        """Test valid single quantum clustering."""
        # Note: the cluster config should be ignored.
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {"cl1": {"pipetasks": "T1, T2, T3, T4", "dimensions": "D1, D2"}},
            }
        )

        cqg = single_quantum_clustering(config, self.qgraph, "single")
        self.assertIsNotNone(cqg)
        self.assertIn(cqg.name, "single")
        self.assertEqual(len(cqg), len(self.qgraph))

    def testClusteringNoTemplate(self):
        """Test valid single quantum clustering wihtout a template for the
        cluster names.
        """
        # Note: the cluster config should be ignored.
        config = BpsConfig({"cluster": {"cl1": {"pipetasks": "T1, T2, T3, T4", "dimensions": "D1, D2"}}})

        cqg = single_quantum_clustering(config, self.qgraph, "single-no-template")
        self.assertIsNotNone(cqg)
        self.assertIn(cqg.name, "single-no-template")
        self.assertEqual(len(cqg), len(self.qgraph))


class TestDimensionClustering(unittest.TestCase):
    """Tests for dimension_clustering method."""

    def setUp(self):
        self.qgraph = make_test_quantum_graph()

    def tearDown(self):
        pass

    def testClusterAllInOne(self):
        """All tasks in one cluster."""
        name = "all-in-one"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {"cl1": {"pipetasks": "T1, T2, T3, T4", "dimensions": "D1, D2"}},
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
            },
            "edges": [],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterTemplate(self):
        """Test uses clusterTemplate value to name clusters."""
        name = "cluster-template"
        config = BpsConfig(
            {
                "templateDataId": "tdid_{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "clusterTemplate": "ct_{D1}_{D2}_{D3}_{D4}",
                        "pipetasks": "T1, T2, T3, T4",
                        "dimensions": "D1, D2",
                    }
                },
            }
        )
        # Note: clusterTemplate can produce trailing underscore
        answer = {
            "name": name,
            "nodes": {
                "ct_1_2_": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "ct_1_4_": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "ct_3_4_": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
            },
            "edges": [],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterNoDims(self):
        """Test if clusters have no dimensions."""
        name = "cluster-no-dims"
        config = BpsConfig(
            {
                "templateDataId": "tdid_{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                    },
                    "cl2": {
                        "pipetasks": "T3, T4",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1": {"label": "cl1", "dims": {}, "counts": {"T1": 3, "T2": 3}},
                "cl2": {"label": "cl2", "dims": {}, "counts": {"T3": 3, "T4": 3}},
            },
            "edges": [("cl1", "cl2")],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterTaskRepeat(self):
        """Can't have PipelineTask in more than one cluster."""
        name = "task-repeat"
        config = BpsConfig(
            {
                "templateDataId": "tdid_{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                    },
                    "cl2": {
                        "pipetasks": "T2, T3, T4",
                    },
                },
            }
        )

        with self.assertRaises(RuntimeError):
            _ = dimension_clustering(config, self.qgraph, name)

    def testClusterMissingDimValue(self):
        """Quantum can't be missing a value for a clustering dimension."""
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {"cl1": {"pipetasks": "T1, T2, T3, T4", "dimensions": "D1, NotThere"}},
            }
        )

        with self.assertRaises(RuntimeError):
            _ = dimension_clustering(config, self.qgraph, "missing-dim-value")

    def testClusterEqualDim1(self):
        """Test equalDimensions using right half."""
        name = "equal-dim"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3, T4",
                        "dimensions": "D1, NotThere",
                        "equalDimensions": "NotThere:D2",
                    }
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "NotThere": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
            },
            "edges": [],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterEqualDim2(self):
        """Test equalDimensions using left half."""
        name = "equal-dim-2"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3, T4",
                        "dimensions": "D1, NotThere",
                        "equalDimensions": "D2:NotThere",
                    }
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "NotThere": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
            },
            "edges": [],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterMult(self):
        """Test multiple tasks in multiple clusters."""
        name = "cluster-mult"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T2", "dimensions": "D1, D2"},
                    "cl2": {"pipetasks": "T3, T4", "dimensions": "D1, D2"},
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl2_1_2": {"label": "cl2", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1, "T4": 1}},
                "cl2_1_4": {"label": "cl2", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "cl2_3_4": {"label": "cl2", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
            },
            "edges": [("cl1_3_4", "cl2_3_4"), ("cl1_1_2", "cl2_1_2"), ("cl1_1_4", "cl2_1_4")],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterPart(self):
        """Test will use templateDataId if no clusterTemplate."""
        name = "cluster-part"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T2", "dimensions": "D1, D2"},
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "NODENAME_T3_1_2_": {"label": "T3", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1}},
                "NODENAME_T3_1_4_": {"label": "T3", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T3_3_4_": {"label": "T3", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T3_1_2_"),
                ("cl1_3_4", "NODENAME_T3_3_4_"),
                ("cl1_1_4", "NODENAME_T3_1_4_"),
            ],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterPartNoTemplate(self):
        """No templateDataId nor clusterTemplate (use cluster label)."""
        name = "cluster-part-no-template"
        config = BpsConfig(
            {
                "cluster": {
                    "cl1": {"pipetasks": "T1, T2"},
                }
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1": {"label": "cl1", "dims": {}, "counts": {"T1": 3, "T2": 3}},
                "NODEONLY_T3_{'D1': 1, 'D2': 2}": {
                    "label": "T3",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T3": 1},
                },
                "NODEONLY_T3_{'D1': 1, 'D2': 4}": {
                    "label": "T3",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T3": 1},
                },
                "NODEONLY_T3_{'D1': 3, 'D2': 4}": {
                    "label": "T3",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T3": 1},
                },
                "NODEONLY_T4_{'D1': 1, 'D2': 2}": {
                    "label": "T4",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T4": 1},
                },
                "NODEONLY_T4_{'D1': 1, 'D2': 4}": {
                    "label": "T4",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T4": 1},
                },
                "NODEONLY_T4_{'D1': 3, 'D2': 4}": {
                    "label": "T4",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T4": 1},
                },
            },
            "edges": [
                ("cl1", "NODEONLY_T3_{'D1': 1, 'D2': 2}"),
                ("cl1", "NODEONLY_T3_{'D1': 1, 'D2': 4}"),
                ("cl1", "NODEONLY_T3_{'D1': 3, 'D2': 4}"),
            ],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterExtra(self):
        """Clustering includes labels of pipetasks that aren't in QGraph.
        They should just be ignored.
        """
        name = "extra"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, Extra1, T2, Extra2, T3, T4", "dimensions": "D1, D2"},
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1, "T4": 1},
                },
            },
            "edges": [],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterRepeat(self):
        """A PipelineTask appears in more than one cluster definition."""
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T2, T3, T4", "dimensions": "D1, D2"},
                    "cl2": {"pipetasks": "T2", "dimensions": "D1, D2"},
                },
            }
        )

        with self.assertRaises(RuntimeError):
            _ = dimension_clustering(config, self.qgraph, "repeat-task")

    def testClusterDepends(self):
        """Part of a chain of PipelineTask appears in different cluster."""
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T3, T4", "dimensions": "D1, D2"},
                    "cl2": {"pipetasks": "T2", "dimensions": "D1, D2"},
                },
            }
        )

        with self.assertRaises(RuntimeError):
            _ = dimension_clustering(config, self.qgraph, "task-depends")

    def testClusterOrder(self):
        """Ensure clusters method is in topological order as some
        uses require to always have processed parent before
        children.
        """
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl2": {"pipetasks": "T2, T3", "dimensions": "D1, D2"},
                },
            }
        )

        cqg = dimension_clustering(config, self.qgraph, "task-cluster-order")
        processed = set()
        for cluster in cqg.clusters():
            for parent in cqg.predecessors(cluster.name):
                self.assertIn(
                    parent.name,
                    processed,
                    f"clusters() returned {cluster.name} before its parent {parent.name}",
                )
            processed.add(cluster.name)

    def testAddClusterDependenciesSink(self):
        name = "AddClusterDependenciesSink"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "sink",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
            },
            "edges": [],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testAddClusterDependenciesSource(self):
        name = "AddClusterDependenciesSource"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "source",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
            },
            "edges": [],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testAddClusterDependenciesExtra(self):
        # Test if dependencies return more than in cluster
        name = "AddClusterDependenciesExtra"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "source",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1},
                },
                "NODENAME_T3_1_2_": {"label": "T3", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1}},
                "NODENAME_T3_1_4_": {"label": "T3", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T3_3_4_": {"label": "T3", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T3_1_2_"),
                ("cl1_1_4", "NODENAME_T3_1_4_"),
                ("cl1_3_4", "NODENAME_T3_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testAddClusterDependenciesSameCluster(self):
        name = "AddClusterDependenciesSameCluster"
        config = BpsConfig(
            {
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3",
                        "dimensions": "D1",
                        "findDependencyMethod": "source",
                        "clusterTemplate": "ct_{D1}_{D2}_{D3}_{D4}",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "ct_1_": {
                    "label": "cl1",
                    "dims": {"D1": 1},
                    "counts": {"T1": 2, "T2": 2, "T3": 2},
                },
                "ct_3_": {
                    "label": "cl1",
                    "dims": {"D1": 3},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "NODEONLY_T4_{'D1': 1, 'D2': 2}": {
                    "label": "T4",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T4": 1},
                },
                "NODEONLY_T4_{'D1': 1, 'D2': 4}": {
                    "label": "T4",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T4": 1},
                },
                "NODEONLY_T4_{'D1': 3, 'D2': 4}": {
                    "label": "T4",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T4": 1},
                },
            },
            "edges": [],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testAddClusterDependenciesBadMethod(self):
        name = "AddClusterDependenciesBadMethod"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2, T3",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "bad",
                    },
                },
            }
        )
        with self.assertRaises(RuntimeError):
            _ = dimension_clustering(config, self.qgraph, name)


if __name__ == "__main__":
    unittest.main()
