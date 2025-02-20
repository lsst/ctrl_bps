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
"""Unit tests for the clustering methods."""

# Turn off "doesn't conform to snake_case naming style" because matching
# the unittest casing.
# pylint: disable=invalid-name

import logging
import os
import unittest

from cqg_test_utils import check_cqg
from qg_test_utils import make_test_quantum_graph

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.quantum_clustering_funcs import dimension_clustering, single_quantum_clustering

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
                "cluster": {"cl1": {"pipetasks": "T1, T2, T2b, T3, T4", "dimensions": "D1, D2"}},
            }
        )

        name = "single"
        cqg = single_quantum_clustering(config, self.qgraph, name)
        answer = {
            "name": name,
            "nodes": {
                "NODENAME_T1_1_2_": {"label": "T1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1}},
                "NODENAME_T1_1_4_": {"label": "T1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1}},
                "NODENAME_T1_3_4_": {"label": "T1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1}},
                "NODENAME_T2_1_2_": {"label": "T2", "dims": {"D1": 1, "D2": 2}, "counts": {"T2": 1}},
                "NODENAME_T2_1_4_": {"label": "T2", "dims": {"D1": 1, "D2": 4}, "counts": {"T2": 1}},
                "NODENAME_T2_3_4_": {"label": "T2", "dims": {"D1": 3, "D2": 4}, "counts": {"T2": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T3_1_2_": {"label": "T3", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1}},
                "NODENAME_T3_1_4_": {"label": "T3", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T3_3_4_": {"label": "T3", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("NODENAME_T1_1_2_", "NODENAME_T2_1_2_"),
                ("NODENAME_T1_1_4_", "NODENAME_T2_1_4_"),
                ("NODENAME_T1_3_4_", "NODENAME_T2_3_4_"),
                ("NODENAME_T2_1_2_", "NODENAME_T2b_1_2_"),
                ("NODENAME_T2_1_4_", "NODENAME_T2b_1_4_"),
                ("NODENAME_T2_3_4_", "NODENAME_T2b_3_4_"),
                ("NODENAME_T2_1_2_", "NODENAME_T3_1_2_"),
                ("NODENAME_T2_1_4_", "NODENAME_T3_1_4_"),
                ("NODENAME_T2_3_4_", "NODENAME_T3_3_4_"),
                ("NODENAME_T3_1_2_", "NODENAME_T4_1_2_"),
                ("NODENAME_T3_1_4_", "NODENAME_T4_1_4_"),
                ("NODENAME_T3_3_4_", "NODENAME_T4_3_4_"),
            ],
        }

        check_cqg(cqg, answer)

    def testClusteringNoTemplate(self):
        """Test valid single quantum clustering without a template for the
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
                "cluster": {"cl1": {"pipetasks": "T1, T2, T2b, T3, T4, T5", "dimensions": "D1, D2"}},
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
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
                        "pipetasks": "T1, T2, T2b, T3, T4, T5",
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
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
                },
                "ct_1_4_": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
                },
                "ct_3_4_": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1, "T5": 1},
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
                "NODENAME_T2b_tdid_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_tdid_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_tdid_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "cl2": {"label": "cl2", "dims": {}, "counts": {"T3": 3, "T4": 3}},
                "NODENAME_T5_tdid_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_tdid_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_tdid_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1", "cl2"),
                ("cl1", "NODENAME_T2b_tdid_1_2_"),
                ("cl1", "NODENAME_T2b_tdid_1_4_"),
                ("cl1", "NODENAME_T2b_tdid_3_4_"),
            ],
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
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
            ],
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
                        "pipetasks": "T1, T2, T2b, T3, T4",
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
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "NotThere": 4},
                    "counts": {"T1": 1, "T2": 1, "T2b": 1, "T3": 1, "T4": 1},
                },
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
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
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "cl2_1_2"),
                ("cl1_1_4", "cl2_1_4"),
                ("cl1_3_4", "cl2_3_4"),
            ],
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
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T3_1_2_": {"label": "T3", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1}},
                "NODENAME_T3_1_4_": {"label": "T3", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T3_3_4_": {"label": "T3", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_1_2", "NODENAME_T3_1_2_"),
                ("cl1_3_4", "NODENAME_T3_3_4_"),
                ("cl1_1_4", "NODENAME_T3_1_4_"),
                ("NODENAME_T3_1_2_", "NODENAME_T4_1_2_"),
                ("NODENAME_T3_3_4_", "NODENAME_T4_3_4_"),
                ("NODENAME_T3_1_4_", "NODENAME_T4_1_4_"),
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
                "NODEONLY_T2b_{'D1': 1, 'D2': 2}": {
                    "label": "T2b",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T2b": 1},
                },
                "NODEONLY_T2b_{'D1': 1, 'D2': 4}": {
                    "label": "T2b",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T2b": 1},
                },
                "NODEONLY_T2b_{'D1': 3, 'D2': 4}": {
                    "label": "T2b",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T2b": 1},
                },
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
                "NODEONLY_T5_{'D1': 1, 'D2': 2}": {
                    "label": "T5",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T5": 1},
                },
                "NODEONLY_T5_{'D1': 1, 'D2': 4}": {
                    "label": "T5",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T5": 1},
                },
                "NODEONLY_T5_{'D1': 3, 'D2': 4}": {
                    "label": "T5",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T5": 1},
                },
            },
            "edges": [
                ("cl1", "NODEONLY_T2b_{'D1': 1, 'D2': 2}"),
                ("cl1", "NODEONLY_T2b_{'D1': 1, 'D2': 4}"),
                ("cl1", "NODEONLY_T2b_{'D1': 3, 'D2': 4}"),
                ("cl1", "NODEONLY_T3_{'D1': 1, 'D2': 2}"),
                ("cl1", "NODEONLY_T3_{'D1': 1, 'D2': 4}"),
                ("cl1", "NODEONLY_T3_{'D1': 3, 'D2': 4}"),
                ("NODEONLY_T3_{'D1': 1, 'D2': 2}", "NODEONLY_T4_{'D1': 1, 'D2': 2}"),
                ("NODEONLY_T3_{'D1': 1, 'D2': 4}", "NODEONLY_T4_{'D1': 1, 'D2': 4}"),
                ("NODEONLY_T3_{'D1': 3, 'D2': 4}", "NODEONLY_T4_{'D1': 3, 'D2': 4}"),
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
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
            ],
        }

        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testClusterCycle(self):
        """The clustering produces a cycle."""
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {"pipetasks": "T1, T3, T4", "dimensions": "D1, D2"},
                    "cl2": {"pipetasks": "T2", "dimensions": "D1, D2"},
                },
            }
        )

        with self.assertLogs("lsst.ctrl.bps.quantum_clustering_funcs", level=logging.ERROR) as cm:
            with self.assertRaisesRegex(RuntimeError, "Cluster pipetasks do not create a DAG"):
                _ = dimension_clustering(config, self.qgraph, "cycle")
            self.assertRegex(cm.records[-1].getMessage(), "Found cycle when making clusters: .*")

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

    def testDependenciesSink(self):
        name = "DependenciesSink"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T2, T3",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "sink",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "NODENAME_T1_1_2_": {"label": "T1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1}},
                "NODENAME_T1_1_4_": {"label": "T1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1}},
                "NODENAME_T1_3_4_": {"label": "T1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1}},
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T2": 1, "T3": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T2": 1, "T3": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T2": 1, "T3": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("NODENAME_T1_1_2_", "cl1_1_2"),
                ("NODENAME_T1_1_4_", "cl1_1_4"),
                ("NODENAME_T1_3_4_", "cl1_3_4"),
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "NODENAME_T4_1_2_"),
                ("cl1_1_4", "NODENAME_T4_1_4_"),
                ("cl1_3_4", "NODENAME_T4_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSink2Clusters(self):
        name = "DependenciesSink2Clusters"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "sink",
                    },
                    "cl2": {
                        "pipetasks": "T3, T4",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "sink",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "cl2_1_2": {"label": "cl2", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1, "T4": 1}},
                "cl2_1_4": {"label": "cl2", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "cl2_3_4": {"label": "cl2", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "cl2_1_2"),
                ("cl1_1_4", "cl2_1_4"),
                ("cl1_3_4", "cl2_3_4"),
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSinkMultDepends(self):
        name = "DependenciesSinkMultClusters"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T2, T3",
                        "findDependencyMethod": "sink",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "NODENAME_T1_1_2_": {"label": "T1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1}},
                "NODENAME_T1_1_4_": {"label": "T1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1}},
                "NODENAME_T1_3_4_": {"label": "T1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1}},
                "cl1": {"label": "cl1", "dims": {}, "counts": {"T2": 3, "T3": 3}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("NODENAME_T1_1_2_", "cl1"),
                ("NODENAME_T1_1_4_", "cl1"),
                ("NODENAME_T1_3_4_", "cl1"),
                ("cl1", "NODENAME_T2b_1_2_"),
                ("cl1", "NODENAME_T2b_1_4_"),
                ("cl1", "NODENAME_T2b_3_4_"),
                ("cl1", "NODENAME_T4_1_2_"),
                ("cl1", "NODENAME_T4_1_4_"),
                ("cl1", "NODENAME_T4_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSource(self):
        name = "DependenciesSource"
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
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "NODENAME_T4_1_2_"),
                ("cl1_1_4", "NODENAME_T4_1_4_"),
                ("cl1_3_4", "NODENAME_T4_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSource2Clusters(self):
        name = "DependenciesSource2Clusters"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "source",
                    },
                    "cl2": {
                        "pipetasks": "T3, T4",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "source",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "cl2_1_2": {"label": "cl2", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1, "T4": 1}},
                "cl2_1_4": {"label": "cl2", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "cl2_3_4": {"label": "cl2", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "cl2_1_2"),
                ("cl1_1_4", "cl2_1_4"),
                ("cl1_3_4", "cl2_3_4"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSourceMultDepends(self):
        name = "DependenciesSourceMultClusters"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T2, T3",
                        "findDependencyMethod": "source",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "NODENAME_T1_1_2_": {"label": "T1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1}},
                "NODENAME_T1_1_4_": {"label": "T1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1}},
                "NODENAME_T1_3_4_": {"label": "T1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1}},
                "cl1": {"label": "cl1", "dims": {}, "counts": {"T2": 3, "T3": 3}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("NODENAME_T1_1_2_", "cl1"),
                ("NODENAME_T1_1_4_", "cl1"),
                ("NODENAME_T1_3_4_", "cl1"),
                ("cl1", "NODENAME_T2b_1_2_"),
                ("cl1", "NODENAME_T2b_1_4_"),
                ("cl1", "NODENAME_T2b_3_4_"),
                ("cl1", "NODENAME_T4_1_2_"),
                ("cl1", "NODENAME_T4_1_4_"),
                ("cl1", "NODENAME_T4_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesExtra(self):
        # Test if dependencies return more than in cluster
        name = "DependenciesExtra"
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
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T3_1_2_": {"label": "T3", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1}},
                "NODENAME_T3_1_4_": {"label": "T3", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T3_3_4_": {"label": "T3", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T3_1_2_"),
                ("cl1_1_4", "NODENAME_T3_1_4_"),
                ("cl1_3_4", "NODENAME_T3_3_4_"),
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("NODENAME_T3_1_2_", "NODENAME_T4_1_2_"),
                ("NODENAME_T3_1_4_", "NODENAME_T4_1_4_"),
                ("NODENAME_T3_3_4_", "NODENAME_T4_3_4_"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSameCluster(self):
        name = "DependenciesSameCluster"
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
                "NODEONLY_T2b_{'D1': 1, 'D2': 2}": {
                    "label": "T2b",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T2b": 1},
                },
                "NODEONLY_T2b_{'D1': 1, 'D2': 4}": {
                    "label": "T2b",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T2b": 1},
                },
                "NODEONLY_T2b_{'D1': 3, 'D2': 4}": {
                    "label": "T2b",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T2b": 1},
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
                "NODEONLY_T5_{'D1': 1, 'D2': 2}": {
                    "label": "T5",
                    "dims": {"D1": 1, "D2": 2},
                    "counts": {"T5": 1},
                },
                "NODEONLY_T5_{'D1': 1, 'D2': 4}": {
                    "label": "T5",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T5": 1},
                },
                "NODEONLY_T5_{'D1': 3, 'D2': 4}": {
                    "label": "T5",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T5": 1},
                },
            },
            "edges": [
                ("ct_1_", "NODEONLY_T2b_{'D1': 1, 'D2': 2}"),
                ("ct_1_", "NODEONLY_T2b_{'D1': 1, 'D2': 4}"),
                ("ct_3_", "NODEONLY_T2b_{'D1': 3, 'D2': 4}"),
                ("ct_1_", "NODEONLY_T4_{'D1': 1, 'D2': 2}"),
                ("ct_1_", "NODEONLY_T4_{'D1': 1, 'D2': 4}"),
                ("ct_3_", "NODEONLY_T4_{'D1': 3, 'D2': 4}"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesBoth2Clusters(self):
        name = "DependenciesBoth2Clusters"
        config = BpsConfig(
            {
                "templateDataId": "{D1}_{D2}_{D3}_{D4}",
                "cluster": {
                    "cl1": {
                        "pipetasks": "T1, T2",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "sink",
                    },
                    "cl2": {
                        "pipetasks": "T3, T4",
                        "dimensions": "D1, D2",
                        "findDependencyMethod": "source",
                    },
                },
            }
        )
        answer = {
            "name": name,
            "nodes": {
                "cl1_1_2": {"label": "cl1", "dims": {"D1": 1, "D2": 2}, "counts": {"T1": 1, "T2": 1}},
                "cl1_1_4": {"label": "cl1", "dims": {"D1": 1, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "cl1_3_4": {"label": "cl1", "dims": {"D1": 3, "D2": 4}, "counts": {"T1": 1, "T2": 1}},
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "cl2_1_2": {"label": "cl2", "dims": {"D1": 1, "D2": 2}, "counts": {"T3": 1, "T4": 1}},
                "cl2_1_4": {"label": "cl2", "dims": {"D1": 1, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "cl2_3_4": {"label": "cl2", "dims": {"D1": 3, "D2": 4}, "counts": {"T3": 1, "T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_2", "NODENAME_T2b_1_2_"),
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "cl2_1_2"),
                ("cl1_1_4", "cl2_1_4"),
                ("cl1_3_4", "cl2_3_4"),
            ],
        }
        cqg = dimension_clustering(config, self.qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesBadMethod(self):
        name = "DependenciesBadMethod"
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

    def testDependenciesSinkUneven(self):
        name = "DependenciesSinkUneven"
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
                    "counts": {"T3": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T2": 1, "T3": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "NODENAME_T4_1_2_"),
                ("cl1_1_4", "NODENAME_T4_1_4_"),
                ("cl1_3_4", "NODENAME_T4_3_4_"),
            ],
        }
        qgraph = make_test_quantum_graph(uneven=True)
        cqg = dimension_clustering(config, qgraph, name)
        check_cqg(cqg, answer)

    def testDependenciesSourceUneven(self):
        name = "DependenciesSourceUneven"
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
                    "counts": {"T3": 1},
                },
                "cl1_1_4": {
                    "label": "cl1",
                    "dims": {"D1": 1, "D2": 4},
                    "counts": {"T2": 1, "T3": 1},
                },
                "cl1_3_4": {
                    "label": "cl1",
                    "dims": {"D1": 3, "D2": 4},
                    "counts": {"T1": 1, "T2": 1, "T3": 1},
                },
                "NODENAME_T2b_1_2_": {"label": "T2b", "dims": {"D1": 1, "D2": 2}, "counts": {"T2b": 1}},
                "NODENAME_T2b_1_4_": {"label": "T2b", "dims": {"D1": 1, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T2b_3_4_": {"label": "T2b", "dims": {"D1": 3, "D2": 4}, "counts": {"T2b": 1}},
                "NODENAME_T4_1_2_": {"label": "T4", "dims": {"D1": 1, "D2": 2}, "counts": {"T4": 1}},
                "NODENAME_T4_1_4_": {"label": "T4", "dims": {"D1": 1, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T4_3_4_": {"label": "T4", "dims": {"D1": 3, "D2": 4}, "counts": {"T4": 1}},
                "NODENAME_T5_1_2_": {"label": "T5", "dims": {"D1": 1, "D2": 2}, "counts": {"T5": 1}},
                "NODENAME_T5_1_4_": {"label": "T5", "dims": {"D1": 1, "D2": 4}, "counts": {"T5": 1}},
                "NODENAME_T5_3_4_": {"label": "T5", "dims": {"D1": 3, "D2": 4}, "counts": {"T5": 1}},
            },
            "edges": [
                ("cl1_1_4", "NODENAME_T2b_1_4_"),
                ("cl1_3_4", "NODENAME_T2b_3_4_"),
                ("cl1_1_2", "NODENAME_T4_1_2_"),
                ("cl1_1_4", "NODENAME_T4_1_4_"),
                ("cl1_3_4", "NODENAME_T4_3_4_"),
            ],
        }
        qgraph = make_test_quantum_graph(uneven=True)
        cqg = dimension_clustering(config, qgraph, name)
        check_cqg(cqg, answer)


if __name__ == "__main__":
    unittest.main()
