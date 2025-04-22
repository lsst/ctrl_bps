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
import dataclasses
import io
import logging
import unittest
from collections import Counter

import networkx

import lsst.ctrl.bps.generic_workflow as gw
import lsst.ctrl.bps.tests.gw_test_utils as gtu


class TestGenericWorkflowNode(unittest.TestCase):
    """Test of generic workflow node base class."""

    def testNoNodeType(self):
        @dataclasses.dataclass(slots=True)
        class GenericWorkflowNoNodeType(gw.GenericWorkflowNode):
            dummy_val: int

        job = GenericWorkflowNoNodeType("myname", "mylabel", 3)
        with self.assertRaises(NotImplementedError):
            _ = job.node_type

    def testHash(self):
        job = gw.GenericWorkflowNode("myname", "mylabel")
        job2 = gw.GenericWorkflowNode("myname2", "mylabel")
        job3 = gw.GenericWorkflowNode("myname", "mylabel2")
        self.assertNotEqual(hash(job), hash(job2))
        self.assertEqual(hash(job), hash(job3))


class TestGenericWorkflowJob(unittest.TestCase):
    """Test of generic workflow jobs."""

    def testEquality(self):
        job1 = gw.GenericWorkflowJob("job1", "label1")
        job2 = gw.GenericWorkflowJob("job1", "label1")
        self.assertEqual(job1, job2)


class TestGenericWorkflow(unittest.TestCase):
    """Test generic workflow."""

    def setUp(self):
        self.exec1 = gw.GenericWorkflowExec(
            name="test1.py", src_uri="${CTRL_BPS_DIR}/bin/test1.py", transfer_executable=False
        )
        self.job1 = gw.GenericWorkflowJob("job1", "label1")
        self.job1.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        self.job1.executable = self.exec1

        self.job2 = gw.GenericWorkflowJob("job2", "label2")
        self.job2.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        self.job2.executable = self.exec1

    def testAddJobDuplicate(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        with self.assertRaises(RuntimeError):
            gwf.add_job(self.job1)

    def testAddJobValid(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        self.assertEqual(1, gwf.number_of_nodes())
        self.assertListEqual(["job1"], list(gwf))
        getjob = gwf.get_job("job1")
        self.assertEqual(self.job1, getjob)

    def testAddNode(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_node(self.job1)
        self.assertEqual(1, gwf.number_of_nodes())
        self.assertListEqual(["job1"], list(gwf))
        self.assertEqual(self.job1, gwf.get_job("job1"))

    def testAddJobRelationshipsSingle(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job_relationships("job1", "job2")
        self.assertListEqual([("job1", "job2")], list(gwf.edges()))

    def testAddJobRelationshipsMultiChild(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        job3.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job3.executable = self.exec1

        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships("job1", ["job2", "job3"])
        self.assertListEqual([("job1", "job2"), ("job1", "job3")], list(gwf.edges()))

    def testAddJobRelationshipsMultiParents(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        job3.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job3.executable = self.exec1
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.assertListEqual([("job1", "job3"), ("job2", "job3")], list(gwf.edges()))

    def testAddJobRelationshipsNone(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job_relationships(None, "job1")
        self.assertListEqual([], list(gwf.edges()))
        gwf.add_job_relationships("job1", None)
        self.assertListEqual([], list(gwf.edges()))

    def testGetJobExists(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        get_job = gwf.get_job("job1")
        self.assertIs(self.job1, get_job)

    def testGetJobError(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        with self.assertRaises(KeyError):
            _ = gwf.get_job("job_not_there")

    def testAddEdgeBadParent(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        with self.assertRaisesRegex(RuntimeError, "notthere not in GenericWorkflow"):
            gwf.add_edge("notthere", "job2")

    def testAddEdgeBadChild(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        with self.assertRaisesRegex(RuntimeError, "notthere2 not in GenericWorkflow"):
            gwf.add_edge("job1", "notthere2")

    def testQuantaCounts(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        truth = Counter({"label1": 6, "label2": 6, "label3": 6})
        self.assertEqual(gwf.quanta_counts, truth)

    def testGetExecutablesNames(self):
        gwf = gw.GenericWorkflow("mytest")
        self.job1.executable = gw.GenericWorkflowExec("exec1")
        gwf.add_job(self.job1)
        self.assertEqual(gwf._executables["exec1"], self.job1.executable)
        results = gwf.get_executables(data=False, transfer_only=False)
        self.assertEqual(results, ["exec1"])

    def testGetExecutablesData(self):
        gwf = gw.GenericWorkflow("mytest")
        self.job1.executable = gw.GenericWorkflowExec("exec1")
        gwf.add_job(self.job1)
        results = gwf.get_executables(data=True, transfer_only=False)
        self.assertEqual(results, [self.job1.executable])
        self.assertEqual(hash(self.job1.executable), hash(results[0]))

    def testAddFileTwice(self):
        gwf = gw.GenericWorkflow("mytest")
        gwfile = gw.GenericWorkflowFile("file1")
        gwf.add_file(gwfile)
        with self.assertLogs("lsst.ctrl.bps.generic_workflow", level=logging.DEBUG) as cm:
            gwf.add_file(gwfile)
            self.assertRegex(cm.records[-1].getMessage(), "Skipped add_file for existing file file1")

    def testGetFilesNames(self):
        gwf = gw.GenericWorkflow("mytest")
        gwfile1 = gw.GenericWorkflowFile("file1", wms_transfer=True)
        gwf.add_file(gwfile1)
        gwfile2 = gw.GenericWorkflowFile("file2", wms_transfer=False)
        gwf.add_file(gwfile2)
        results = gwf.get_files(data=False, transfer_only=False)
        self.assertEqual(results, ["file1", "file2"])

    def testGetFilesData(self):
        gwf = gw.GenericWorkflow("mytest")
        gwfile1 = gw.GenericWorkflowFile("file1", wms_transfer=True)
        gwf.add_file(gwfile1)
        gwfile2 = gw.GenericWorkflowFile("file2", wms_transfer=False)
        gwf.add_file(gwfile2)
        results = gwf.get_files(data=True, transfer_only=True)
        self.assertEqual(results, [gwfile1])
        self.assertEqual(hash(gwfile1), hash(results[0]))

    def testJobInputs(self):
        # test add + get
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwfile1 = gw.GenericWorkflowFile("file1", wms_transfer=True)
        gwf.add_job_inputs("job1", gwfile1)
        gwfile2 = gw.GenericWorkflowFile("file2", wms_transfer=False)
        gwf.add_job_inputs("job2", gwfile2)
        self.assertIn("file1", gwf.get_files())
        self.assertEqual(["file1"], gwf.get_job_inputs("job1", data=False))
        self.assertEqual([gwfile1], gwf.get_job_inputs("job1"))
        self.assertEqual([], gwf.get_job_inputs("job2", data=False, transfer_only=True))

    def testSaveInvalidFormat(self):
        gwf = gw.GenericWorkflow("mytest")
        stream = io.BytesIO()
        with self.assertRaisesRegex(RuntimeError, r"Unknown format \(bad_format\)"):
            gwf.save(stream, "bad_format")

    def testLoadInvalidFormat(self):
        stream = io.BytesIO()
        with self.assertRaisesRegex(RuntimeError, r"Unknown format \(bad_format\)"):
            _ = gw.GenericWorkflow.load(stream, "bad_format")

    def testValidate(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        # No exception should be raised
        gwf.validate()

    def testValidateGroups(self):
        gwf = gtu.make_3_label_workflow_groups_sort("test_validate", final=True)
        gwf.validate()

    def testSavePickle(self):
        # test save and load
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job_relationships("job1", "job2")
        stream = io.BytesIO()
        gwf.save(stream, "pickle")
        stream.seek(0)
        gwf2 = gw.GenericWorkflow.load(stream, "pickle")
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, gwf2),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testDrawBadFormat(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        stream = io.BytesIO()
        with self.assertRaisesRegex(RuntimeError, r"Unknown draw format \(bad_format\)"):
            gwf.draw(stream, "bad_format")

    def testLabels(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.assertListEqual(["label1", "label2"], gwf.labels)

    def testRegenerateLabels(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.job1.label = "label1b"
        self.job2.label = "label1b"
        job3.label = "label2b"
        gwf.regenerate_labels()
        self.assertListEqual(["label1b", "label2b"], gwf.labels)

    def testJobCounts(self):
        gwf = gtu.make_3_label_workflow("test1", final=False)
        truth = Counter({"pipetaskInit": 1, "label1": 6, "label2": 6, "label3": 6})
        self.assertEqual(gwf.job_counts, truth)

    def testJobCountsFinal(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        truth = Counter({"pipetaskInit": 1, "label1": 6, "label2": 6, "label3": 6, "finalJob": 1})
        self.assertEqual(gwf.job_counts, truth)

    def testDelJob(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        gwf.del_job("job2")

        self.assertListEqual([("job1", "job3")], list(gwf.edges()))
        self.assertEqual(Counter({"label1": 1, "label2": 1}), gwf.job_counts)

    def testAddWorkflowSource(self):
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        srcjob1 = gw.GenericWorkflowJob("srcjob1", "srclabel1")
        srcjob1.executable = self.exec1
        srcjob2 = gw.GenericWorkflowJob("srcjob2", "srclabel1")
        srcjob2.executable = self.exec1
        srcjob3 = gw.GenericWorkflowJob("srcjob3", "srclabel2")
        srcjob3.executable = self.exec1
        srcjob4 = gw.GenericWorkflowJob("srcjob4", "srclabel2")
        srcjob4.executable = self.exec1
        gwf2 = gw.GenericWorkflow("mytest2")
        gwf2.add_job(srcjob1)
        gwf2.add_job(srcjob2)
        gwf2.add_job(srcjob3)
        gwf2.add_job(srcjob4)
        gwf2.add_job_relationships("srcjob1", "srcjob3")
        gwf2.add_job_relationships("srcjob2", "srcjob4")

        gwf.add_workflow_source(gwf2)

        self.assertEqual(Counter({"srclabel1": 2, "srclabel2": 2, "label1": 1, "label2": 2}), gwf.job_counts)
        self.assertListEqual(["srclabel1", "srclabel2", "label1", "label2"], gwf.labels)
        self.assertListEqual(
            sorted(
                [
                    ("srcjob1", "srcjob3"),
                    ("srcjob2", "srcjob4"),
                    ("srcjob3", "job1"),
                    ("srcjob3", "job2"),
                    ("srcjob4", "job1"),
                    ("srcjob4", "job2"),
                    ("job1", "job3"),
                    ("job2", "job3"),
                ]
            ),
            sorted(gwf.edges()),
        )

    def testGetJobsByLabel(self):
        job3 = gw.GenericWorkflowJob("job3", "label3")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        self.assertListEqual([job3], gwf.get_jobs_by_label("label3"))

    def testAddJobInvalidType(self):
        @dataclasses.dataclass(slots=True)
        class GenericWorkflowNodeNoInherit:
            name: str
            label: str

            def __hash__(self):
                return hash(self.name)

            @property
            def node_type(self):
                return gw.GenericWorkflowNodeType.NOOP

        job3 = GenericWorkflowNodeNoInherit("myname", "mylabel")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        with self.assertRaisesRegex(RuntimeError, "Invalid type for job to be added to GenericWorkflowGraph"):
            gwf.add_job(job3)

    def testGroupJobsByDependencies(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        group_config = {"labels": "label2, label3", "dimensions": "visit", "findDependencyMethod": "sink"}
        group_to_label_subgraph = gwf._check_job_ordering_config({"order1": group_config})
        job_groups = gwf._group_jobs_by_dependencies(
            "order1", group_config, group_to_label_subgraph["order1"]
        )
        self.assertEqual(sorted(job_groups.keys()), sorted([(10001,), (10002,), (301,)]))

    def testGroupJobsByDependenciesSource(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        group_config = {"labels": "label2, label3", "dimensions": "visit", "findDependencyMethod": "source"}
        group_to_label_subgraph = gwf._check_job_ordering_config({"order1": group_config})
        job_groups = gwf._group_jobs_by_dependencies(
            "order1", group_config, group_to_label_subgraph["order1"]
        )
        self.assertEqual(sorted(job_groups.keys()), sorted([(10001,), (10002,), (301,)]))

    def testGroupJobsByDependenciesBadMethod(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)

        group_config = {
            "labels": "label2, label3",
            "dimensions": "visit",
            "findDependencyMethod": "bad_method",
        }
        group_to_label_subgraph = gwf._check_job_ordering_config({"order1": group_config})

        with self.assertRaisesRegex(RuntimeError, r"Invalid findDependencyMethod \(bad_method\)"):
            _ = gwf._group_jobs_by_dependencies("order1", group_config, group_to_label_subgraph["order1"])

    def testCheckJobOrderingConfigBadImplementation(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        with self.assertRaisesRegex(RuntimeError, "Invalid implementation for"):
            gwf._check_job_ordering_config(
                {"order1": {"implementation": "bad", "labels": "label2", "dimensions": "visit"}}
            )

    def testCheckJobOrderingConfigBadType(self):
        gwf = gtu.make_3_label_workflow("test1", final=True)
        with self.assertRaisesRegex(RuntimeError, "Invalid ordering_type for"):
            gwf._check_job_ordering_config(
                {"order1": {"ordering_type": "badtype", "labels": "label2", "dimensions": "visit"}}
            )

    def testCheckJobOrderingConfigBadLabel(self):
        gwf = gtu.make_3_label_workflow("test_bad_label", final=True)
        with self.assertRaisesRegex(
            RuntimeError, "Job label label2 appears in more than one job ordering group"
        ):
            gwf._check_job_ordering_config(
                {
                    "order1": {"ordering_type": "sort", "labels": "label2", "dimensions": "visit"},
                    "order2": {"ordering_type": "sort", "labels": "label2,label3", "dimensions": "visit"},
                }
            )

    def testCheckJobOrderingConfigUnusedLabel(self):
        gwf = gtu.make_3_label_workflow("test_unused_label", final=True)
        with self.assertRaisesRegex(
            RuntimeError,
            r"Job label\(s\) \(unused1,unused2\) from job ordering group "
            "order1 does not exist in workflow.  Aborting.",
        ):
            gwf._check_job_ordering_config(
                {
                    "order1": {
                        "ordering_type": "sort",
                        "labels": "label2,unused1,label3,unused2",
                        "dimensions": "visit",
                    },
                }
            )

    def testCheckJobOrderingConfigMissingDim(self):
        gwf = gtu.make_3_label_workflow("test_missing_dim", final=True)
        with self.assertRaisesRegex(KeyError, "Missing dimensions entry in ordering group order1"):
            gwf._check_job_ordering_config({"order1": {"ordering_type": "sort", "labels": "label2"}})

    def testCheckJobOrderingConfigSort(self):
        gwf = gtu.make_3_label_workflow("test_bad_dim", final=True)
        results = gwf._check_job_ordering_config(
            {"order1": {"ordering_type": "sort", "labels": "label1,label2", "dimensions": "visit"}}
        )
        self.assertEqual(len(results), 1)

        graph = networkx.DiGraph()
        graph.add_nodes_from(["label1", "label2"])
        graph.add_edges_from([("label1", "label2")])
        self.assertTrue(networkx.is_isomorphic(results["order1"], graph, node_match=lambda x, y: x == y))

    def testAddSpecialJobOrderingNoopSort(self):
        # Also making sure noop jobs don't alter quanta_counts
        gwf = gtu.make_3_label_workflow("test", final=True)
        quanta_counts_before = gwf.quanta_counts

        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "noop",
                    "ordering_type": "sort",
                    "labels": "label1,label2",
                    "dimensions": "visit",
                }
            }
        )

        quanta_counts_after = gwf.quanta_counts
        self.assertEqual(quanta_counts_after, quanta_counts_before)

        gwf.regenerate_labels()
        quanta_counts_after = gwf.quanta_counts
        self.assertEqual(quanta_counts_after, quanta_counts_before)

        gwf_noop = gtu.make_3_label_workflow_noop_sort("truth", final=True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, gwf_noop),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testAddSpecialJobOrderingGroupSort(self):
        gwf = gtu.make_3_label_workflow("test", final=True)
        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "labels": "label1,label2",
                    "dimensions": "visit",
                }
            }
        )

        truth = gtu.make_3_label_workflow_groups_sort("truth", final=True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testAddSpecialJobOrderingGroupSortSink(self):
        gwf = gtu.make_3_label_workflow("test_group_sort", final=True)
        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "sink",
                    "labels": "label1,label2",
                    "dimensions": "visit",
                }
            }
        )

        truth = gtu.make_3_label_workflow_groups_sort("truth", final=True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupValues(self):
        gwf = gtu.make_5_label_workflow("mid_group_even_values", True, False, True)
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, False, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupSink(self):
        gwf = gtu.make_5_label_workflow("mid_group_even_sink", True, False, True)
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "sink",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, False, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupSource(self):
        gwf = gtu.make_5_label_workflow("mid_group_even_source", True, False, True)
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "source",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, False, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupValuesUneven(self):
        gwf = gtu.make_5_label_workflow("mid_group_uneven_values", True, True, True)
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, True, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupSinkUneven(self):
        gwf = gtu.make_5_label_workflow("mid_group_uneven_sink", True, True, True)

        # Note:  also checking changing blocking value from default
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "sink",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                    "blocking": True,
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, True, True, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def testMiddleGroupSourceUneven(self):
        gwf = gtu.make_5_label_workflow("mid_group_uneven_source", True, True, True)
        gwf.add_special_job_ordering(
            {
                "mid": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "source",
                    "labels": "T2, T2b, T3",
                    "dimensions": "visit",
                }
            }
        )
        truth = gtu.make_5_label_workflow_middle_groups("truth", True, True, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def test2GroupsEven(self):
        gwf = gtu.make_5_label_workflow("two_groups_even", True, False, True)
        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "sink",
                    "labels": "T1, T2",
                    "dimensions": "visit",
                    "equalDimensions": "visit:group",
                    "blocking": True,
                },
                "order2": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "source",
                    "labels": "T3, T4",
                    "dimensions": "visit",
                    "blocking": True,
                },
            }
        )

        truth = gtu.make_5_label_workflow_2_groups("truth", True, False, True, True)
        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )

    def test2GroupsUneven(self):
        gwf = gtu.make_5_label_workflow("two_groups_uneven", True, True, True)
        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "sink",
                    "labels": "T1, T2",
                    "dimensions": "visit",
                    "equalDimensions": "visit:group",
                },
                "order2": {
                    "implementation": "group",
                    "ordering_type": "sort",
                    "findDependencyMethod": "source",
                    "labels": "T3, T4",
                    "dimensions": "visit",
                },
            }
        )

        truth = gtu.make_5_label_workflow_2_groups("truth", True, True, True)

        self.assertTrue(
            gtu.compare_generic_workflows(gwf, truth),
            "Results do not match expected GenericWorkflow. See debug messages.",
        )


class TestGenericWorkflowLabels(unittest.TestCase):
    """Tests for GenericWorkflowLabels"""

    def testEmptyLabels(self):
        gwlabels = gw.GenericWorkflowLabels()
        self.assertFalse(len(gwlabels.labels))

    def testEmptyJobCounts(self):
        gwlabels = gw.GenericWorkflowLabels()
        self.assertFalse(len(gwlabels.job_counts))

    def testEmptyGetJobsByLabel(self):
        gwlabels = gw.GenericWorkflowLabels()
        self.assertFalse(len(gwlabels.get_jobs_by_label("label1")))

    def testAddJobFirst(self):
        gwlabels = gw.GenericWorkflowLabels()
        job = gw.GenericWorkflowJob("job1", "label1")
        gwlabels.add_job(job, [], [])
        self.assertEqual(gwlabels._label_to_jobs["label1"], [job])
        self.assertIn("label1", gwlabels._label_graph)

    def testAddJobMult(self):
        gwlabels = gw.GenericWorkflowLabels()
        job1 = gw.GenericWorkflowJob("job1", "label1")
        job2 = gw.GenericWorkflowJob("job2", "label2")
        job3 = gw.GenericWorkflowJob("job3", "label2")
        job4 = gw.GenericWorkflowJob("job4", "label3")
        gwlabels.add_job(job1, [], [])
        gwlabels.add_job(job2, ["label1"], [])
        gwlabels.add_job(job3, ["label1"], [])
        gwlabels.add_job(job4, ["label2"], [])
        self.assertListEqual(gwlabels._label_to_jobs["label1"], [job1])
        self.assertListEqual(gwlabels._label_to_jobs["label2"], [job2, job3])
        self.assertListEqual(gwlabels._label_to_jobs["label3"], [job4])
        self.assertIn("label1", gwlabels._label_graph)
        self.assertIn("label2", gwlabels._label_graph)
        self.assertIn("label3", gwlabels._label_graph)
        self.assertEqual(list(gwlabels._label_graph.successors("label1")), ["label2"])
        self.assertEqual(list(gwlabels._label_graph.successors("label2")), ["label3"])

    def testDelJobRemain(self):
        # Test that can delete one of multiple jobs with same label
        gwlabels = gw.GenericWorkflowLabels()
        job1 = gw.GenericWorkflowJob("job1", "label1")
        gwlabels.add_job(job1, [], [])
        job2 = gw.GenericWorkflowJob("job2", "label2")
        gwlabels.add_job(job2, ["label1"], [])
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwlabels.add_job(job3, ["label1"], [])
        job4 = gw.GenericWorkflowJob("job4", "label3")
        gwlabels.add_job(job4, ["label2"], [])

        gwlabels.del_job(job2)
        self.assertListEqual(gwlabels._label_to_jobs["label2"], [job3])
        self.assertIn("label2", gwlabels._label_graph)
        self.assertEqual(list(gwlabels._label_graph.successors("label1")), ["label2"])
        self.assertEqual(list(gwlabels._label_graph.successors("label2")), ["label3"])

    def testDelJobLast(self):
        # Test when removing only job with a label
        gwlabels = gw.GenericWorkflowLabels()
        job1 = gw.GenericWorkflowJob("job1", "label1")
        gwlabels.add_job(job1, [], [])
        job2 = gw.GenericWorkflowJob("job2", "label2")
        gwlabels.add_job(job2, [], [])
        job3 = gw.GenericWorkflowJob("job3", "label3")
        gwlabels.add_job(job3, ["label1", "label2"], [])
        job4 = gw.GenericWorkflowJob("job4", "label4")
        gwlabels.add_job(job4, ["label3"], [])
        job5 = gw.GenericWorkflowJob("job5", "label5")
        gwlabels.add_job(job5, ["label3"], [])

        gwlabels.del_job(job3)
        self.assertNotIn("label3", gwlabels._label_to_jobs)
        self.assertNotIn("label3", gwlabels._label_graph)
        self.assertEqual(list(gwlabels._label_graph.successors("label1")), ["label4", "label5"])
        self.assertEqual(list(gwlabels._label_graph.successors("label2")), ["label4", "label5"])

    def testAddSpecialJobOrderingBadType(self):
        gwf = gtu.make_3_label_workflow("test_sort", final=True)
        with self.assertRaisesRegex(RuntimeError, "Invalid ordering_type for"):
            gwf.add_special_job_ordering(
                {"order1": {"ordering_type": "badtype", "labels": "label2", "dimensions": "visit"}}
            )

    def testAddSpecialJobOrderingBadLabel(self):
        gwf = gtu.make_3_label_workflow("test_bad_label", final=True)

        with self.assertRaisesRegex(
            RuntimeError, "Job label label2 appears in more than one job ordering group"
        ):
            gwf.add_special_job_ordering(
                {
                    "order1": {"ordering_type": "sort", "labels": "label2", "dimensions": "visit"},
                    "order2": {"ordering_type": "sort", "labels": "label2,label3", "dimensions": "visit"},
                }
            )

    def testAddSpecialJobOrderingBadDim(self):
        gwf = gtu.make_3_label_workflow("test_bad_dim", final=True)

        with self.assertRaisesRegex(
            KeyError, r"Job label2_10001_10 missing dimensions \(notthere\) required for order group order1"
        ):
            gwf.add_special_job_ordering(
                {"order1": {"ordering_type": "sort", "labels": "label2", "dimensions": "notthere"}}
            )

    def testAddSpecialJobOrderingSort(self):
        gwf = gtu.make_3_label_workflow("test_sort", final=True)
        quanta_counts_before = gwf.quanta_counts

        gwf.add_special_job_ordering(
            {
                "order1": {
                    "implementation": "noop",
                    "ordering_type": "sort",
                    "labels": "label2",
                    "dimensions": "visit",
                }
            }
        )

        quanta_counts_after = gwf.quanta_counts
        self.assertEqual(quanta_counts_after, quanta_counts_before)

        gwf.regenerate_labels()
        quanta_counts_after = gwf.quanta_counts
        self.assertEqual(quanta_counts_after, quanta_counts_before)

        gwf_edges = gwf.edges
        for edge in [
            ("label2_301_10", "noop_order1_301"),
            ("label2_301_11", "noop_order1_301"),
            ("noop_order1_301", "label2_10001_10"),
            ("noop_order1_301", "label2_10001_11"),
            ("label2_10001_10", "noop_order1_10001"),
            ("label2_10001_11", "noop_order1_10001"),
            ("noop_order1_10001", "label2_10002_10"),
            ("noop_order1_10001", "label2_10002_11"),
        ]:
            self.assertIn(edge, gwf_edges, f"Missing edge from {edge[0]} to {edge[1]}")


if __name__ == "__main__":
    unittest.main()
