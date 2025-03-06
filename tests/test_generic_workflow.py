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
import networkx.algorithms.isomorphism as iso

import lsst.ctrl.bps.generic_workflow as gw


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
            networkx.is_isomorphic(gwf, gwf2, node_match=iso.categorical_node_match("data", None))
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
        job3 = gw.GenericWorkflowJob("job3", "label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.assertEqual(Counter({"label1": 1, "label2": 2}), gwf.job_counts)

    def testJobCountsFinal(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        job3 = gw.GenericWorkflowJob("finalJob", "finalJob")
        gwf.add_final(job3)
        self.assertEqual(Counter({"label1": 1, "label2": 1, "finalJob": 1}), gwf.job_counts)

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
        gwf = _make_3_label_workflow("test_sort")
        with self.assertRaisesRegex(RuntimeError, "Invalid ordering_type for"):
            gwf.add_special_job_ordering(
                {"order1": {"ordering_type": "badtype", "labels": "label2", "dimensions": "visit"}}
            )

        with self.assertRaisesRegex(RuntimeError, "Invalid ordering_type for"):
            gwf.add_special_job_ordering(
                {"order1": {"ordering_type": "badtype", "labels": "label2", "dimensions": "visit"}}
            )

    def testAddSpecialJobOrderingBadLabel(self):
        gwf = _make_3_label_workflow("test_bad_label")

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
        gwf = _make_3_label_workflow("test_bad_dim")

        with self.assertRaisesRegex(KeyError, "notthere") as cm:
            gwf.add_special_job_ordering(
                {"order1": {"ordering_type": "sort", "labels": "label2", "dimensions": "notthere"}}
            )
        self.assertIn(
            "has label label2 but missing notthere for order group order1", cm.exception.__notes__[0]
        )

    def testAddSpecialJobOrderingSort(self):
        gwf = _make_3_label_workflow("test_sort")
        quanta_counts_before = gwf.quanta_counts

        gwf.add_special_job_ordering(
            {"order1": {"ordering_type": "sort", "labels": "label2", "dimensions": "visit"}}
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


def _make_3_label_workflow(workflow_name):
    gwf = gw.GenericWorkflow("mytest")
    job = gw.GenericWorkflowJob("pipetaskInit", label="pipetaskInit")
    gwf.add_job(job)
    for visit in [10001, 10002, 301]:  # 301 is to ensure numeric sorting
        for detector in [10, 11]:
            prev_name = "pipetaskInit"
            for label in ["label1", "label2", "label3"]:
                name = f"{label}_{visit}_{detector}"
                job = gw.GenericWorkflowJob(name, label=label, tags={"visit": visit, "detector": detector})
                gwf.add_job(job, [prev_name], None)
                prev_name = name
    return gwf


if __name__ == "__main__":
    unittest.main()
