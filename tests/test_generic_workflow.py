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
import io
import unittest
from collections import Counter

import networkx
import networkx.algorithms.isomorphism as iso

import lsst.ctrl.bps.generic_workflow as gw


class TestGenericWorkflowJob(unittest.TestCase):
    """Test of generic workflow jobs."""

    def testEquality(self):
        job1 = gw.GenericWorkflowJob("job1")
        job2 = gw.GenericWorkflowJob("job1")
        self.assertEqual(job1, job2)


class TestGenericWorkflow(unittest.TestCase):
    """Test generic workflow."""

    def setUp(self):
        self.exec1 = gw.GenericWorkflowExec(
            name="test1.py", src_uri="${CTRL_BPS_DIR}/bin/test1.py", transfer_executable=False
        )
        self.job1 = gw.GenericWorkflowJob("job1", label="label1")
        self.job1.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        self.job1.executable = self.exec1

        self.job2 = gw.GenericWorkflowJob("job2", label="label2")
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

    def testAddJobRelationshipsSingle(self):
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job_relationships("job1", "job2")
        self.assertListEqual([("job1", "job2")], list(gwf.edges()))

    def testAddJobRelationshipsMultiChild(self):
        job3 = gw.GenericWorkflowJob("job3")
        job3.label = "label2"
        job3.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job3.executable = self.exec1

        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships("job1", ["job2", "job3"])
        self.assertListEqual([("job1", "job2"), ("job1", "job3")], list(gwf.edges()))

    def testAddJobRelationshipsMultiParents(self):
        job3 = gw.GenericWorkflowJob("job3")
        job3.label = "label2"
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

    def testSaveInvalidFormat(self):
        gwf = gw.GenericWorkflow("mytest")
        stream = io.BytesIO()
        with self.assertRaises(RuntimeError):
            gwf.save(stream, "badformat")

    def testSavePickle(self):
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

    def testLabels(self):
        job3 = gw.GenericWorkflowJob("job3")
        job3.label = "label2"
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.assertListEqual(["label1", "label2"], gwf.labels)

    def testRegenerateLabels(self):
        job3 = gw.GenericWorkflowJob("job3", label="label2")
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
        job3 = gw.GenericWorkflowJob("job3", label="label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")
        self.assertEqual(Counter({"label1": 1, "label2": 2}), gwf.job_counts)

    def testDelJob(self):
        job3 = gw.GenericWorkflowJob("job3", label="label2")
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        gwf.del_job("job2")

        self.assertListEqual([("job1", "job3")], list(gwf.edges()))
        self.assertEqual(Counter({"label1": 1, "label2": 1}), gwf.job_counts)

    def testAddWorkflowSource(self):
        job3 = gw.GenericWorkflowJob("job3")
        job3.label = "label2"
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        srcjob1 = gw.GenericWorkflowJob("srcjob1")
        srcjob1.label = "srclabel1"
        srcjob1.executable = self.exec1
        srcjob2 = gw.GenericWorkflowJob("srcjob2")
        srcjob2.label = "srclabel1"
        srcjob2.executable = self.exec1
        srcjob3 = gw.GenericWorkflowJob("srcjob3")
        srcjob3.label = "srclabel2"
        srcjob3.executable = self.exec1
        srcjob4 = gw.GenericWorkflowJob("srcjob4")
        srcjob4.label = "srclabel2"
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
        job3 = gw.GenericWorkflowJob("job3")
        job3.label = "label3"
        gwf = gw.GenericWorkflow("mytest")
        gwf.add_job(self.job1)
        gwf.add_job(self.job2)
        gwf.add_job(job3)
        gwf.add_job_relationships(["job1", "job2"], "job3")

        self.assertListEqual([job3], gwf.get_jobs_by_label("label3"))


if __name__ == "__main__":
    unittest.main()
