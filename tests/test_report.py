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

"""Tests for reporting mechanism."""

import io
import unittest

from astropy.table import Table
from lsst.ctrl.bps import (
    BaseRunReport,
    DetailedRunReport,
    SummaryRunReport,
    WmsJobReport,
    WmsRunReport,
    WmsStates,
)


class FakeRunReport(BaseRunReport):
    """A fake run report"""

    def add(self, run_report, use_global_id=False):
        id_ = run_report.global_wms_id if use_global_id else run_report.wms_id
        self._table.add_row([id_, run_report.state.name])


class FakeRunReportTestCase(unittest.TestCase):
    """Test shared methods."""

    def setUp(self):
        self.fields = [("ID", "S"), ("STATE", "S")]

        self.report = FakeRunReport(self.fields)
        self.report.add(WmsRunReport(wms_id="2.0", state=WmsStates.RUNNING))
        self.report.add(WmsRunReport(wms_id="1.0", state=WmsStates.SUCCEEDED))

    def testEquality(self):
        """Test if two reports are identical."""
        other = FakeRunReport(self.fields)
        other.add(WmsRunReport(wms_id="2.0", state=WmsStates.RUNNING))
        other.add(WmsRunReport(wms_id="1.0", state=WmsStates.SUCCEEDED))
        self.assertEqual(self.report, other)

    def testInequality(self):
        """Test if two reports are not identical."""
        other = FakeRunReport(self.fields)
        other.add(WmsRunReport(wms_id="1.0", state=WmsStates.FAILED))
        self.assertNotEqual(self.report, other)

    def testLength(self):
        self.assertEqual(len(self.report), 2)

    def testClear(self):
        """Test clearing the report."""
        self.report.clear()
        self.assertEqual(len(self.report), 0)

    def testSortWithKnownKey(self):
        """Test sorting the report using known column."""
        expected_output = io.StringIO()
        expected = Table(dtype=self.fields)
        expected.add_row(["1.0", WmsStates.SUCCEEDED.name])
        expected.add_row(["2.0", WmsStates.RUNNING.name])
        print(expected, file=expected_output)

        actual_output = io.StringIO()
        self.report.sort("ID")
        print(self.report, file=actual_output)

        self.assertEqual(actual_output.getvalue(), expected_output.getvalue())

        expected_output.close()
        actual_output.close()

    def testSortWithUnknownKey(self):
        """Test sorting the report using unknown column."""
        with self.assertRaises(AttributeError):
            self.report.sort("foo")


class SummaryRunReportTestCase(unittest.TestCase):
    """Test a summary run report."""

    def setUp(self):
        self.fields = [
            ("X", "S"),
            ("STATE", "S"),
            ("%S", "S"),
            ("ID", "S"),
            ("OPERATOR", "S"),
            ("PROJECT", "S"),
            ("CAMPAIGN", "S"),
            ("PAYLOAD", "S"),
            ("RUN", "S"),
        ]
        self.run = WmsRunReport(
            wms_id="1.0",
            global_wms_id="foo#1.0",
            path="/path/to/run",
            label="label",
            run="run",
            project="dev",
            campaign="testing",
            payload="test",
            operator="tester",
            run_summary="foo:1;bar:1",
            state=WmsStates.RUNNING,
            jobs=None,
            total_number_jobs=2,
            job_state_counts={
                state: 1 if state in {WmsStates.SUCCEEDED, WmsStates.RUNNING} else 0 for state in WmsStates
            },
            job_summary=None,
        )
        self.report = SummaryRunReport(self.fields)

        self.expected = Table(dtype=self.fields)
        self.expected.add_row(["", "RUNNING", "50", "1.0", "tester", "dev", "testing", "test", "run"])

        self.expected_output = io.StringIO()
        self.actual_output = io.StringIO()

    def tearDown(self):
        self.expected_output.close()
        self.actual_output.close()

    def testAddWithNoFlag(self):
        """Test adding a report for a run with no issues."""
        print("\n".join(self.expected.pformat_all()), file=self.expected_output)

        self.report.add(self.run)
        print(self.report, file=self.actual_output)

        self.assertEqual(self.actual_output.getvalue(), self.expected_output.getvalue())

    def testAddWithFailedFlag(self):
        """Test adding a run with a failed job."""
        self.expected["X"][0] = "F"
        print("\n".join(self.expected.pformat_all()), file=self.expected_output)

        # Alter the run report to include a failed job.
        self.run.job_state_counts = {
            state: 1 if state in {WmsStates.FAILED, WmsStates.SUCCEEDED} else 0 for state in WmsStates
        }
        self.report.add(self.run)
        print(self.report, file=self.actual_output)

        self.assertEqual(self.actual_output.getvalue(), self.expected_output.getvalue())

    def testAddWithHeldFlag(self):
        """Test adding a run with a held job."""
        self.expected["X"][0] = "H"
        print("\n".join(self.expected.pformat_all()), file=self.expected_output)

        # Alter the run report to include a held job.
        self.run.job_state_counts = {
            state: 1 if state in {WmsStates.SUCCEEDED, WmsStates.HELD} else 0 for state in WmsStates
        }
        self.report.add(self.run)
        print(self.report, file=self.actual_output)

        self.assertEqual(self.actual_output.getvalue(), self.expected_output.getvalue())

    def testAddWithDeletedFlag(self):
        """Test adding a run with a deleted job."""
        self.expected["X"][0] = "D"
        print("\n".join(self.expected.pformat_all()), file=self.expected_output)

        # Alter the run report to include a deleted job.
        self.run.job_state_counts = {
            state: 1 if state in {WmsStates.SUCCEEDED, WmsStates.DELETED} else 0 for state in WmsStates
        }
        self.report.add(self.run)
        print(self.report, file=self.actual_output)

        self.assertEqual(self.actual_output.getvalue(), self.expected_output.getvalue())


class DetailedRunReportTestCase(unittest.TestCase):
    """Test a detailed run report."""

    def setUp(self):
        self.fields = [("", "S")] + [(state.name, "I") for state in WmsStates] + [("EXPECTED", "i")]

        table = Table(dtype=self.fields)
        table.add_row(
            ["TOTAL"]
            + [1 if state in {WmsStates.RUNNING, WmsStates.SUCCEEDED} else 0 for state in WmsStates]
            + [2]
        )
        table.add_row(["foo"] + [1 if state == WmsStates.SUCCEEDED else 0 for state in WmsStates] + [1])
        table.add_row(["bar"] + [1 if state == WmsStates.RUNNING else 0 for state in WmsStates] + [1])
        self.expected = DetailedRunReport.from_table(table)

        self.run = WmsRunReport(
            wms_id="1.0",
            global_wms_id="foo#1.0",
            path="/path/to/run",
            label="label",
            run="run",
            project="dev",
            campaign="testing",
            payload="test",
            operator="tester",
            run_summary="foo:1;bar:1",
            state=WmsStates.RUNNING,
            jobs=[
                WmsJobReport(wms_id="1.0", name="", label="foo", state=WmsStates.SUCCEEDED),
                WmsJobReport(wms_id="2.0", name="", label="bar", state=WmsStates.RUNNING),
            ],
            total_number_jobs=2,
            job_state_counts={
                state: 1 if state in {WmsStates.SUCCEEDED, WmsStates.RUNNING} else 0 for state in WmsStates
            },
            job_summary={
                "foo": {state: 1 if state == WmsStates.SUCCEEDED else 0 for state in WmsStates},
                "bar": {state: 1 if state == WmsStates.RUNNING else 0 for state in WmsStates},
            },
        )

        self.actual = DetailedRunReport(self.fields)

    def testAddWithJobSummary(self):
        """Test adding a run with a job summary."""
        self.run.jobs = None
        self.actual.add(self.run)

        self.assertEqual(self.actual, self.expected)

    def testAddWithJobs(self):
        """Test adding a run with a job info, but not job summary."""
        self.run.job_summary = None
        self.actual.add(self.run)

        self.assertEqual(self.actual, self.expected)

    def testAddWithoutJobInfo(self):
        """Test adding a run without either a job summary or job info."""
        self.run.jobs = None
        self.run.job_summary = None
        self.actual.add(self.run)

        self.assertEqual(len(self.actual), 1)
        self.assertRegex(self.actual.message, r"^WARNING.*incomplete")

    def testAddWithoutRunSummary(self):
        """Test adding a run without a run summary."""
        table = Table(dtype=self.fields)
        table.add_row(
            ["TOTAL"]
            + [1 if state in {WmsStates.RUNNING, WmsStates.SUCCEEDED} else 0 for state in WmsStates]
            + [2]
        )
        table.add_row(["bar"] + [1 if state == WmsStates.RUNNING else 0 for state in WmsStates] + [-1])
        table.add_row(["foo"] + [1 if state == WmsStates.SUCCEEDED else 0 for state in WmsStates] + [-1])
        expected = DetailedRunReport.from_table(table)

        self.run.run_summary = None
        self.actual.add(self.run)

        self.assertRegex(self.actual.message, r"^WARNING.*sorted alphabetically")
        self.assertEqual(self.actual, expected)


if __name__ == "__main__":
    unittest.main()
