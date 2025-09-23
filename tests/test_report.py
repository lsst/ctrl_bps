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

"""Tests for reporting mechanism."""

import dataclasses
import unittest

from wms_test_utils import TEST_REPORT

from lsst.ctrl.bps import compile_job_summary
from lsst.ctrl.bps.report import retrieve_report


class RetrieveReportTestCase(unittest.TestCase):
    """Test report retrieval."""

    def setUp(self):
        self.report = dataclasses.replace(TEST_REPORT)

    def tearDown(self):
        pass

    def testRetrievalPostprocessingSuccessful(self):
        """Test retrieving a report successfully."""
        reports, messages = retrieve_report(
            "wms_test_utils.WmsServiceSuccess", run_id="1.0", postprocessors=(compile_job_summary,)
        )
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0], self.report)
        self.assertFalse(messages)

    def testRetrievalPostprocessingFailed(self):
        """Test failing to retrieve a report."""
        report, messages = retrieve_report(
            "wms_test_utils.WmsServiceFailure", postprocessors=(compile_job_summary,)
        )
        self.assertEqual(len(messages), 1)
        self.assertRegex(messages[0], "issue.*postprocessing")

    def testRetrievalInvalidClass(self):
        """Test retrieving a report with an invalid class."""
        with self.assertRaises(TypeError):
            retrieve_report("wms_test_utils.WmsServiceInvalid", run_id="1.0", postprocessors=None)


if __name__ == "__main__":
    unittest.main()
