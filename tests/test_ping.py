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
"""Unit tests for ping.py."""

import logging
import unittest

from lsst.ctrl.bps.ping import ping


class TestPing(unittest.TestCase):
    """Tests for ping."""

    def setUp(self):
        self.logger = logging.getLogger("test_ping")
        self.logger.setLevel(logging.INFO)

    def tearDown(self):
        self.logger.setLevel(logging.NOTSET)

    def testSuccess(self):
        status, msg = ping("wms_test_utils.WmsServiceSuccess", None)
        self.assertEqual(status, 0)
        self.assertEqual(msg, "")

    def testFailed(self):
        with self.assertLogs(level=logging.WARNING) as cm:
            status, msg = ping("wms_test_utils.WmsServiceFailure", None)
            self.assertNotEqual(status, 0)
            self.assertEqual(msg, "Couldn't contact service X")
            self.assertRegex(cm.records[0].getMessage(), "service failure")

    def testPassThru(self):
        with self.assertLogs(level=logging.INFO) as cm:
            pass_thru = "EXTRA_VALUES"
            status, msg = ping("wms_test_utils.WmsServicePassThru", pass_thru)
            self.assertEqual(status, 0)
            self.assertEqual(msg, pass_thru)
            self.assertRegex(cm.records[0].getMessage(), pass_thru)


if __name__ == "__main__":
    unittest.main()
