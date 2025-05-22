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
"""Unit tests for status.py."""

import unittest

from lsst.ctrl.bps import WmsStates
from lsst.ctrl.bps.status import status


class TestStatus(unittest.TestCase):
    """Tests for status."""

    def testSuccess(self):
        retval, message = status("wms_test_utils.WmsServiceSuccess", run_id="/dummy/path", hist=3)
        self.assertEqual(retval, WmsStates.SUCCEEDED)
        self.assertEqual(message, "")

    def testFailed(self):
        retval, message = status("wms_test_utils.WmsServiceFailure", run_id="/dummy/path", hist=3)
        self.assertEqual(retval, WmsStates.FAILED)
        self.assertEqual(message, "Dummy error message.")


if __name__ == "__main__":
    unittest.main()
