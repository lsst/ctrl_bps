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
import unittest

try:
    import htcondor
    from lsst.ctrl.bps.wms.htcondor import lssthtc
except ImportError:
    htcondor = None


@unittest.skipIf(not htcondor, "Warning: Missing HTCondor API. Skipping")
class TestLsstHtc(unittest.TestCase):

    def testHtcEscapeInt(self):
        self.assertEqual(lssthtc.htc_escape(100), 100)

    def testHtcEscapeDouble(self):
        self.assertEqual(lssthtc.htc_escape('"double"'), '""double""')

    def testHtcEscapeSingle(self):
        self.assertEqual(lssthtc.htc_escape("'single'"), "''single''")

    def testHtcEscapeNoSideEffect(self):
        val = "'val'"
        self.assertEqual(lssthtc.htc_escape(val), "''val''")
        self.assertEqual(val, "'val'")

    def testHtcEscapeQuot(self):
        self.assertEqual(lssthtc.htc_escape("&quot;val&quot;"), '"val"')


if __name__ == "__main__":
    unittest.main()
