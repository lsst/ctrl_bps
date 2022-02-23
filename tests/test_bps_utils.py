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
import os
import shutil
import tempfile
import unittest

from lsst.ctrl.bps.bps_utils import chdir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestChdir(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testSuccessfulChdir(self):
        self.assertNotEqual(os.getcwd(), self.tmpdir)
        with chdir(self.tmpdir):
            self.assertEqual(os.getcwd(), self.tmpdir)
        self.assertNotEqual(os.getcwd(), self.tmpdir)

    def testFailingChdir(self):
        dir_not_there = os.path.join(self.tmpdir, "notthere")
        with self.assertRaises(FileNotFoundError):
            with chdir(dir_not_there):
                pass  # should not get here


if __name__ == "__main__":
    unittest.main()
