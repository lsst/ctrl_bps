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

from lsst.ctrl.bps.drivers import _init_submission_driver

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestInitSubmissionDriver(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testDeprecatedOutCollection(self):
        with self.assertRaisesRegex(KeyError, "outCollection"):
            _init_submission_driver({"payload": {"outCollection": "bad"}})

    def testMissingOutputRun(self):
        with self.assertRaisesRegex(KeyError, "outputRun"):
            _init_submission_driver({"payload": {"inCollection": "bad"}})

    def testMissingSubmitPath(self):
        with self.assertRaisesRegex(KeyError, "submitPath"):
            _init_submission_driver({"payload": {"outputRun": "bad"}})


if __name__ == "__main__":
    unittest.main()
