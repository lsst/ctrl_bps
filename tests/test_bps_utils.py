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
import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.bps_utils import _make_id_link, chdir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestChdir(unittest.TestCase):
    """Test directory changing."""

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


class TestMakeIdLink(unittest.TestCase):
    """Test _make_id_link function."""

    def setUp(self):
        self.cwd = os.getcwd()
        self.tmpdir = Path(tempfile.mkdtemp(dir=TESTDIR))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testMakeIdLinkFalse(self):
        """Test skipping making link."""
        config = BpsConfig({"makeIdLink": False})
        dir_of_links = self.tmpdir / "bps_links"
        _make_id_link(config, "100.0")
        self.assertFalse(dir_of_links.exists())

    def testNoRunID(self):
        """Test no link made if no run id."""
        config = BpsConfig({"makeIdLink": True})
        dir_of_links = self.tmpdir / "bps_links"
        _make_id_link(config, None)
        self.assertFalse(dir_of_links.exists())

    def testSuccessfulLink(self):
        """Test successfully made id link."""
        config = BpsConfig({"makeIdLink": True})

        submit_path = self.tmpdir / "test_submit/testrun/1"
        submit_path.mkdir(parents=True)
        config["submitPath"] = str(submit_path)

        # Make sure can make multiple dirs
        dir_of_links = self.tmpdir / "test_bps/links"
        config["idLinkPath"] = str(dir_of_links)

        _make_id_link(config, "100.0")
        link_path = dir_of_links / "100.0"
        self.assertTrue(link_path.is_symlink())
        self.assertEqual(link_path.readlink(), submit_path)

    def testSubmitDoesNotExist(self):
        """Test checking that submit directory exists."""
        config = BpsConfig({"makeIdLink": True})

        submit_path = self.tmpdir / "test_submit/testrun/1"
        submit_path.mkdir(parents=True)
        config["submitPath"] = str(submit_path / "notthere")

        dir_of_links = self.tmpdir / "test_bps_links"
        config["idLinkPath"] = str(dir_of_links)

        with self.assertLogs(level=logging.WARNING) as cm:
            _make_id_link(config, "100.0")
            self.assertRegex(
                cm.records[0].getMessage(), "Could not make id softlink: submitPath does not exist"
            )
        self.assertFalse(dir_of_links.exists())

    def testFileExistsError(self):
        """Test catching of FileExistsError."""
        config = BpsConfig({"makeIdLink": True})

        submit_path = self.tmpdir / "test_submit/testrun/1"
        submit_path.mkdir(parents=True)

        dir_of_links = self.tmpdir / "test_bps_links"
        config["idLinkPath"] = str(dir_of_links)

        # Make a directory with the link path so
        # that get FileExistsError
        link_path = dir_of_links / "100.0"
        link_path.mkdir(parents=True)

        with self.assertLogs(level=logging.WARNING) as cm:
            _make_id_link(config, "100.0")
            self.assertRegex(cm.records[0].getMessage(), "Could not make id softlink")
        self.assertFalse(link_path.is_symlink())

    def testPermissionError(self):
        """Test catching of PermissionError."""
        config = BpsConfig({"makeIdLink": True})

        submit_path = self.tmpdir / "test_submit/testrun/1"
        submit_path.mkdir(parents=True)

        dir_of_links = self.tmpdir / "test_bps_links"
        # Create dir without write permissions to cause the error.
        dir_of_links.mkdir(mode=0o555, parents=True)
        config["idLinkPath"] = str(dir_of_links)

        with self.assertLogs(level=logging.WARNING) as cm:
            _make_id_link(config, "100.0")
            self.assertRegex(cm.records[0].getMessage(), "Could not make id softlink")
        link_path = dir_of_links / "100.0"
        self.assertFalse(link_path.is_symlink())


if __name__ == "__main__":
    unittest.main()
