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
import unittest

import click

from lsst.ctrl.bps import BpsSubprocessError
from lsst.ctrl.bps.cli import bps
from lsst.ctrl.bps.cli.cmd.commands import catch_errors
from lsst.daf.butler.cli.utils import LogCliRunner


class TestCatchErrors(unittest.TestCase):
    """Test context manager for catching BPS command errors."""

    def testSuccess(self):
        """Test a command that executes successfully."""

    def testSubprocessFailure(self):
        """Test if the error is handled when command's child process failed."""

        def driver():
            raise BpsSubprocessError(-9, "Subprocess failed")

        with self.assertRaises(click.exceptions.Exit) as cm:
            ctx = click.Context(click.Command("command"))
            with ctx:
                with catch_errors():
                    driver()
        self.assertEqual(cm.exception.exit_code, -9)

    def testFailure(self):
        """Test if the error is handled when a command failed."""

        def driver():
            raise KeyError("Key not found.")

        with self.assertRaisesRegex(KeyError, "Key not found"):
            with catch_errors():
                driver()


class TestCommandPing(unittest.TestCase):
    """Test executing the ping subcommand."""

    def setUp(self):
        self.runner = LogCliRunner()

    def testPingNoArgs(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.ping_driver") as mock_driver:
            mock_driver.return_value = 0
            result = self.runner.invoke(bps.cli, ["ping"])
            self.assertEqual(result.exit_code, 0)
            mock_driver.assert_called_with(wms_service=None, pass_thru="")

    def testPingClass(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.ping_driver") as mock_driver:
            mock_driver.return_value = 0
            result = self.runner.invoke(
                bps.cli, ["ping", "--wms-service-class", "wms_test_utils.WmsServiceSuccess"]
            )
            self.assertEqual(result.exit_code, 0)
            mock_driver.assert_called_with(wms_service="wms_test_utils.WmsServiceSuccess", pass_thru="")

    def testPingFailure(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.ping_driver") as mock_driver:
            mock_driver.return_value = 64  # avoid 1 and 2 as returned when cli problems
            result = self.runner.invoke(
                bps.cli, ["ping", "--wms-service-class", "wms_test_utils.WmsServiceFailure"]
            )
            self.assertEqual(result.exit_code, 64)
            mock_driver.assert_called_with(
                wms_service="wms_test_utils.WmsServiceFailure",
                pass_thru="",
            )

    def testPingPassthru(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.ping_driver") as mock_driver:
            mock_driver.return_value = 0
            result = self.runner.invoke(
                bps.cli,
                [
                    "ping",
                    "--wms-service-class",
                    "wms_test_utils.WmsServicePassThru",
                    "--pass-thru",
                    "EXTRA_VALUES",
                ],
            )
            self.assertEqual(result.exit_code, 0)
            mock_driver.assert_called_with(
                wms_service="wms_test_utils.WmsServicePassThru",
                pass_thru="EXTRA_VALUES",
            )


class TestCommandStatus(unittest.TestCase):
    """Test executing the status subcommand."""

    def setUp(self):
        self.runner = LogCliRunner()

    def testStatusSuccess(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.status_driver") as mock_driver:
            mock_driver.return_value = 0
            result = self.runner.invoke(
                bps.cli,
                [
                    "status",
                    "--wms-service-class",
                    "wms_test_utils.WmsServiceDummy",
                    "--id",
                    "100",
                ],
            )
            self.assertEqual(result.exit_code, 0)
            mock_driver.assert_called_with(
                wms_service="wms_test_utils.WmsServiceDummy",
                run_id="100",
                hist_days=0.0,
                is_global=False,
            )

    def testStatusMissingIDCommandLine(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.status_driver") as mock_driver:
            mock_driver.return_value = 0
            result = self.runner.invoke(
                bps.cli,
                [
                    "status",
                    "--wms-service-class",
                    "wms_test_utils.WmsServiceDummy",
                    "--user",
                    "user_not_allowed",
                ],
            )
            self.assertEqual(result.exit_code, 2)

    def testStatusNonZeroStatus(self):
        with unittest.mock.patch("lsst.ctrl.bps.cli.cmd.commands.status_driver") as mock_driver:
            mock_driver.return_value = 15
            result = self.runner.invoke(
                bps.cli,
                [
                    "status",
                    "--wms-service-class",
                    "wms_test_utils.WmsServiceDummy",
                    "--id",
                    "/dummy/path",
                    "--hist",
                    "6",
                    "--global",
                ],
            )
            self.assertEqual(result.exit_code, 15)
            mock_driver.assert_called_with(
                wms_service="wms_test_utils.WmsServiceDummy",
                run_id="/dummy/path",
                hist_days=6.0,
                is_global=True,
            )


if __name__ == "__main__":
    unittest.main()
