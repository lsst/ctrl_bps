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

from lsst.ctrl.bps.cli import bps
from lsst.daf.butler.cli.utils import LogCliRunner


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


if __name__ == "__main__":
    unittest.main()
