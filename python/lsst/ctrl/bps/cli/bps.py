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
import click

from lsst.daf.butler.cli.butler import LoaderCLI
from lsst.daf.butler.cli.opt import log_level_option, long_log_option


class BpsCli(LoaderCLI):

    localCmdPkg = "lsst.ctrl.bps.cli.cmd"


@click.command(cls=BpsCli,
               context_settings=dict(help_option_names=["-h", "--help"]))
@log_level_option(default=["WARNING"])
@long_log_option()
def cli(log_level, long_log):
    pass


def main():
    return cli()
