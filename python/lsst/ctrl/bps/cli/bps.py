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
from lsst.daf.butler.cli.opt import (
    log_file_option,
    log_label_option,
    log_level_option,
    log_tty_option,
    long_log_option,
)
from lsst.daf.butler.cli.utils import unwrap

epilog = unwrap(
    """Note:

Commands 'acquire', 'cluster', 'transform', and 'prepare' halt the submission
process at different points prior to submitting the workflow for execution. See
the help for individual command for more details.

However, the current version does not support starting the submission process
from any of these intermediate points.
"""
)


class BpsCli(LoaderCLI):
    localCmdPkg = "lsst.ctrl.bps.cli.cmd"


@click.command(cls=BpsCli, context_settings=dict(help_option_names=["-h", "--help"]), epilog=epilog)
@log_level_option(default=["INFO"])
@long_log_option()
@log_file_option()
@log_tty_option()
@log_label_option()
def cli(log_level, long_log, log_file, log_tty, log_label):
    pass


def main():
    return cli()
