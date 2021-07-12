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
"""Subcommand definitions.
"""
import click

from lsst.daf.butler.cli.utils import MWCommand
from ...drivers import (
    acquire_qgraph_driver,
    cluster_qgraph_driver,
    transform_driver,
    prepare_driver,
    submit_driver,
    report_driver,
    cancel_driver
)
from .. import opt


class BpsCommand(MWCommand):
    """Command subclass with bps-command specific overrides."""

    extra_epilog = "See 'bps --help' for more options."


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
def acquire(*args, **kwargs):
    """Create a new quantum graph or read existing one from a file.
    """
    acquire_qgraph_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
def cluster(*args, **kwargs):
    """Create a clustered quantum graph.
    """
    cluster_qgraph_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
def transform(*args, **kwargs):
    """Transform a quantum graph to a generic workflow.
    """
    transform_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
def prepare(*args, **kwargs):
    """Prepare a workflow for submission.
    """
    prepare_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
def submit(*args, **kwargs):
    """Submit a workflow for execution.
    """
    submit_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@click.option("--wms", "wms_service",
              default="lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorService",
              help="Workload Management System service class")
@click.option("--id", "run_id",
              help="Restrict report to specific WMS run id.")
@click.option("--user",
              help="Restrict report to specific user.")
@click.option("--hist", "hist_days",
              default=0.0,
              help="Search WMS history X days for completed info.")
@click.option("--pass-thru",
              help="Pass the given string to the WMS service class")
def report(*args, **kwargs):
    """Display execution status for submitted workflows.
    """
    report_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@click.option("--wms", "wms_service",
              default="lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorService",
              help="Workload Management System service class.")
@click.option("--id", "run_id",
              help="Run id of workflow to cancel.")
@click.option("--user",
              help="User for which to cancel all submitted workflows.")
@click.option("--require-bps/--skip-require-bps", "require_bps", default=True, show_default=True,
              help="Only cancel jobs submitted via bps.")
@click.option("--pass-thru", "pass_thru", default=str(),
              help="Pass the given string to the WMS service.")
def cancel(*args, **kwargs):
    """Cancel submitted workflow(s).
    """
    cancel_driver(*args, **kwargs)
