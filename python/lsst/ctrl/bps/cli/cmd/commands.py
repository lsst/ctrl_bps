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
"""Subcommand definitions."""

import logging
import shutil
from collections.abc import Iterator
from contextlib import contextmanager

import click

from lsst.daf.butler.cli.utils import MWCommand
from lsst.utils.timer import time_this

from ... import BpsSubprocessError
from ...constants import (
    DEFAULT_MEM_FMT,
    DEFAULT_MEM_UNIT,
)
from ...drivers import (
    acquire_qgraph_driver,
    cancel_driver,
    cluster_qgraph_driver,
    ping_driver,
    prepare_driver,
    report_driver,
    restart_driver,
    status_driver,
    submit_driver,
    submitcmd_driver,
    transform_driver,
)
from ...summary import BpsSummary
from .. import opt

_LOG = logging.getLogger(__name__)


def save_summary(summary: BpsSummary) -> None:
    """Save the summary file.

    Parameters
    ----------
    summary : `lsst.ctrl.bps.BpsSummary`
        Summary information to save.
    """
    if summary.submit_path and summary.name:
        filename = f"{summary.submit_path}/{summary.name}_summary.json"
        summary.save(filename)
        if summary.user_copy:
            shutil.copy2(filename, summary.user_copy)
    elif summary.user_copy:
        print("Not enough summary information.  Skipping writing.")


@contextmanager
def catch_errors(summary: BpsSummary | None = None) -> Iterator[None]:
    """Handle errors that occurred during command execution.

    Returns
    -------
    context : `contextlib.AbstractContextManager` [ `None` ]
        A context manager that does not return a value when entered.

    Notes
    -----
    At the moment, the main responsibility of this context manager is to catch
    the exception related to failures of the commands BPS runs in its
    subprocesses to force Click CLI report command's actual exit code instead
    of always returning 1.
    """
    try:
        yield None
    except BpsSubprocessError as e:
        if summary:
            summary.set_exception(e)
            summary.exit_code = e.errno
            save_summary(summary)
        click.echo(e)
        click.get_current_context().exit(e.errno)
    except BaseException as e:
        print(e)
        print(dir(e))
        if summary:
            summary.set_exception(e)
            save_summary(summary)
        raise


class BpsCommand(MWCommand):
    """Command subclass with bps-command specific overrides."""

    extra_epilog = "See 'bps --help' for more options."


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.submission_options()
def acquire(*args, **kwargs):
    """Create a new quantum graph or read existing one from a file."""
    summary = BpsSummary(
        user_copy=kwargs["submit_summary_file"], include_details=kwargs["include_summary_details"]
    )
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Acquire process completed",
        mem_usage=True,
        mem_child=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ) as metadata:
        with catch_errors(summary):
            acquire_qgraph_driver(*args, **kwargs)
    summary.store_time_this_metadata(metadata)
    save_summary(summary)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.submission_options()
def cluster(*args, **kwargs):
    """Create a clustered quantum graph."""
    summary = BpsSummary(
        user_copy=kwargs["submit_summary_file"], include_details=kwargs["include_summary_details"]
    )
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Cluster process completed",
        mem_usage=True,
        mem_child=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ) as metadata:
        with catch_errors(summary):
            cluster_qgraph_driver(*args, **kwargs)
    summary.store_time_this_metadata(metadata)
    save_summary(summary)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.submission_options()
def transform(*args, **kwargs):
    """Transform a quantum graph to a generic workflow."""
    summary = BpsSummary(
        user_copy=kwargs["submit_summary_file"], include_details=kwargs["include_summary_details"]
    )
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Transform process completed",
        mem_usage=True,
        mem_child=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ) as metadata:
        with catch_errors(summary):
            transform_driver(*args, **kwargs)
    summary.store_time_this_metadata(metadata)
    save_summary(summary)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.wms_service_option()
@opt.submission_options()
def prepare(*args, **kwargs):
    """Prepare a workflow for submission."""
    summary = BpsSummary(
        user_copy=kwargs["submit_summary_file"], include_details=kwargs["include_summary_details"]
    )
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Prepare process completed",
        mem_usage=True,
        mem_child=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ) as metadata:
        with catch_errors(summary):
            prepare_driver(*args, **kwargs)
    summary.store_time_this_metadata(metadata)
    save_summary(summary)


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.wms_service_option()
@opt.compute_site_option()
@opt.submission_options()
def submit(*args, **kwargs):
    """Submit a workflow for execution."""
    summary = BpsSummary(
        user_copy=kwargs["submit_summary_file"], include_details=kwargs["include_summary_details"]
    )
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Submission process completed",
        mem_usage=True,
        mem_child=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ) as metadata:
        with catch_errors(summary):
            submit_driver(*args, summary=summary, **kwargs)
    summary.store_time_this_metadata(metadata)
    save_summary(summary)


@click.command(cls=BpsCommand)
@opt.wms_service_option()
@click.option("--id", "run_id", help="Run id of workflow to restart.")
def restart(*args, **kwargs):
    """Restart a failed workflow."""
    restart_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.wms_service_option()
@click.option("--id", "run_id", help="Restrict report to specific WMS run id.")
@click.option("--user", help="Restrict report to specific user.")
@click.option("--hist", "hist_days", default=0.0, help="Search WMS history X days for completed info.")
@click.option("--pass-thru", help="Pass the given string to the WMS service class.")
@click.option(
    "--return-exit-codes",
    is_flag=True,
    show_default=True,
    default=False,
    help="Return exit codes from jobs with a non-success status.",
)
@click.option(
    "--global/--no-global",
    "is_global",
    default=False,
    help="Query all available job queues for job information.",
)
def report(*args, **kwargs):
    """Display execution information for submitted workflows."""
    report_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.wms_service_option()
@click.option("--id", "run_id", required=True, help="Restrict report to specific WMS run id.")
@click.option("--hist", "hist_days", default=0.0, help="Search WMS history X days for completed info.")
@click.option(
    "--global/--no-global",
    "is_global",
    default=False,
    help="Query all available job queues for job information.",
)
def status(*args, **kwargs):
    """Exit with execution status of single submitted workflow."""
    # Note: Using return statement doesn't actually return the value
    # to the shell.  Using click function instead.
    click.get_current_context().exit(status_driver(*args, **kwargs))


@click.command(cls=BpsCommand)
@opt.wms_service_option()
@click.option("--id", "run_id", help="Run id of workflow to cancel.")
@click.option("--user", help="User for which to cancel all submitted workflows.")
@click.option(
    "--require-bps/--skip-require-bps",
    "require_bps",
    default=True,
    show_default=True,
    help="Only cancel jobs submitted via bps.",
)
@click.option("--pass-thru", "pass_thru", default="", help="Pass the given string to the WMS service.")
@click.option(
    "--global/--no-global",
    "is_global",
    default=False,
    help="Cancel jobs matching the search criteria from all job queues.",
)
def cancel(*args, **kwargs):
    """Cancel submitted workflow(s)."""
    cancel_driver(*args, **kwargs)


@click.command(cls=BpsCommand)
@opt.wms_service_option()
@click.option("--pass-thru", "pass_thru", default="", help="Pass the given string to the WMS service.")
def ping(*args, **kwargs):
    """Ping workflow services."""
    # Note: Using return statement doesn't actually return the value
    # to the shell.  Using click function instead.
    click.get_current_context().exit(ping_driver(*args, **kwargs))


@click.command(cls=BpsCommand)
@opt.config_file_argument(required=True)
@opt.wms_service_option()
@opt.compute_site_option()
@click.option("--dry-run", "dry_run", is_flag=True, help="Prepare workflow but don't submit")
def submitcmd(*args, **kwargs):
    """Submit a command for execution."""
    submitcmd_driver(*args, **kwargs)
