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

"""Supporting functions for reporting on runs submitted to a WMS.

Note: Expectations are that future reporting effort will revolve around LSST
oriented database tables.
"""

__all__ = ["display_report", "retrieve_report"]

import logging
import sys
from collections.abc import Callable, Sequence
from typing import TextIO

from lsst.utils import doImportType

from .bps_reports import DetailedRunReport, ExitCodesReport, SummaryRunReport
from .wms_service import BaseWmsService, WmsRunReport, WmsStates

_LOG = logging.getLogger(__name__)


def display_report(
    runs: list[WmsRunReport],
    messages: list[str],
    is_detailed: bool = False,
    is_global: bool = False,
    return_exit_codes: bool = False,
    file: TextIO = sys.stdout,
) -> None:
    """Print out summary of jobs submitted for execution.

    Parameters
    ----------
    runs : `list` [`str`]
        Runs to include in the summary.
    messages : `list` [`str`]
        Errors that happened during report and/or processing. Empty if
        no issues were encountered.
    is_detailed : `bool`, optional
        If set, the function prints out a detailed report including statuses
        of each task in the workflow grouped by task labels. By default, only
        a brief summary of each run is displayed.
    is_global : `bool`, optional
        If set, a global run id(s) will be used when displaying the report.
        By default, the report will use local run id(s).

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    return_exit_codes : `bool`, optional
        If set, return exit codes related to jobs with a
        non-success status. Defaults to False, which means that only
        the summary state is returned.

        Only applicable in the context of a WMS with associated
        handlers to return exit codes from jobs.
    file : TextIO
        File or file-like object to write the output to.
    """
    run_brief = SummaryRunReport(
        [
            ("X", "S"),
            ("STATE", "S"),
            ("%S", "S"),
            ("ID", "S"),
            ("OPERATOR", "S"),
            ("PROJECT", "S"),
            ("CAMPAIGN", "S"),
            ("PAYLOAD", "S"),
            ("RUN", "S"),
        ]
    )

    if is_detailed:
        fields = [(" ", "S")] + [(state.name, "i") for state in WmsStates] + [("EXPECTED", "i")]
        run_report = DetailedRunReport(fields)

        for run in runs:
            run_brief.add(run, use_global_id=is_global)

            run_report.add(run, use_global_id=is_global)
            if run_report.message:
                messages.append(run_report.message)

            print(run_brief, file=file)
            print("\n", file=file)
            print(f"Path: {run.path}", file=file)
            print(f"Global job id: {run.global_wms_id}", file=file)
            if run.specific_info:
                print(run.specific_info, file=file)
            print("\n", file=file)
            print(run_report, file=file)

            if return_exit_codes:
                fields = [
                    (" ", "S"),
                    ("PAYLOAD ERROR COUNT", "i"),
                    ("PAYLOAD ERROR CODES", "S"),
                    ("INFRASTRUCTURE ERROR COUNT", "i"),
                    ("INFRASTRUCTURE ERROR CODES", "S"),
                ]
                run_exits_report = ExitCodesReport(fields)
                run_exits_report.add(run, use_global_id=is_global)
                if run_exits_report.message:
                    messages.append(run_exits_report.message)
                print("\n", file=file)
                print(run_exits_report, file=file)
                run_exits_report.clear()

            run_brief.clear()
            run_report.clear()
    else:
        for run in runs:
            run_brief.add(run, use_global_id=is_global)
        run_brief.sort("ID")
        print(run_brief, file=file)

    if messages:
        uniques = list(dict.fromkeys(messages))
        print("\n".join(uniques), file=file)
        print("\n", file=file)


def retrieve_report(
    wms_service_fqn: str,
    *,
    run_id: str | None = None,
    user: str | None = None,
    hist: float | None = None,
    pass_thru: str | None = None,
    is_global: bool = False,
    return_exit_codes: bool = False,
    postprocessors: Sequence[Callable[[WmsRunReport], list[str]]] | None = None,
) -> tuple[list[WmsRunReport], list[str]]:
    """Retrieve summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service_fqn : `str`
        Name of the WMS service class.
    run_id : `str`, optional
        A run id the report will be restricted to.
    user : `str`, optional
        A username the report will be restricted to.
    hist : `float`, optional
        Include runs from the given number of past days.
    pass_thru : `str`, optional
        A string to pass directly to the WMS service class.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    return_exit_codes : `bool`, optional
        If set, return exit codes related to jobs with a
        non-success status. Defaults to False, which means that only
        the summary state is returned.

        Only applicable in the context of a WMS with associated
        handlers to return exit codes from jobs.
    postprocessors : `collections.abc.Sequence` [callable], optional
        List of functions for "massaging" reports returned by the plugin. Each
        function must take one positional argument:

        - ``report``: run report (`lsst.ctrl.bps.WmsRunReport`)

        If None (default), each run report returned by the plugin (if any)
        will be returned as is.

    Returns
    -------
    reports : `list` [`WmsRunReport`]
        Run reports satisfying the search criteria.
    messages : `list` [`str`]
        Errors that happened during report retrieval and/or processing.
        Empty if no issues were encountered.

    Raises
    ------
    TypeError
        Raised if the WMS service class is not a subclass of BaseWmsService.
    """
    messages: list[str] = []

    wms_service_class = doImportType(wms_service_fqn)
    if not issubclass(wms_service_class, BaseWmsService):
        raise TypeError(
            f"Invalid WMS service class '{wms_service_fqn}'; must be a subclass of BaseWmsService"
        )
    wms_service = wms_service_class({})

    reports, message = wms_service.report(
        wms_workflow_id=run_id,
        user=user,
        hist=hist,
        pass_thru=pass_thru,
        is_global=is_global,
        return_exit_codes=return_exit_codes,
    )
    if message:
        messages.append(message)

    if postprocessors:
        for report in reports:
            for postprocessor in postprocessors:
                if warnings := postprocessor(report):
                    for warning in warnings:
                        messages.append(
                            f"WARNING: Report may be incomplete. "
                            f"There was an issue with report postprocessing for '{report.wms_id}': "
                            f"{warning} (origin: {postprocessor.__name__})"
                        )

    return reports, messages
