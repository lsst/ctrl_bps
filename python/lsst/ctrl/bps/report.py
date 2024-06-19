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

__all__ = ["BPS_POSTPROCESSORS", "display_report", "retrieve_report"]

import logging
from collections.abc import Callable, Sequence

from lsst.utils import doImport

from .bps_reports import DetailedRunReport, ExitCodesReport, SummaryRunReport, compile_job_summary
from .wms_service import WmsRunReport, WmsStates

BPS_POSTPROCESSORS = (compile_job_summary,)
"""Postprocessors for massaging run reports
(`tuple` [`Callable` [[`WmsRunReport`], None]).
"""

_LOG = logging.getLogger(__name__)


def display_report(
    wms_service: str,
    *,
    run_id: str | None = None,
    user: str | None = None,
    hist: int | None = None,
    pass_thru: str | None = None,
    is_global: bool = False,
    return_exit_codes: bool = False,
) -> None:
    """Print out summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service : `str`
        Name of the WMS service class.
    run_id : `str`, optional
        A run id the report will be restricted to.
    user : `str`, optional
        A username the report will be restricted to.
    hist : int, optional
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

    if run_id:
        fields = [(" ", "S")] + [(state.name, "i") for state in WmsStates] + [("EXPECTED", "i")]
        run_report = DetailedRunReport(fields)

        # When reporting on single run, increase history until better mechanism
        # for handling completed jobs is available.
        hist = max(hist, 2)

        runs, messages = retrieve_report(
            wms_service,
            run_id=run_id,
            hist=hist,
            pass_thru=pass_thru,
            is_global=is_global,
            postprocessors=BPS_POSTPROCESSORS,
        )

        for run in runs:
            run_brief.add(run, use_global_id=is_global)
            run_report.add(run, use_global_id=is_global)
            if run_report.message:
                print(run_report.message)

            print(run_brief)
            print("\n")
            print(f"Path: {run.path}")
            print(f"Global job id: {run.global_wms_id}")
            print("\n")
            print(run_report)

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
                print("\n")
                print(run_exits_report)
                run_exits_report.clear()

            run_brief.clear()
            run_report.clear()
        if not runs and not messages:
            print(
                f"No records found for job id '{run_id}'. "
                f"Hints: Double check id, retry with a larger --hist value (currently: {hist}), "
                "and/or use --global to search all job queues."
            )
    else:
        runs, messages = retrieve_report(
            wms_service,
            user=user,
            hist=hist,
            pass_thru=pass_thru,
            is_global=is_global,
        )
        for run in runs:
            run_brief.add(run, use_global_id=is_global)
        run_brief.sort("ID")
        print(run_brief)

    if messages:
        print("\n".join(messages))
        print("\n")


def retrieve_report(
    wms_service: str,
    *,
    run_id: str | None = None,
    user: str | None = None,
    hist: float | None = None,
    pass_thru: str | None = None,
    is_global: bool = False,
    postprocessors: Sequence[Callable[[WmsRunReport], None]] | None = None,
) -> tuple[list[WmsRunReport], list[str]]:
    """Retrieve summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service : `str`
        Name of the WMS service class.
    run_id : `str`, optional
        A run id the report will be restricted to.
    user : `str`, optional
        A username the report will be restricted to.
    hist : int, optional
        Include runs from the given number of past days.
    pass_thru : `str`, optional
        A string to pass directly to the WMS service class.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
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
    """
    messages = []

    wms_service_class = doImport(wms_service)
    wms_service = wms_service_class({})
    reports, message = wms_service.report(
        wms_workflow_id=run_id, user=user, hist=hist, pass_thru=pass_thru, is_global=is_global
    )
    if message:
        messages.append(message)

    if postprocessors:
        for report in reports:
            for postprocessor in postprocessors:
                try:
                    postprocessor(report)
                except Exception as exc:
                    messages.append(
                        f"Postprocessing error for '{report.wms_id}': {str(exc)} "
                        f"(origin: {postprocessor.__name__})"
                    )

    return reports, messages
