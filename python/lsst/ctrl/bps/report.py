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

"""Supporting functions for reporting on runs submitted to a WMS.

Note: Expectations are that future reporting effort will revolve around LSST
oriented database tables.
"""

import logging

from lsst.utils import doImport

from .bps_reports import DetailedRunReport, SummaryRunReport
from .wms_service import WmsStates

_LOG = logging.getLogger(__name__)


def report(wms_service, run_id, user, hist_days, pass_thru, is_global=False):
    """Print out summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service : `str`
        Name of the class.
    run_id : `str`
        A run id the report will be restricted to.
    user : `str`
        A username the report will be restricted to.
    hist_days : int
        Number of days
    pass_thru : `str`
        A string to pass directly to the WMS service class.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    wms_service_class = doImport(wms_service)
    wms_service = wms_service_class({})

    # If reporting on single run, increase history until better mechanism
    # for handling completed jobs is available.
    if run_id:
        hist_days = max(hist_days, 2)

    runs, message = wms_service.report(run_id, user, hist_days, pass_thru, is_global=is_global)

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

            run_brief.clear()
            run_report.clear()
        if not runs and not message:
            print(
                f"No records found for job id '{run_id}'. "
                f"Hints: Double check id, retry with a larger --hist value (currently: {hist_days}), "
                "and/or use --global to search all job queues."
            )
    else:
        for run in runs:
            run_brief.add(run, use_global_id=is_global)
        run_brief.sort("ID")
        print(run_brief)
    if message:
        print(message)
        print("\n")
