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

from astropy.table import Table
from lsst.utils import doImport

from . import WmsStates

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
        A user name the report will be restricted to.
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

    if run_id:
        for run in runs:
            print_single_run_summary(run, is_global=is_global)
        if not runs and not message:
            print(
                f"No records found for job id '{run_id}'. "
                f"Hints: Double check id, retry with a larger --hist value (currently: {hist_days}), "
                f"and/or use --global to search all job queues."
            )
    else:
        summary = init_summary()
        for run in sorted(runs, key=lambda j: j.wms_id if not is_global else j.global_wms_id):
            summary = add_single_run_summary(summary, run, is_global=is_global)
        for line in summary.pformat_all():
            print(line)
    if message:
        print(message)
        print("\n\n")


def init_summary():
    """Initialize the summary report table.

    Returns
    -------
    table : `astropy.table.Table`
        Initialized summary report table.
    """
    columns = [
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
    return Table(dtype=columns)


def add_single_run_summary(summary, run_report, is_global=False):
    """Add a single run info to the summary.

    Parameters
    ----------
    summary : `astropy.tables.Table`
        The table representing the run summary.
    run_report : `lsst.ctrl.bps.WmsRunReport`
        Information for single run.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    # Flag any running workflow that might need human attention
    run_flag = " "
    if run_report.state == WmsStates.RUNNING:
        if run_report.job_state_counts.get(WmsStates.FAILED, 0):
            run_flag = "F"
        elif run_report.job_state_counts.get(WmsStates.DELETED, 0):
            run_flag = "D"
        elif run_report.job_state_counts.get(WmsStates.HELD, 0):
            run_flag = "H"

    percent_succeeded = "UNK"
    _LOG.debug("total_number_jobs = %s", run_report.total_number_jobs)
    _LOG.debug("run_report.job_state_counts = %s", run_report.job_state_counts)
    if run_report.total_number_jobs:
        succeeded = run_report.job_state_counts.get(WmsStates.SUCCEEDED, 0)
        _LOG.debug("succeeded = %s", succeeded)
        percent_succeeded = f"{int(succeeded / run_report.total_number_jobs * 100)}"

    row = (
        run_flag,
        run_report.state.name,
        percent_succeeded,
        run_report.global_wms_id if is_global else run_report.wms_id,
        run_report.operator,
        run_report.project,
        run_report.campaign,
        run_report.payload,
        run_report.run,
    )
    summary.add_row(row)
    return summary


def group_jobs_by_state(jobs):
    """Divide given jobs into groups based on their state value.

    Parameters
    ----------
    jobs : `list` [`lsst.ctrl.bps.WmsJobReport`]
        Jobs to divide into groups based on state.

    Returns
    -------
    by_state : `dict`
        Mapping of job state to a list of jobs.
    """
    _LOG.debug("group_jobs_by_state: jobs=%s", jobs)
    by_state = {state: [] for state in WmsStates}
    for job in jobs:
        by_state[job.state].append(job)
    return by_state


def group_jobs_by_label(jobs):
    """Divide given jobs into groups based on their label value.

    Parameters
    ----------
    jobs : `list` [`lsst.ctrl.bps.WmsJobReport`]
        Jobs to divide into groups based on label.

    Returns
    -------
    by_label : `dict` [`str`, `lsst.ctrl.bps.WmsJobReport`]
        Mapping of job state to a list of jobs.
    """
    by_label = {}
    for job in jobs:
        group = by_label.setdefault(job.label, [])
        group.append(job)
    return by_label


def print_single_run_summary(run_report, is_global=False):
    """Print runtime info for single run including job summary per task abbrev.

    Parameters
    ----------
    run_report : `lsst.ctrl.bps.WmsRunReport`
        Summary runtime info for a run + runtime info for jobs.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    # Print normal run summary.
    summary = init_summary()
    summary = add_single_run_summary(summary, run_report, is_global=is_global)
    for line in summary.pformat_all():
        print(line)
    print("\n\n")

    # Print more run information.
    print(f"Path: {run_report.path}")
    print(f"Global job id: {run_report.global_wms_id}")
    print("\n\n")

    by_label = group_jobs_by_label(run_report.jobs)

    # Count the jobs by label and WMS state.
    label_order = []
    by_label_expected = {}
    if run_report.run_summary:
        for part in run_report.run_summary.split(";"):
            label, count = part.split(":")
            label_order.append(label)
            by_label_expected[label] = int(count)
    else:
        print("Warning: Cannot determine order of pipeline.  Instead printing alphabetical.")
        label_order = sorted(by_label.keys())

    # Initialize table for saving the detailed run info.
    columns = [(" ", "S")] + [(s.name, "i") for s in WmsStates] + [("EXPECTED", "i")]
    details = Table(dtype=columns)

    total = ["TOTAL"]
    total.extend([run_report.job_state_counts[state] for state in WmsStates])
    total.append(sum(by_label_expected.values()))
    details.add_row(total)

    for label in label_order:
        if label in by_label:
            by_label_state = group_jobs_by_state(by_label[label])
            _LOG.debug("by_label_state = %s", by_label_state)
            counts = {state: len(jobs) for state, jobs in by_label_state.items()}
            if label in by_label_expected:
                already_counted = sum(counts.values())
                if already_counted != by_label_expected[label]:
                    counts[WmsStates.UNREADY] += by_label_expected[label] - already_counted
        else:
            counts = dict.fromkeys(WmsStates, -1)

        row = [label]
        row.extend([counts[state] for state in WmsStates])
        row.append([by_label_expected[label]])
        details.add_row(row)

    # Format the report summary and print it out.
    alignments = ["<"]
    alignments.extend([">" for _ in WmsStates])
    alignments.append(">")
    lines = details.pformat_all(align=alignments)
    lines.insert(3, lines[1])
    for line in lines:
        print(line)
