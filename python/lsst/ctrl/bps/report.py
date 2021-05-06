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

"""Supporting functions for reporting on runs submitted to a WMS

Note: Expectations are that future reporting effort will revolve around
   LSST oriented database tables.
"""

import logging

from .wms_service import WmsStates


SUMMARY_FMT = "{:1} {:>10} {:>3} {:>9} {:10} {:10} {:20} {:20} {:<60}"

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""

_LOG = logging.getLogger(__name__)


def print_headers():
    """Print headers.
    """
    print(SUMMARY_FMT.format("X", "STATE", "%S", "ID", "OPERATOR", "PRJ", "CMPGN", "PAYLOAD", "RUN"))
    print("-" * 156)


def print_run(run_report):
    """Print single run info.

    Parameters
    ----------
    run_report : `WmsRunReport`
        Information for single run.
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

    print(SUMMARY_FMT.format(run_flag, run_report.state.name, percent_succeeded, run_report.wms_id,
                             run_report.operator[:10], run_report.project[:10], run_report.campaign[:20],
                             run_report.payload[:20], run_report.run[:60]))


def group_jobs_by_state(jobs):
    """Divide given jobs into groups based on their state value.

    Parameters
    ----------
    jobs : `list` of `~lsst.ctrl.bps.wms_service.WmsJobReport`
        Jobs to divide into groups based on state.

    Returns
    -------
    by_state : `dict`
        Mapping of job state to a list of jobs.
    """
    _LOG.debug("group_jobs_by_state: jobs=%s", jobs)
    by_state = dict.fromkeys(WmsStates)
    for state in by_state:
        by_state[state] = []    # Note: If added [] to fromkeys(), they shared single list.

    for job in jobs:
        by_state[job.state].append(job)
    return by_state


def group_jobs_by_label(jobs):
    """Divide given jobs into groups based on their label value.

    Parameters
    ----------
    jobs : `list` of `~lsst.ctrl.bps.wms_service.WmsJobReport`
        Jobs to divide into groups based on label.

    Returns
    -------
    by_label : `dict`
        Mapping of job state to a list of jobs .
    """
    by_label = {}
    for job in jobs:
        if job.label not in by_label:
            by_label[job.label] = []
        by_label[job.label].append(job)
    return by_label


def print_single_run_summary(run_report):
    """Print runtime info for single run including job summary per task abbrev.

    Parameters
    ----------
    run_report : `~lsst.ctrl.bps.wms_service.WmsRunReport`
        Summary runtime info for a run + runtime info for jobs.
    """
    # Print normal run summary.
    print_headers()
    print_run(run_report)
    print("\n\n")

    # Print more run information.
    print(f"Path: {run_report.path}\n")

    print(f"{'':35} {' | '.join([f'{s.name[:6]:6}' for s in WmsStates])}")
    print(f"{'Total':35} {' | '.join([f'{run_report.job_state_counts[s]:6}' for s in WmsStates])}")
    print("-" * (35 + 3 + (6 + 2) * (len(run_report.job_state_counts) + 1)))

    by_label = group_jobs_by_label(run_report.jobs)

    # Print job level info by print counts of jobs by label and WMS state.
    label_order = []
    by_label_totals = {}
    if run_report.run_summary:
        # Workaround until get pipetaskInit job into run_summary
        if not run_report.run_summary.startswith("pipetaskInit"):
            label_order.append("pipetaskInit")
            by_label_totals["pipetaskInit"] = 1
        for part in run_report.run_summary.split(";"):
            label, count = part.split(":")
            label_order.append(label)
            by_label_totals[label] = int(count)
    else:
        print("Warning: Cannot determine order of pipeline.  Instead printing alphabetical.")
        label_order = sorted(by_label.keys())

    for label in label_order:
        counts = dict.fromkeys(WmsStates, 0)
        if label in by_label:
            by_label_state = group_jobs_by_state(by_label[label])
            _LOG.debug("by_label_state = %s", by_label_state)
            counts = dict.fromkeys(WmsStates)
            for state in WmsStates:
                counts[state] = len(by_label_state[state])
        elif label in by_label_totals:
            already_counted = sum(counts.values())
            if already_counted != by_label_totals[label]:
                counts[WmsStates.UNREADY] += by_label_totals[label] - already_counted
        else:
            counts = dict.fromkeys(WmsStates, -1)
        print(f"{label[:35]:35} {' | '.join([f'{counts[s]:6}' for s in WmsStates])}")
    print("\n")
