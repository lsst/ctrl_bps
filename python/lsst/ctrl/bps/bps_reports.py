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

"""Classes and functions used in reporting run status."""

__all__ = [
    "BaseRunReport",
    "DetailedRunReport",
    "ExitCodesReport",
    "SummaryRunReport",
    "compile_code_summary",
    "compile_job_summary",
]

import abc
import logging

from astropy.table import Table

from .wms_service import WmsRunReport, WmsStates

_LOG = logging.getLogger(__name__)


class BaseRunReport(abc.ABC):
    """The base class representing a run report.

    Parameters
    ----------
    fields : `list` [ `tuple` [ `str`, `str`]]
        The list of column specification, fields, to include in the report.
        Each field has a name and a type.
    """

    def __init__(self, fields):
        self._table = Table(dtype=fields)
        self._msg = None

    def __eq__(self, other):
        if isinstance(other, BaseRunReport):
            return self._table.pformat() == other._table.pformat()
        return False

    def __len__(self):
        """Return the number of runs in the report."""
        return len(self._table)

    def __str__(self):
        lines = list(self._table.pformat(max_lines=-1, max_width=-1))
        return "\n".join(lines)

    @property
    def message(self):
        """Extra information a method need to pass to its caller (`str`)."""
        return self._msg

    def clear(self):
        """Remove all entries from the report."""
        self._msg = None
        self._table.remove_rows(slice(len(self)))

    def sort(self, columns, ascending=True):
        """Sort the report entries according to one or more keys.

        Parameters
        ----------
        columns : `str` | `list` [ `str` ]
            The column(s) to order the report by.
        ascending : `bool`, optional
            Sort report entries in ascending order, default.

        Raises
        ------
        AttributeError
            Raised if supplied with non-existent column(s).
        """
        if isinstance(columns, str):
            columns = [columns]
        unknown_keys = set(columns) - set(self._table.colnames)
        if unknown_keys:
            raise AttributeError(
                f"cannot sort the report entries: column(s) {', '.join(unknown_keys)} not found"
            )
        self._table.sort(keys=columns, reverse=not ascending)

    @classmethod
    def from_table(cls, table):
        """Create a report from a table.

        Parameters
        ----------
        table : `astropy.table.Table`
            Information about a run in a tabular form.

        Returns
        -------
        inst : `lsst.ctrl.bps.bps_reports.BaseRunReport`
            A report created based on the information in the provided table.
        """
        inst = cls(table.dtype.descr)
        inst._table = table.copy()
        return inst

    @abc.abstractmethod
    def add(self, run_report, use_global_id=False):
        """Add a single run info to the report.

        Parameters
        ----------
        run_report : `lsst.ctrl.bps.WmsRunReport`
            Information for single run.
        use_global_id : `bool`, optional
            If set, use global run id. Defaults to False which means that
            the local id will be used instead.

            Only applicable in the context of a WMS using distributed job
            queues (e.g., HTCondor).
        """


class SummaryRunReport(BaseRunReport):
    """A summary run report."""

    def add(self, run_report, use_global_id=False):
        # Docstring inherited from the base class.

        # Flag any running workflow that might need human attention.
        run_flag = " "
        if run_report.state == WmsStates.RUNNING:
            if run_report.job_state_counts.get(WmsStates.HELD, 0):
                run_flag = "H"
            elif run_report.job_state_counts.get(WmsStates.DELETED, 0):
                run_flag = "D"
            elif run_report.job_state_counts.get(WmsStates.FAILED, 0):
                run_flag = "F"

        # Estimate success rate.
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
            run_report.global_wms_id if use_global_id else run_report.wms_id,
            run_report.operator,
            run_report.project,
            run_report.campaign,
            run_report.payload,
            run_report.run,
        )
        self._table.add_row(row)


class DetailedRunReport(BaseRunReport):
    """A detailed run report."""

    def add(self, run_report, use_global_id=False):
        # Docstring inherited from the base class.

        # If run summary exists, use it to get the reference job counts.
        by_label_expected = {}
        if run_report.run_summary:
            for part in run_report.run_summary.split(";"):
                label, count = part.split(":")
                by_label_expected[label] = int(count)

        total = ["TOTAL"]
        total.extend([run_report.job_state_counts[state] for state in WmsStates])
        total.append(sum(by_label_expected.values()) if by_label_expected else run_report.total_number_jobs)
        self._table.add_row(total)

        job_summary = run_report.job_summary
        if job_summary is None:
            id_ = run_report.global_wms_id if use_global_id else run_report.wms_id
            self._msg = f"WARNING: Job summary for run '{id_}' not available, report may be incomplete."
            return

        if by_label_expected:
            job_order = list(by_label_expected)
        else:
            job_order = sorted(job_summary)
            self._msg = "WARNING: Could not determine order of pipeline, instead sorted alphabetically."
        for label in job_order:
            try:
                counts = job_summary[label]
            except KeyError:
                counts = dict.fromkeys(WmsStates, -1)
            else:
                if label in by_label_expected:
                    already_counted = sum(counts.values())
                    if already_counted != by_label_expected[label]:
                        counts[WmsStates.UNREADY] += by_label_expected[label] - already_counted

            run = [label]
            run.extend([counts[state] for state in WmsStates])
            run.append(by_label_expected[label] if by_label_expected else -1)
            self._table.add_row(run)

    def __str__(self):
        alignments = ["<"] + [">"] * (len(self._table.colnames) - 1)
        lines = list(self._table.pformat(max_lines=-1, max_width=-1, align=alignments))
        lines.insert(3, lines[1])
        return str("\n".join(lines))


class ExitCodesReport(BaseRunReport):
    """An extension of run report to give information about
    error handling from the wms service.
    """

    def add(self, run_report: WmsRunReport, use_global_id: bool = False) -> None:
        # Docstring inherited from the base class.

        exit_code_summary = run_report.exit_code_summary
        if not exit_code_summary:
            id_ = run_report.global_wms_id if use_global_id else run_report.wms_id
            self._msg = f"WARNING: Exit code summary for run '{id_}' not available, report may be incomplete."
            return

        warnings = []

        # If available, use label ordering from the run summary as it should
        # reflect the ordering of the pipetasks in the pipeline.
        labels = []
        if run_report.run_summary:
            for part in run_report.run_summary.split(";"):
                label, _ = part.split(":")
                labels.append(label)
        if not labels:
            labels = sorted(exit_code_summary)
            warnings.append("WARNING: Could not determine order of pipeline, instead sorted alphabetically.")

        # Payload (e.g. pipetask) error codes:
        # * 1: general failure,
        # * 2: command line error (e.g. unknown command and/or option).
        pyld_error_codes = {1, 2}

        missing_labels = set()
        for label in labels:
            try:
                exit_codes = exit_code_summary[label]
            except KeyError:
                missing_labels.add(label)
            else:
                pyld_errors = [code for code in exit_codes if code in pyld_error_codes]
                pyld_error_count = len(pyld_errors)
                pyld_error_summary = (
                    ", ".join(sorted(str(code) for code in set(pyld_errors))) if pyld_errors else "None"
                )

                infra_errors = [code for code in exit_codes if code not in pyld_error_codes]
                infra_error_count = len(infra_errors)
                infra_error_summary = (
                    ", ".join(sorted(str(code) for code in set(infra_errors))) if infra_errors else "None"
                )

                run = [label, pyld_error_count, pyld_error_summary, infra_error_count, infra_error_summary]
                self._table.add_row(run)
        if missing_labels:
            warnings.append(
                f"WARNING: Exit code summary was not available for job labels: {', '.join(missing_labels)}"
            )
        if warnings:
            self._msg = "\n".join(warnings)

    def __str__(self):
        alignments = ["<"] + [">"] * (len(self._table.colnames) - 1)
        lines = list(self._table.pformat(max_lines=-1, max_width=-1, align=alignments))
        return str("\n".join(lines))


def compile_job_summary(report: WmsRunReport) -> list[str]:
    """Add a job summary to the run report if necessary.

    If the job summary is not provided, the function will attempt to compile
    it from information available for individual jobs (if any) and add it to
    the report. If the report already includes a job summary, the function is
    effectively a no-op.

    Parameters
    ----------
    report : `lsst.ctrl.bps.WmsRunReport`
        Information about a single run.

    Returns
    -------
    warnings : `list` [`str`]
        List of messages describing any non-critical issues encountered during
        processing. Empty if none.
    """
    warnings: list[str] = []

    # If the job summary already exists, exit early.
    if report.job_summary:
        return warnings

    if report.jobs:
        job_summary = {}
        by_label = group_jobs_by_label(report.jobs)
        for label, job_group in by_label.items():
            by_label_state = group_jobs_by_state(job_group)
            _LOG.debug("by_label_state = %s", by_label_state)
            counts = {state: len(jobs) for state, jobs in by_label_state.items()}
            job_summary[label] = counts
        report.job_summary = job_summary
    else:
        warnings.append("information about individual jobs not available")

    return warnings


def compile_code_summary(report: WmsRunReport) -> list[str]:
    """Add missing entries to the exit code summary if necessary.

    A WMS plugin may exclude job labels for which there are no failures from
    the exit code summary. The function will attempt to use the job summary,
    if available, to add missing entries for these labels.

    Parameters
    ----------
    report : `lsst.ctrl.bps.WmsRunReport`
        Information about a single run.

    Returns
    -------
    warnings : `list` [`str`]
        List of messages describing any non-critical issues encountered during
        processing. Empty if none.
    """
    warnings: list[str] = []

    # If the job summary is not available, exit early.
    if not report.job_summary:
        return warnings

    # A shallow copy is enough here because we won't be modifying the existing
    # entries, only adding new ones if necessary.
    exit_code_summary = dict(report.exit_code_summary) if report.exit_code_summary else {}

    # Use the job summary to add the entries for labels with no failures
    # *without* modifying already existing entries.
    failure_summary = {label: states[WmsStates.FAILED] for label, states in report.job_summary.items()}
    for label, count in failure_summary.items():
        if count == 0:
            exit_code_summary.setdefault(label, [])

    # Check if there are any discrepancies between the data in the exit code
    # summary and the job summary.
    code_summary_labels = set(exit_code_summary)
    failure_summary_labels = set(failure_summary)
    mismatches = {
        label
        for label in failure_summary_labels & code_summary_labels
        if len(exit_code_summary[label]) != failure_summary[label]
    }
    if mismatches:
        warnings.append(
            f"number of exit codes differs from number of failures for job labels: {', '.join(mismatches)}"
        )
    missing = failure_summary_labels - code_summary_labels
    if missing:
        warnings.append(f"exit codes not available for job labels: {', '.join(missing)}")

    if exit_code_summary:
        report.exit_code_summary = exit_code_summary

    return warnings


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
    by_label : `dict` [`str`, `list` [`lsst.ctrl.bps.WmsJobReport`]]
        Mapping of job state to a list of jobs.
    """
    by_label = {}
    for job in jobs:
        group = by_label.setdefault(job.label, [])
        group.append(job)
    return by_label
