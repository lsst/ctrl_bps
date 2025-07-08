# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""BPS summary classes."""

__all__ = [
    "AcquireStageSummary",
    "BpsSummary",
    "ClusterStageSummary",
    "PrepareStageSummary",
    "SubmitStageSummary",
    "TransformStageSummary",
]


import socket
import sys
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import pydantic

from lsst.ctrl.mpexec import ExceptionInfo
from lsst.pipe.base import QgraphSummary
from lsst.utils.timer import _TimerResult

from .clustered_qgraph_summary import ClusteredQgraphSummary


class BaseSummary(pydantic.BaseModel):
    """Common summary information."""

    start_UTC: datetime = pydantic.Field(default_factory=lambda: datetime.now(UTC))
    """Start time of stage."""
    duration: float = 0.0
    """Duration of context in seconds."""
    mem_current_usage: float | None = None
    """Memory usage at end of context in requested units."""
    mem_peak_delta: float | None = None
    """Peak differential between entering and leaving context."""
    mem_current_delta: float | None = None
    """Difference in usage between entering and leaving context."""
    details: pydantic.BaseModel | None = pydantic.Field(exclude=True, default=None)
    """More detailed summary that usually grows in size."""
    details_filename: str | None = None
    """Location where details were saved."""
    details_type: str | None = None
    """Type of details to be used for validation."""

    def store_time_this_metadata(self, timer_result: _TimerResult):
        """Save values from a time_this call.

        Parameters
        ----------
        timer_result : `lsst.utils._TimerResult`
            Values to save into this summary.
        """
        self.duration = timer_result.duration
        self.mem_current_usage = timer_result.mem_current_usage
        self.mem_peak_delta = timer_result.mem_peak_delta
        self.mem_current_delta = timer_result.mem_current_delta

    def load_details(self) -> None:
        if self.details_filename:
            with open(self._details_filename) as fh:
                json_data = fh.read()
                self.details = pydantic.TypeAdapter(self._details_type).validate_python(json_data)

    def save_details(self, output_dir: str) -> None:
        if self.details:
            self._details_type = type(self.details).__name__
            self._details_filename = f"{output_dir}/{self._details_type}.json"
            with open(self._details_filename, "w") as fh:
                fh.write(self.model_dump_json(exclude_none=True, indent=2))

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:  # pragma: no cover

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump_json(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


class AcquireStageSummary(BaseSummary):
    """Summary information for acquire stage."""

    qgraph_filename: str | None = None
    """Filename for QuantumGraph."""
    number_quanta: int = 0
    """Total number of quanta."""
    quanta_counts: Counter[str] | None = None
    """Counts of quanta per PipelineTask label."""
    details: QgraphSummary | None = None
    """Summary of QuantumGraph generated or read."""
    details_type: str = "QgraphSummary"
    """Type of details to be used for validation."""
    generation_output: str | None = None
    """Filename for output of QuantumGraph generation if run."""


class ClusterStageSummary(BaseSummary):
    """Summary information for cluster stage."""

    clustered_qgraph_filename: str | None = None
    """Location of ClusteredQuantumGraph file if saved."""
    number_clusters: int = 0
    """Total number of clusters."""
    cluster_counts: Counter[str] | None = None
    """Counts of clusters per cluster label."""
    details: ClusteredQgraphSummary | None = None
    """Detailed summary of ClusteredQuantumGraph."""
    details_type: str = "ClusteredQgraphSummary"
    """Type of details to be used for validation."""


class TransformStageSummary(BaseSummary):
    """Summary information for transform stage."""

    generic_workflow_filename: str | None = None
    """Location of GenericWorkflow file if saved."""
    number_jobs: int = 0
    """Total number of jobs."""
    job_counts: Counter[str] | None = None
    """Counts of jobs per job label."""


class PrepareStageSummary(BaseSummary):
    """Summary information for prepare stage."""

    wms_service: str | None = None
    """Qualified name of the WMS service class."""


class SubmitStageSummary(BaseSummary):
    """Summary information for submit stage."""

    wms_service: str | None = None
    """Qualified name of the WMS service class."""
    wms_id: str | None = None
    """ID returned from the WMS when job submitted."""


class BpsSummary(BaseSummary):
    """Information generated by bps submit command."""

    name: str | None = None
    """Unique processing name."""
    cmd_line: list[str] = pydantic.Field(default_factory=lambda: sys.argv)
    """Command line for the submission."""
    submit_path: str | None = None
    """Directory made for submission."""
    butler_config: str | None = None
    """Location of the butler config file."""
    output_run: str | None = None
    """Output run collection."""
    acquire_summary: AcquireStageSummary | None = None
    """Summary for the acquire stage."""
    cluster_summary: ClusterStageSummary | None = None
    """Summary for the cluster stage."""
    transform_summary: TransformStageSummary | None = None
    """Summary for the transform stage."""
    prepare_summary: PrepareStageSummary | None = None
    """Summary for the prepare stage."""
    submit_summary: SubmitStageSummary | None = None
    """Summary for the submit stage."""
    host: str = pydantic.Field(default_factory=socket.gethostname)
    """Machine on which bps command was executed."""
    exit_code: int = 0
    """Submission exit code."""
    exception_info: ExceptionInfo | None = None
    """Exception information if exception was raised."""
    user_copy: str | None = pydantic.Field(exclude=True, default=None)
    """Location for user copy of summary."""
    include_details: bool = False
    """Whether to generate and save details."""

    def set_exception(self, exception: Exception) -> None:
        """Update exception information from an exception object.

        Parameters
        ----------
        exception : `Exception`
            Exception to use to extract information from.
        """
        self.exception_info = ExceptionInfo.from_exception(exception)

    @classmethod
    def load(cls, filename: str, load_details: bool = False) -> "BpsSummary":
        """Load the summary from the specified file.

        Parameters
        ----------
        filename : `str`
            URI of file from which to load the main summary.
        load_details : `bool`, optional
            Whether to also load the details which grow in size with
            number of quanta or jobs.  Defaults to False. Location of
            details specified in main summary.
        """
        with open(filename) as fh:
            json_data = fh.read()
            cls = BpsSummary.model_validate_json(json_data)

        if load_details:
            cls.acquire_summary.load_details()
            cls.cluster_summary.load_details()
            cls.transform_summary.load_details()
            cls.prepare_summary.load_details()
            cls.submit_summary.load_details()
        return cls

    def save(self, filename: str, details_dir: str = None) -> None:
        """Save the summary to files.

        Parameters
        ----------
        filename : `str`
            URI of file where to save the main summary.

        details_dir : `str`, optional
            Directory where to save the details which grow in size
            with number of quanta or jobs.  Defaults to submit_path
            saved in summary.
        """
        if self.include_details:
            if not details_dir:
                details_dir = self.submit_path
            if self.acquire_summary:
                self.acquire_summary.save_details(details_dir)
            if self.cluster_summary:
                self.cluster_summary.save_details(details_dir)
            if self.transform_summary:
                self.transform_summary.save_details(details_dir)
            if self.prepare_summary:
                self.prepare_summary.save_details(details_dir)
            if self.submit_summary:
                self.submit_summary.save_details(details_dir)

        with open(filename, "w") as fh:
            fh.write(self.model_dump_json(exclude_none=True, indent=2))
