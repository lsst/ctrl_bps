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

import sys
from collections import Counter
from typing import Any

import pydantic


class JobLabelSummary(pydantic.BaseModel):
    """Summary for a job label in a GenericWorkflow."""

    label: str
    """Label for 1 or more jobs."""

    dimensions: list[str] | None = None
    equal_dimensions: list[tuple] | None = None
    quanta_counts: Counter
    """Number of Quanta per PipelineTask label."""
    tags: dict[str, str] | None = None

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


class GenericWorkflowSummary(pydantic.BaseModel):
    """Summary for a GenericWorkflow."""

    creation_UTC: str | None = None
    """Time of creation."""

    cluster_summaries: dict[str, JobLabelSummary] = {}
    """Information summarized per cluster."""

    path: str | None = None
    """Location of GenericWorkflow if saved."""

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

    @classmethod
    def load(cls, filename: str) -> "GenericWorkflowSummary":
        """Load the GenericWorkflow summary from the specified file.

        Parameters
        ----------
        filename : `str`
            URI of file from which to load the GenericWorkflow summary.
        """
        with open(filename) as fh:
            json_data = fh.read()
        cls = pydantic.TypeAdapter("GenericWorkflowSummary").validate_python(json_data)
        return cls

    def save(self, filename: str) -> None:
        """Save the summary to a file.

        Parameters
        ----------
        filename : `str`
            URI of file where to save the GenericWorkflow summary.
        """
        with open(filename, "w") as fh:
            fh.write(self.model_dump_json(exclude_none=True, indent=2))
