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

"""Configuration model for the config for a bps job.
"""

from pydantic import BaseModel, Field


class JobConfigExec(BaseModel):
    """Job executable"""

    executable: str
    arguments: str
    cmdvals: dict[str, str]
    transfer: bool


class JobConfigFile(BaseModel):
    """Job input/output file"""

    external_uri: str
    is_dir: bool
    job_uri: str | None
    transfer: bool


class JobConfigJob(BaseModel):
    """Job description"""

    cmd: JobConfigExec
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    files: dict[str, JobConfigFile] = Field(default_factory=dict)


class JobConfigChunk(BaseModel):
    """Group of jobs"""

    workflow_name: str
    filename: str
    jobs: dict[str, JobConfigJob] = Field(default_factory=dict)
