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

"""Driver for execution inside a compute job.
"""
import logging
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

from lsst.resources import ResourcePath
from lsst.utils.timer import time_this

_LOG = logging.getLogger(__name__)


def copy_data(dest_path, src_path, is_dir=False):
    """Copy data recursively from one location to another.

    Parameters
    ----------
    src_path : `str`
        What to copy (may be file or directory)
    dest_path : `str`
        Where to copy src data (must be directory)
    is_dir : `bool`
        Whether src_path is directory

    Returns
    -------
    dest_uri : `str` or None
        Location of copy (is directory if src_path was directory).
        Returns None if no files found in src_path to copy.
    """
    src = ResourcePath(src_path, forceDirectory=is_dir)
    _LOG.debug("src = %s", src)
    _LOG.debug("dest_path = %s", dest_path)
    if is_dir:
        files_to_copy = ResourcePath.findFileResources([src])
        if not files_to_copy:
            _LOG.warning("Found 0 files to copy in %s", str(src))
            return None
    else:
        files_to_copy = [src]

    dest_base = ResourcePath(dest_path, forceAbsolute=True, forceDirectory=True)
    for file_ in files_to_copy:
        rel_file = file_.relative_to(src.parent())
        dest = dest_base.join(rel_file)
        dest.transfer_from(file_, transfer="copy")
        print(f"copied {file_.path} " f"to {dest.path}", file=sys.stderr)

    if is_dir:
        # ResourcePath.basename returns None if ends in slash
        dest_uri = dest_base.join(src.dirname().relative_to(src.parent()))
    else:
        dest_uri = dest_base.join(src.basename())
    _LOG.debug("dest_uri = %s", dest_uri)

    return dest_uri


def run_job(job_name, job):
    """Control executions inside a job.

    Parameters
    ----------
    job_name : `str`
        Name of job to be executed.
    job : `lsst.ctrl.bps.job_config.JobConfigChunk`
        Name of job to be executed.
    """
    _LOG.debug("job_name = %s", job_name)

    # Stage input files
    _LOG.info("Staging input files")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Staging input files completed"):
        for input_ in job.inputs:
            _LOG.debug("Working on input %s", input_)
            file_ = job.files[input_]
            if file_.transfer:
                if not file_.job_uri:
                    file_.job_uri = copy_data(Path.cwd(), file_.external_uri, file_.is_dir)
            else:
                file_.job_uri = file_.external_uri

    # Stage executable if needed
    job_exec = job.cmd.executable
    if job.cmd.transfer:
        job_exec = ResourcePath(job.cmd.executable)
        dest = ResourcePath(job_exec.basename())
        dest.transfer_from(job_exec, transfer="copy")
        _LOG.debug("copied executable %s to %s", job_exec, dest.path)
        job_exec = dest.path
    else:
        for key in re.findall(r"<ENV:([^>]+)>", job_exec):
            job_exec = job_exec.replace(rf"<ENV:{key}>", os.environ[key])

    # Create command line
    arguments = ""
    if job.cmd.arguments:
        arguments = job.cmd.arguments.format(**job.cmd.cmdvals)
        for name in re.findall(r"<FILE:([^>]+)>", arguments):
            print(f"file name = {name}")
            print(job.files[name])
            arguments = arguments.replace(f"<FILE:{name}>", str(job.files[name].job_uri))
        for key in re.findall(r"<ENV:([^>]+)>", arguments):
            arguments = arguments.replace(rf"<ENV:{key}>", os.environ[key])
    command = f"{job_exec} {arguments}"

    # Run command
    _LOG.debug("Running command: %s", command)
    subprocess.run(shlex.split(command), shell=False, check=True)

    # Stage output files

    # Write job report file
