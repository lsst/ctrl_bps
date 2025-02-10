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

"""Misc supporting classes and functions for BPS."""

__all__ = [
    "_dump_env_info",
    "_dump_pkg_info",
    "_make_id_link",
    "chdir",
    "create_count_summary",
    "create_job_quantum_graph_filename",
    "mkdir",
    "parse_count_summary",
    "save_qg_subgraph",
]

import contextlib
import dataclasses
import errno
import logging
import os
from collections import Counter
from enum import Enum
from pathlib import Path

import yaml

from lsst.utils.packages import Packages

_LOG = logging.getLogger(__name__)


class WhenToSaveQuantumGraphs(Enum):
    """Values for when to save the job quantum graphs."""

    QGRAPH = 1  # Must be using single_quantum_clustering algorithm.
    TRANSFORM = 2
    PREPARE = 3
    SUBMIT = 4
    NEVER = 5  # Always use full QuantumGraph.


@contextlib.contextmanager
def chdir(path):
    """Change working directory.

    A chdir function that can be used inside a context.

    Parameters
    ----------
    path : `str` or `pathlib.Path`
        Path to be made current working directory.
    """
    cur_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cur_dir)


def mkdir(path: str) -> Path:
    """Create a new directory at this given path.

    Parameters
    ----------
    path : `str`
        A string representing the path to create.

    Returns
    -------
    path: `pathlib.Path`
        The object representing the created directory.

    Raises
    ------
    OSError
        Raised if any issues were encountered during the attempt to create the
        directory. Depending on the system error code a specific subclass of
        ``OSError`` will get raised. For example, the function will raise
        ``PermissionError`` when trying to create a directory at the location
        it has no adequate access rights.
    """
    path = Path(path)
    try:
        path.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            reason = "directory already exists"
        else:
            reason = exc.strerror
        raise type(exc)(f"cannot create directory '{path}': {reason}") from None
    return path


def create_job_quantum_graph_filename(config, job, out_prefix=None):
    """Create a filename to be used when storing the QuantumGraph for a job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    job : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which the QuantumGraph file is being saved.
    out_prefix : `str`, optional
        Path prefix for the QuantumGraph filename.  If no out_prefix is given,
        uses current working directory.

    Returns
    -------
    full_filename : `str`
        The filename for the job's QuantumGraph.
    """
    curvals = dataclasses.asdict(job)
    if job.tags:
        curvals.update(job.tags)
    found, subdir = config.search("subDirTemplate", opt={"curvals": curvals})
    if not found:
        subdir = "{job.label}"
    full_filename = Path("inputs") / subdir / f"quantum_{job.name}.qgraph"

    if out_prefix is not None:
        full_filename = Path(out_prefix) / full_filename

    return str(full_filename)


def save_qg_subgraph(qgraph, out_filename, node_ids=None):
    """Save subgraph to file.

    Parameters
    ----------
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph to save.
    out_filename : `str`
        Name of the output file.
    node_ids : `list` [`lsst.pipe.base.NodeId`]
        NodeIds for the subgraph to save to file.
    """
    if not os.path.exists(out_filename):
        _LOG.debug("Saving QuantumGraph with %d nodes to %s", len(qgraph), out_filename)
        if node_ids is None:
            qgraph.saveUri(out_filename)
        else:
            qgraph.subset(qgraph.getQuantumNodeByNodeId(nid) for nid in node_ids).saveUri(out_filename)
    else:
        _LOG.debug("Skipping saving QuantumGraph to %s because already exists.", out_filename)


def create_count_summary(counts):
    """Create summary from count mapping.

    Parameters
    ----------
    counts : `collections.Counter` or `dict` [`str`, `int`]
        Mapping of counts to keys.

    Returns
    -------
    summary : `str`
        Semi-colon delimited string of key:count pairs.
        (e.g. "key1:cnt1;key2;cnt2")  Parsable by
        parse_count_summary().
    """
    summary = ""
    if isinstance(counts, dict):
        summary = ";".join([f"{key}:{counts[key]}" for key in counts])
    return summary


def parse_count_summary(summary):
    """Parse summary into count mapping.

    Parameters
    ----------
    summary : `str`
        Semi-colon delimited string of key:count pairs.

    Returns
    -------
    counts : `collections.Counter`
        Mapping representation of given summary for easier
        individual count lookup.
    """
    counts = Counter()
    for part in summary.split(";"):
        label, count = part.split(":")
        counts[label] = count
    return counts


def _dump_pkg_info(filename):
    """Save information about versions of packages in use for future reference.

    Parameters
    ----------
    filename : `str`
        The name of the file where to save the information about the versions
        of the packages.
    """
    file = Path(filename)
    if file.suffix.lower() not in {".yaml", ".yml"}:
        file = file.with_suffix(f"{file.suffix}.yaml")
    packages = Packages.fromSystem()
    packages.write(str(file))


def _dump_env_info(filename):
    """Save information about runtime environment for future reference.

    Parameters
    ----------
    filename : `str`
        The name of the file where to save the information about the runtime
        environment.
    """
    file = Path(filename)
    if file.suffix.lower() not in {".yaml", ".yml"}:
        file = file.with_suffix(f"{file.suffix}.yaml")
    with open(file, "w", encoding="utf-8") as fh:
        yaml.dump(dict(os.environ), fh)


def _make_id_link(config, run_id):
    """Make id softlink to the submit run directory if makeIdLink
    is true.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    run_id : `str`
        WMS run ID.
    """
    _, make_id_link = config.search("makeIdLink")
    if make_id_link:
        if run_id is None:
            _LOG.info("Run ID is None.  Skipping making id link.")
        else:
            found, submit_path = config.search("submitPath")
            # pathlib.Path.symlink_to() does not care if target exists
            # so we check it ourselves.
            if found and Path(submit_path).exists():
                _, id_link_path = config.search("idLinkPath")
                _LOG.debug("submit_path=%s, id_link_path=%s", submit_path, id_link_path)
                id_link_path = Path(id_link_path)
                id_link_path = id_link_path / f"{run_id}"
                _LOG.debug("submit_path=%s, id_link_path=%s", submit_path, id_link_path)
                if (
                    id_link_path.exists()
                    and id_link_path.is_symlink()
                    and str(id_link_path.readlink()) == submit_path
                ):
                    _LOG.debug("Correct softlink already exists (%s)", id_link_path)
                else:
                    _LOG.debug("Softlink doesn't already exist (%s)", id_link_path)
                    try:
                        id_link_path.parent.mkdir(parents=True, exist_ok=True)
                        id_link_path.symlink_to(submit_path)
                        _LOG.info("Made id softlink: %s", id_link_path)
                    except (OSError, FileExistsError, PermissionError) as exc:
                        _LOG.warning("Could not make id softlink: %s", exc)
            else:
                _LOG.warning("Could not make id softlink: submitPath does not exist (%s)", submit_path)
    else:
        _LOG.debug("Not asked to make id link (makeIdLink=%s)", make_id_link)
