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

"""Misc supporting classes and functions for BPS.
"""

import dataclasses
import os
import shlex
import subprocess
import contextlib
import logging
from pathlib import Path
from enum import Enum


_LOG = logging.getLogger(__name__)


class WhenToSaveQuantumGraphs(Enum):
    """Values for when to save the job quantum graphs.
    """
    QGRAPH = 1   # Must be using single_quantum_clustering algorithm.
    TRANSFORM = 2
    PREPARE = 3
    SUBMIT = 4
    NEVER = 5    # Always use full QuantumGraph.


@contextlib.contextmanager
def chdir(path):
    """A chdir function that can be used inside a context.

    Parameters
    ----------
    path : `str`
        Path to be made current working directory
    """
    cur_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cur_dir)


def create_job_quantum_graph_filename(config, job, out_prefix=None):
    """Create a filename to be used when storing the QuantumGraph
    for a job.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration (at minimum must contain qgraphFile and
        outCollection).
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
    found, subdir = config.search("subDirTemplate", opt={'curvals': curvals})
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


def _create_execution_butler(config, qgraph_filename, execution_butler_dir, out_prefix):
    """Create the execution butler for use by the compute jobs.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration (at minimum must contain qgraphFile and
        outCollection).
    qgraph_filename : `str`
        Run QuantumGraph filename.
    execution_butler_dir : `str`
        Directory in which to create the execution butler.
    out_prefix : `str` or None
        Prefix for output filename to contain both stdout and stderr.

    Raises
    ------
    CalledProcessError
        Raised if command to create execution butler exits with non-zero
        exit code.
    """
    _, command = config.search(".executionButler.createCommand",
                               opt={"curvals": {"executionButlerDir": execution_butler_dir,
                                                "qgraphFile": qgraph_filename},
                                    "replaceVars": True})
    out_filename = "execution_butler_creation.out"
    if out_prefix is not None:
        out_filename = os.path.join(out_prefix, out_filename)
    with open(out_filename, "w") as fh:
        print(command, file=fh)
        print("\n", file=fh)  # Note: want a blank line
        subprocess.run(shlex.split(command), shell=False, check=True, stdout=fh, stderr=subprocess.STDOUT)
