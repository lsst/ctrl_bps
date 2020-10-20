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

"""Core functionality of BPS
"""

import os
import contextlib
import logging
from importlib import import_module
from enum import Enum

_LOG = logging.getLogger()


class WhenToSaveQuantumGraphs(Enum):
    """Values for when to save the job quantum graphs."""
    QGRAPH = 1   # Must be using single_quantum_clustering algorithm.
    TRANSFORM = 2
    PREPARE = 3
    SUBMIT = 4

def dynamically_load(name):
    """Import at runtime given name.

    Late import allows BPS to keep package dependencies minimal.
    Also allows users to use their own subclasses without requiring
    changes to BPS code.

    Parameters
    ----------
    name: `str`
        Name of class or function to load.  Must include full module path.

    Returns
    -------
    The specified class ready to be instantiated
    """
    mod_parts = name.split(".")
    from_name = ".".join(mod_parts[0:-1])
    import_name = mod_parts[-1]
    _LOG.info("%s %s", from_name, import_name)
    mod = import_module(from_name)
    return getattr(mod, import_name)


@contextlib.contextmanager
def chdir(path):
    cur_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cur_dir)


def create_job_quantum_graph_filename(_, job, out_prefix=None):
    name_parts = []
    if out_prefix is not None:
        name_parts.append(out_prefix)
    name_parts.append("inputs")
    if job.label is not None:
        name_parts.append(job.label)
    name_parts.append(f"quantum_{job.name}.pickle")
    full_filename = os.path.join(*name_parts)
    return full_filename


def save_qg_subgraph(qgraph, out_filename):
    """Save subgraph to file

    Parameters
    ----------
    qgraph : `~lsst.pipe.base.graph.QuantumGraph`
        QuantumGraph to save
    out_filename : `str`
        Name of the output file
    """
    if not os.path.exists(out_filename):
        _LOG.info("Saving QuantumGraph with %d nodes to %s", len(qgraph), out_filename)
        if len(os.path.dirname(out_filename)) > 0:
            os.makedirs(os.path.dirname(out_filename), exist_ok=True)
        with open(out_filename, "wb") as outfh:
            qgraph.save(outfh)
    else:
        _LOG.debug("Skipping saving QuantumGraph to %s because already exists.", out_filename)
