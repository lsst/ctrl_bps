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

"""Driver to execute steps outside of BPS that need to be done first
including running QuantumGraph generation and reading the QuantumGraph
into memory.
"""

import logging
import subprocess
import os
import shlex
import shutil
import time

from lsst.daf.butler import DimensionUniverse
from lsst.pipe.base.graph import QuantumGraph
from lsst.utils import doImport

_LOG = logging.getLogger(__name__)


def pre_transform(config, out_prefix=None):
    """Steps outside of BPS that need to be done first including generating
    a QuantumGraph.

    Parameters
    ----------
    config : `.bps_config.BpsConfig`
        Configuration values for BPS.  In particular, looking for qgraphFile.
    out_prefix : `str` or None
        Output path for the QuantumGraph and stdout/stderr from generating
        the QuantumGraph.

    Returns
    -------
    qgraph_filename : `str`
        Name of file containing QuantumGraph that was read into qgraph.
    qgraph : `~lsst.pipe.base.graph.QuantumGraph`
        A QuantumGraph read in from pre-generated file or one that is the
        result of running code that generates it.
    """
    # Check to see if user provided pre-generated QuantumGraph.
    found, input_qgraph_filename = config.search("qgraphFile")
    if found:
        if out_prefix is not None:
            # Save a copy of the QuantumGraph file in out_prefix.
            _LOG.info("Copying quantum graph (%s)", input_qgraph_filename)
            stime = time.time()
            qgraph_filename = os.path.join(out_prefix, os.path.basename(input_qgraph_filename))
            shutil.copy2(input_qgraph_filename, qgraph_filename)
            _LOG.info("Copying quantum graph took %.2f seconds", time.time() - stime)
        else:
            # Use QuantumGraph file in original given location.
            qgraph_filename = input_qgraph_filename
    else:
        # Run command to create the QuantumGraph.
        _LOG.info("Creating quantum graph")
        stime = time.time()
        qgraph_filename = create_quantum_graph(config, out_prefix)
        _LOG.info("Creating quantum graph took %.2f seconds", time.time() - stime)

    _LOG.info("Reading quantum graph (%s)", qgraph_filename)
    stime = time.time()
    qgraph = read_quantum_graph(qgraph_filename)
    _LOG.info("Reading quantum graph with %d nodes took %.2f seconds", len(qgraph),
              time.time() - stime)

    return qgraph_filename, qgraph


def execute(command, filename):
    """Execute a command.

    Parameters
    ----------
    command : `str`
        String representing the command to execute.
    filename : `str`
        A file to which both stderr and stdout will be written to.

    Returns
    -------
    exit_code : `int`
        The exit code the command being executed finished with.
    """
    buffer_size = 5000
    with open(filename, "w") as fh:
        print(command, file=fh)
        print("\n", file=fh)  # Note: want a blank line
        process = subprocess.Popen(
            shlex.split(command), shell=False, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        while process.poll is None or buffer:
            print(buffer, end="", file=fh)
            _LOG.info(buffer)
            buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        process.stdout.close()
        process.wait()
    return process.returncode


def create_quantum_graph(config, out_prefix=None):
    """Create QuantumGraph from pipeline definition

    Parameters
    ----------
    config : `.bps_config.BpsConfig`
        BPS configuration.
    out_prefix : `str` or None
        Path in which to output QuantumGraph as well as the stdout/stderr
        from generating the QuantumGraph.  If out_prefix is None, code will write
        the QuantumGraph and stdout/stderr to the current directory.

    Returns
    -------
    qgraph_filename : `str`
        Name of file containing generated QuantumGraph
    """
    # Create name of file to store QuantumGraph.
    qgraph_filename = f"{config['uniqProcName']}.qgraph"
    if out_prefix is not None:
        qgraph_filename = os.path.join(out_prefix, qgraph_filename)

    # Get QuantumGraph generation command.
    search_opt = {"curvals": {"qgraphFile": qgraph_filename}}
    found, cmd = config.search("createQuantumGraph", opt=search_opt)
    if not found:
        _LOG.error("command for generating QuantumGraph not found")
    _LOG.info(cmd)

    # Run QuantumGraph generation.
    out = "quantumGraphGeneration.out"
    if out_prefix is not None:
        out = os.path.join(out_prefix, out)
    status = execute(cmd, out)
    if status != 0:
        raise RuntimeError(f"QuantumGraph generation exited with non-zero exit code ({status})\n"
                           f"Check {out} for more details.")
    return qgraph_filename


def read_quantum_graph(qgraph_filename):
    """Read the QuantumGraph from disk.

    Parameters
    ----------
    qgraph_filename : `str`
        Name of file containing QuantumGraph to be used for workflow generation.

    Returns
    -------
    qgraph : `~lsst.pipe.base.graph.QuantumGraph`
        The QuantumGraph read from a file.

    Raises
    ------
    `RuntimeError`
        If the QuantumGraph contains 0 Quanta
    """
    qgraph = QuantumGraph.loadUri(qgraph_filename, DimensionUniverse())
    if len(qgraph) == 0:
        raise RuntimeError("QuantumGraph is empty")
    return qgraph


def cluster_quanta(config, qgraph, name):
    """Call specified function to group quanta into clusters to be run together.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration.
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Original full QuantumGraph for the run.
    name : `str`
        Name for the ClusteredQuantumGraph that will be generated.

    Returns
    -------
    graph : `~lsst.ctrl.bps.clustered_quantum_graph.ClusteredQuantumGraph`
        Generated ClusteredQuantumGraph.
    """
    cluster_func = doImport(config["clusterAlgorithm"])
    return cluster_func(config, qgraph, name)
