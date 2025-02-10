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

"""Driver to execute steps outside of BPS that need to be done first
including running QuantumGraph generation and reading the QuantumGraph
into memory.
"""

import logging
import os
import shlex
import shutil
import subprocess
from pathlib import Path

from lsst.ctrl.bps import BpsConfig
from lsst.pipe.base.graph import QuantumGraph
from lsst.utils import doImport
from lsst.utils.logging import VERBOSE
from lsst.utils.timer import time_this, timeMethod

_LOG = logging.getLogger(__name__)


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def acquire_quantum_graph(config: BpsConfig, out_prefix: str = "") -> tuple[str, QuantumGraph]:
    """Read a quantum graph from a file or create one from scratch.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration values for BPS.  In particular, looking for qgraphFile.
    out_prefix : `str`, optional
        Output path for the QuantumGraph and stdout/stderr from generating
        the QuantumGraph.  Default value is empty string.

    Returns
    -------
    qgraph_filename : `str`
        Name of file containing QuantumGraph that was read into qgraph.
    qgraph : `lsst.pipe.base.graph.QuantumGraph`
        A QuantumGraph read in from pre-generated file or one that is the
        result of running code that generates it.
    """
    # Check to see if user provided pre-generated QuantumGraph.
    found, input_qgraph_filename = config.search("qgraphFile")
    if found and input_qgraph_filename:
        if out_prefix is not None:
            # Save a copy of the QuantumGraph file in out_prefix.
            _LOG.info("Copying quantum graph from '%s'", input_qgraph_filename)
            with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed copying quantum graph"):
                qgraph_filename = os.path.join(out_prefix, os.path.basename(input_qgraph_filename))
                shutil.copy2(input_qgraph_filename, qgraph_filename)
        else:
            # Use QuantumGraph file in original given location.
            qgraph_filename = input_qgraph_filename

        # Update the output run in the user provided quantum graph.
        if "finalJob" in config:
            update_quantum_graph(config, qgraph_filename, out_prefix)
    else:
        # Run command to create the QuantumGraph.
        _LOG.info("Creating quantum graph")
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed creating quantum graph"):
            qgraph_filename = create_quantum_graph(config, out_prefix)

    _LOG.info("Reading quantum graph from '%s'", qgraph_filename)
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed reading quantum graph"):
        qgraph = QuantumGraph.loadUri(qgraph_filename)

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
            shlex.split(command), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        while process.poll is None or buffer:
            print(buffer, end="", file=fh)
            _LOG.info(buffer)
            buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        process.stdout.close()
        process.wait()
    return process.returncode


def create_quantum_graph(config, out_prefix=""):
    """Create QuantumGraph from pipeline definition.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    out_prefix : `str`, optional
        Path in which to output QuantumGraph as well as the stdout/stderr
        from generating the QuantumGraph.  Defaults to empty string so
        code will write the QuantumGraph and stdout/stderr to the current
        directory.

    Returns
    -------
    qgraph_filename : `str`
        Name of file containing generated QuantumGraph.
    """
    # Create name of file to store QuantumGraph.
    qgraph_filename = os.path.join(out_prefix, config["qgraphFileTemplate"])

    # Get QuantumGraph generation command.
    search_opt = {"curvals": {"qgraphFile": qgraph_filename}}
    found, cmd = config.search("createQuantumGraph", opt=search_opt)
    if not found:
        _LOG.error("command for generating QuantumGraph not found")
    _LOG.info(cmd)

    # Run QuantumGraph generation.
    out = os.path.join(out_prefix, "quantumGraphGeneration.out")
    status = execute(cmd, out)
    if status != 0:
        raise RuntimeError(
            f"QuantumGraph generation exited with non-zero exit code ({status})\n"
            f"Check {out} for more details."
        )
    return qgraph_filename


def update_quantum_graph(config, qgraph_filename, out_prefix="", inplace=False):
    """Update output run in an existing quantum graph.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.
    qgraph_filename : `str`
        Name of file containing the quantum graph that needs to be updated.
    out_prefix : `str`, optional
        Path in which to output QuantumGraph as well as the stdout/stderr
        from generating the QuantumGraph.  Defaults to empty string so
        code will write the QuantumGraph and stdout/stderr to the current
        directory.
    inplace : `bool`, optional
        If set to True, all updates of the graph will be done in place without
        creating a backup copy. Defaults to False.
    """
    src_qgraph = Path(qgraph_filename)
    dest_qgraph = Path(qgraph_filename)

    # If requested, create a backup copy of the quantum graph by adding
    # '_orig' suffix to its stem (the filename without the extension).
    if not inplace:
        _LOG.info("Backing up quantum graph from '%s'", qgraph_filename)
        src_qgraph = src_qgraph.parent / f"{src_qgraph.stem}_orig{src_qgraph.suffix}"
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed backing up quantum graph"):
            shutil.copy2(qgraph_filename, src_qgraph)

    # Get the command for updating the quantum graph.
    search_opt = {"curvals": {"inputQgraphFile": str(src_qgraph), "qgraphFile": str(dest_qgraph)}}
    found, cmd = config.search("updateQuantumGraph", opt=search_opt)
    if not found:
        _LOG.error("command for updating quantum graph not found")
    _LOG.info(cmd)

    # Run the command to update the quantum graph.
    out = os.path.join(out_prefix, "quantumGraphUpdate.out")
    status = execute(cmd, out)
    if status != 0:
        raise RuntimeError(
            f"Updating quantum graph failed with non-zero exit code ({status})\nCheck {out} for more details."
        )


def cluster_quanta(config, qgraph, name):
    """Call specified function to group quanta into clusters to be run
    together.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    qgraph : `lsst.pipe.base.QuantumGraph`
        Original full QuantumGraph for the run.
    name : `str`
        Name for the ClusteredQuantumGraph that will be generated.

    Returns
    -------
    cqgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        Generated ClusteredQuantumGraph.

    Raises
    ------
    RuntimeError
        If asked to validate and generated ClusteredQuantumGraph fails a test.
    """
    cluster_func = doImport(config["clusterAlgorithm"])
    cqgraph = cluster_func(config, qgraph, name)
    _, validate = config.search("validateClusteredQgraph", opt={"default": False})
    if validate:
        cqgraph.validate()
    return cqgraph
