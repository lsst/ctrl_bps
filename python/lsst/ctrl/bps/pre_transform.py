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
import os
import shlex
import shutil
import subprocess

from lsst.ctrl.bps.bps_utils import _create_execution_butler
from lsst.daf.butler import Butler
from lsst.pipe.base.graph import QuantumGraph
from lsst.utils import doImport
from lsst.utils.logging import VERBOSE
from lsst.utils.timer import time_this, timeMethod

_LOG = logging.getLogger(__name__)


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def acquire_quantum_graph(config, out_prefix=""):
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
    execution_butler_dir : `str` or None
        The directory containing the execution butler if user-provided or
        created during this submission step.
    """
    # consistently name execution butler directory
    _, execution_butler_dir = config.search("executionButlerTemplate")
    if not execution_butler_dir.startswith("/"):
        execution_butler_dir = os.path.join(config["submitPath"], execution_butler_dir)
    _, when_create = config.search(".executionButler.whenCreate")

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

        # Copy Execution Butler if user provided (shouldn't provide execution
        # butler if not providing QuantumGraph)
        if when_create.upper() == "USER_PROVIDED":
            found, user_exec_butler_dir = config.search(".executionButler.executionButlerDir")
            if not found:
                raise KeyError("Missing .executionButler.executionButlerDir for when_create == USER_PROVIDED")

            # Save a copy of the execution butler file in out_prefix.
            _LOG.info("Copying execution butler to '%s'", user_exec_butler_dir)
            with time_this(
                log=_LOG, level=logging.INFO, prefix=None, msg="Completed copying execution butler"
            ):
                shutil.copytree(user_exec_butler_dir, execution_butler_dir)
    else:
        if when_create.upper() == "USER_PROVIDED":
            raise KeyError("Missing qgraphFile to go with provided executionButlerDir")

        # Run command to create the QuantumGraph.
        _LOG.info("Creating quantum graph")
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed creating quantum graph"):
            qgraph_filename = create_quantum_graph(config, out_prefix)

    _LOG.info("Reading quantum graph from '%s'", qgraph_filename)
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed reading quantum graph"):
        qgraph = read_quantum_graph(qgraph_filename, config["butlerConfig"])

    if when_create.upper() == "QGRAPH_CMDLINE":
        if not os.path.exists(execution_butler_dir):
            raise OSError(
                f"Missing execution butler dir ({execution_butler_dir}) after "
                "creating QuantumGraph (whenMakeExecutionButler == QGRAPH_CMDLINE"
            )
    elif when_create.upper() == "ACQUIRE":
        _create_execution_butler(config, qgraph_filename, execution_butler_dir, config["submitPath"])

    return qgraph_filename, qgraph, execution_butler_dir


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


def read_quantum_graph(qgraph_filename, butler_uri):
    """Read the QuantumGraph from disk.

    Parameters
    ----------
    qgraph_filename : `str`
        Name of file containing QuantumGraph to be used for workflow
        generation.
    butler_uri : `str`
        Location of butler repository that can be used to create a
        butler object.

    Returns
    -------
    qgraph : `lsst.pipe.base.graph.QuantumGraph`
        The QuantumGraph read from a file.

    Raises
    ------
    RuntimeError
        If the QuantumGraph contains 0 Quanta.
    """
    # Get the DimensionUniverse from the butler repository
    butler = Butler(butler_uri, writeable=False)
    qgraph = QuantumGraph.loadUri(qgraph_filename, butler.registry.dimensions)
    if len(qgraph) == 0:
        raise RuntimeError("QuantumGraph is empty")
    return qgraph


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
    graph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        Generated ClusteredQuantumGraph.
    """
    cluster_func = doImport(config["clusterAlgorithm"])
    return cluster_func(config, qgraph, name)
