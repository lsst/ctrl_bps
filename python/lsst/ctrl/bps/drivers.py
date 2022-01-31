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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Driver functions for each subcommand.

Driver functions ensure that ensure all setup work is done before running
the subcommand method.
"""


__all__ = [
    "acquire_qgraph_driver",
    "cluster_qgraph_driver",
    "transform_driver",
    "prepare_driver",
    "submit_driver",
    "report_driver",
    "cancel_driver",
]


import getpass
import logging
import os
import re
import shutil
from collections import Iterable


from lsst.daf.butler.core.utils import time_this
from lsst.obs.base import Instrument
from lsst.utils import doImport

from . import BPS_SEARCH_ORDER, BpsConfig
from .pre_transform import acquire_quantum_graph, cluster_quanta
from .transform import transform
from .prepare import prepare
from .submit import submit
from .cancel import cancel
from .report import report

_LOG = logging.getLogger(__name__)


def _init_submission_driver(config_file, **kwargs):
    """Initialize runtime environment.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    config : `lsst.ctrl.bps.BpsConfig`
        Batch Processing Service configuration.
    """
    config = BpsConfig(config_file, BPS_SEARCH_ORDER)

    # Override config with command-line values
    # Handle diffs between pipetask argument names vs bps yaml
    translation = {"input": "inCollection",
                   "output_run": "outputRun",
                   "qgraph": "qgraphFile",
                   "pipeline": "pipelineYaml"}
    for key, value in kwargs.items():
        # Don't want to override config with None or empty string values.
        if value:
            # pipetask argument parser converts some values to list,
            # but bps will want string.
            if not isinstance(value, str) and isinstance(value, Iterable):
                value = ",".join(value)
            new_key = translation.get(key, re.sub(r"_(\S)", lambda match: match.group(1).upper(), key))
            config[f".bps_cmdline.{new_key}"] = value

    # Set some initial values
    config[".bps_defined.timestamp"] = Instrument.makeCollectionTimestamp()
    if "operator" not in config:
        config[".bps_defined.operator"] = getpass.getuser()

    if "outCollection" in config:
        raise KeyError("outCollection is deprecated.  Replace all outCollection references with outputRun.")

    if "outputRun" not in config:
        raise KeyError("Must specify the output run collection using outputRun")

    if "uniqProcName" not in config:
        config[".bps_defined.uniqProcName"] = config["outputRun"].replace("/", "_")

    if "submitPath" not in config:
        raise KeyError("Must specify the submit-side run directory using submitPath")

    # If requested, run WMS plugin checks early in submission process to
    # ensure WMS has what it will need for prepare() or submit().

    if kwargs.get("runWmsSubmissionChecks", False):
        found, wms_class = config.search("wmsServiceClass")
        if not found:
            raise KeyError("Missing wmsServiceClass in bps config.  Aborting.")

        # Check that can import wms service class.
        wms_service_class = doImport(wms_class)
        wms_service = wms_service_class(config)

        try:
            wms_service.run_submission_checks()
        except NotImplementedError:
            # Allow various plugins to implement only when needed to do extra
            # checks.
            _LOG.debug("run_submission_checks is not implemented in %s.", wms_class)
    else:
        _LOG.debug("Skipping submission checks.")

    # make submit directory to contain all outputs
    submit_path = config["submitPath"]
    os.makedirs(submit_path, exist_ok=True)
    config[".bps_defined.submitPath"] = submit_path

    # save copy of configs (orig and expanded config)
    shutil.copy2(config_file, submit_path)
    with open(f"{submit_path}/{config['uniqProcName']}_config.yaml", "w") as fh:
        config.dump(fh)

    return config


def acquire_qgraph_driver(config_file, **kwargs):
    """Read a quantum graph from a file or create one from pipeline definition.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    config : `lsst.ctrl.bps.BpsConfig`
        Updated configuration.
    qgraph : `lsst.pipe.base.graph.QuantumGraph`
        A graph representing quanta.
    """
    config = _init_submission_driver(config_file, **kwargs)
    submit_path = config[".bps_defined.submitPath"]

    _LOG.info("Starting acquire stage (generating and/or reading quantum graph)")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Acquire stage completed"):
        qgraph_file, qgraph, execution_butler_dir = acquire_quantum_graph(config, out_prefix=submit_path)

    config[".bps_defined.executionButlerDir"] = execution_butler_dir
    config[".bps_defined.runQgraphFile"] = qgraph_file
    return config, qgraph


def cluster_qgraph_driver(config_file, **kwargs):
    """Group quanta into clusters.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    config : `lsst.ctrl.bps.BpsConfig`
        Updated configuration.
    clustered_qgraph : `lsst.ctrl.bps.ClusteredQuantumGraph`
        A graph representing clustered quanta.
    """
    config, qgraph = acquire_qgraph_driver(config_file, **kwargs)

    _LOG.info("Starting cluster stage (grouping quanta into jobs)")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Cluster stage completed"):
        clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])

    submit_path = config[".bps_defined.submitPath"]
    _, save_clustered_qgraph = config.search("saveClusteredQgraph", opt={"default": False})
    if save_clustered_qgraph:
        clustered_qgraph.save(os.path.join(submit_path, "bps_clustered_qgraph.pickle"))
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        clustered_qgraph.draw(os.path.join(submit_path, "bps_clustered_qgraph.dot"))
    return config, clustered_qgraph


def transform_driver(config_file, **kwargs):
    """Create a workflow for a specific workflow management system.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    generic_workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    """
    config, clustered_qgraph = cluster_qgraph_driver(config_file, **kwargs)
    submit_path = config[".bps_defined.submitPath"]

    _LOG.info("Starting transform stage (creating generic workflow)")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Transform stage completed"):
        generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
        _LOG.info("Generic workflow name '%s'", generic_workflow.name)

    _, save_workflow = config.search("saveGenericWorkflow", opt={"default": False})
    if save_workflow:
        with open(os.path.join(submit_path, "bps_generic_workflow.pickle"), "wb") as outfh:
            generic_workflow.save(outfh, "pickle")
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        with open(os.path.join(submit_path, "bps_generic_workflow.dot"), "w") as outfh:
            generic_workflow.draw(outfh, "dot")
    return generic_workflow_config, generic_workflow


def prepare_driver(config_file, **kwargs):
    """Create a representation of the generic workflow.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    wms_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    """
    kwargs.setdefault("runWmsSubmissionChecks", True)
    generic_workflow_config, generic_workflow = transform_driver(config_file, **kwargs)
    submit_path = generic_workflow_config[".bps_defined.submitPath"]

    _LOG.info("Starting prepare stage (creating specific implementation of workflow)")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Prepare stage completed"):
        wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)

    wms_workflow_config = generic_workflow_config
    print(f"Submit dir: {wms_workflow.submit_path}")
    return wms_workflow_config, wms_workflow


def submit_driver(config_file, **kwargs):
    """Submit workflow for execution.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.
    """
    kwargs.setdefault("runWmsSubmissionChecks", True)

    _LOG.info("Starting submission process")
    with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed entire submission process"):
        wms_workflow_config, wms_workflow = prepare_driver(config_file, **kwargs)

        _LOG.info("Starting submit stage")
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed submit stage"):
            submit(wms_workflow_config, wms_workflow)
            _LOG.info("Run '%s' submitted for execution with id '%s'", wms_workflow.name, wms_workflow.run_id)

    print(f"Run Id: {wms_workflow.run_id}")


def report_driver(wms_service, run_id, user, hist_days, pass_thru):
    """Print out summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service : `str`
        Name of the class.
    run_id : `str`
        A run id the report will be restricted to.
    user : `str`
        A user name the report will be restricted to.
    hist_days : int
        Number of days
    pass_thru : `str`
        A string to pass directly to the WMS service class.
    """
    report(wms_service, run_id, user, hist_days, pass_thru)


def cancel_driver(wms_service, run_id, user, require_bps, pass_thru):
    """Cancel submitted workflows.

    Parameters
    ----------
    wms_service : `str`
        Name of the Workload Management System service class.
    run_id : `str`
        ID or path of job that should be canceled.
    user : `str`
        User whose submitted jobs should be canceled.
    require_bps : `bool`
        Whether to require given run_id/user to be a bps submitted job.
    pass_thru : `str`
        Information to pass through to WMS.
    """
    cancel(wms_service, run_id, user, require_bps, pass_thru)
