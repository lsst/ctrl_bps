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
    "restart_driver",
    "cancel_driver",
    "ping_driver",
]


import errno
import getpass
import logging
import os
import re
import shutil
from collections.abc import Iterable
from pathlib import Path

from lsst.pipe.base import Instrument
from lsst.utils import doImport
from lsst.utils.timer import time_this
from lsst.utils.usage import get_peak_mem_usage

from . import BPS_DEFAULTS, BPS_SEARCH_ORDER, DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT, BpsConfig
from .bps_utils import _dump_env_info, _dump_pkg_info
from .cancel import cancel
from .ping import ping
from .pre_transform import acquire_quantum_graph, cluster_quanta
from .prepare import prepare
from .report import report
from .restart import restart
from .submit import submit
from .transform import transform

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

    # Override config with command-line values.
    # Handle diffs between pipetask argument names vs bps yaml
    translation = {
        "input": "inCollection",
        "output_run": "outputRun",
        "qgraph": "qgraphFile",
        "pipeline": "pipelineYaml",
        "wms_service": "wmsServiceClass",
        "compute_site": "computeSite",
    }
    for key, value in kwargs.items():
        # Don't want to override config with None or empty string values.
        if value:
            # pipetask argument parser converts some values to list,
            # but bps will want string.
            if not isinstance(value, str) and isinstance(value, Iterable):
                value = ",".join(value)
            new_key = translation.get(key, re.sub(r"_(\S)", lambda match: match.group(1).upper(), key))
            config[f".bps_cmdline.{new_key}"] = value

    # If the WMS service class was not defined neither at the command line nor
    # explicitly in config file, use the value provided by the environmental
    # variable BPS_WMS_SERVICE_CLASS.  If the variable is not set, stick to
    # the package default.
    wms_service = os.environ.get("BPS_WMS_SERVICE_CLASS", None)
    if wms_service is not None and "wmsServiceClass" not in config[".bps_cmdline"]:
        default_config = BpsConfig(BPS_DEFAULTS)
        if config["wmsServiceClass"] == default_config["wmsServiceClass"]:
            config["wmsServiceClass"] = wms_service

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

    # Make submit directory to contain all outputs.
    submit_path = Path(config["submitPath"])
    try:
        submit_path.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            reason = "Directory already exists"
        else:
            reason = exc.strerror
        raise type(exc)(f"cannot create submit directory '{submit_path}': {reason}") from None
    config[".bps_defined.submitPath"] = str(submit_path)
    print(f"Submit dir: {submit_path}")

    # save copy of configs (orig and expanded config)
    shutil.copy2(config_file, submit_path)
    with open(f"{submit_path}/{config['uniqProcName']}_config.yaml", "w") as fh:
        config.dump(fh)

    # Dump information about runtime environment and software versions in use.
    _dump_env_info(f"{submit_path}/{config['uniqProcName']}.env.info.yaml")
    _dump_pkg_info(f"{submit_path}/{config['uniqProcName']}.pkg.info.yaml")

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
    _LOG.info("Initializing execution environment")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Initializing execution environment completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        config = _init_submission_driver(config_file, **kwargs)
        submit_path = config[".bps_defined.submitPath"]
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )

    _LOG.info("Starting acquire stage (generating and/or reading quantum graph)")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Acquire stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        qgraph_file, qgraph, execution_butler_dir = acquire_quantum_graph(config, out_prefix=submit_path)
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )

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
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Cluster stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )
    _LOG.info("ClusteredQuantumGraph contains %d cluster(s)", len(clustered_qgraph))

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
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Transform stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
        _LOG.info("Generic workflow name '%s'", generic_workflow.name)
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )
    num_jobs = sum(generic_workflow.job_counts.values())
    _LOG.info("GenericWorkflow contains %d job(s) (including final)", num_jobs)

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
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Prepare stage completed",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )

    wms_workflow_config = generic_workflow_config
    return wms_workflow_config, wms_workflow


def submit_driver(config_file, **kwargs):
    """Submit workflow for execution.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.
    """
    kwargs.setdefault("runWmsSubmissionChecks", True)

    _LOG.info(
        "DISCLAIMER: All values regarding memory consumption reported below are approximate and may "
        "not accurately reflect actual memory usage by the bps process."
    )

    _LOG.info("Starting submission process")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Completed entire submission process",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_workflow_config, wms_workflow = prepare_driver(config_file, **kwargs)

        _LOG.info("Starting submit stage")
        with time_this(
            log=_LOG,
            level=logging.INFO,
            prefix=None,
            msg="Completed submit stage",
            mem_usage=True,
            mem_unit=DEFAULT_MEM_UNIT,
            mem_fmt=DEFAULT_MEM_FMT,
        ):
            submit(wms_workflow_config, wms_workflow)
            _LOG.info("Run '%s' submitted for execution with id '%s'", wms_workflow.name, wms_workflow.run_id)
    if _LOG.isEnabledFor(logging.INFO):
        _LOG.info(
            "Peak memory usage for bps process %s (main), %s (largest child process)",
            *tuple(f"{val.to(DEFAULT_MEM_UNIT):{DEFAULT_MEM_FMT}}" for val in get_peak_mem_usage()),
        )

    print(f"Run Id: {wms_workflow.run_id}")
    print(f"Run Name: {wms_workflow.name}")


def restart_driver(wms_service, run_id):
    """Restart a failed workflow.

    Parameters
    ----------
    wms_service : `str`
        Name of the class.
    run_id : `str`
        Id or path of workflow that need to be restarted.
    """
    if wms_service is None:
        default_config = BpsConfig(BPS_DEFAULTS)
        wms_service = os.environ.get("BPS_WMS_SERVICE_CLASS", default_config["wmsServiceClass"])

    new_run_id, run_name, message = restart(wms_service, run_id)
    if new_run_id is not None:
        path = Path(run_id)
        if path.exists():
            _dump_env_info(f"{run_id}/{run_name}.env.info.yaml")
            _dump_pkg_info(f"{run_id}/{run_name}.pkg.info.yaml")
        print(f"Run Id: {new_run_id}")
        print(f"Run Name: {run_name}")
    else:
        if message:
            print(f"Restart failed: {message}")
        else:
            print("Restart failed: Unknown error")


def report_driver(wms_service, run_id, user, hist_days, pass_thru, is_global=False):
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
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    if wms_service is None:
        default_config = BpsConfig(BPS_DEFAULTS)
        wms_service = os.environ.get("BPS_WMS_SERVICE_CLASS", default_config["wmsServiceClass"])
    report(wms_service, run_id, user, hist_days, pass_thru, is_global=is_global)


def cancel_driver(wms_service, run_id, user, require_bps, pass_thru, is_global=False):
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
    is_global : `bool`, optional
        If set, all available job queues will be checked for jobs to cancel.
        Defaults to False which means that only a local job queue will be
        checked.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    if wms_service is None:
        default_config = BpsConfig(BPS_DEFAULTS)
        wms_service = os.environ.get("BPS_WMS_SERVICE_CLASS", default_config["wmsServiceClass"])
    cancel(wms_service, run_id, user, require_bps, pass_thru, is_global=is_global)


def ping_driver(wms_service=None, pass_thru=None):
    """Checks whether WMS services are up, reachable, and any authentication,
    if needed, succeeds.

    The services to be checked are those needed for submit, report, cancel,
    restart, but ping cannot guarantee whether jobs would actually run
    successfully.

    Parameters
    ----------
    wms_service : `str`, optional
        Name of the Workload Management System service class.
    pass_thru : `str`, optional
        Information to pass through to WMS.

    Returns
    -------
    success : `int`
        Whether services are up and usable (0) or not (non-zero).
    """
    if wms_service is None:
        default_config = BpsConfig(BPS_DEFAULTS)
        wms_service = os.environ.get("BPS_WMS_SERVICE_CLASS", default_config["wmsServiceClass"])
    status, message = ping(wms_service, pass_thru)

    if message:
        if not status:
            _LOG.info(message)
        else:
            _LOG.error(message)

    # Log overall status message
    if not status:
        _LOG.info("Ping successful.")
    else:
        _LOG.error("Ping failed (%d).", status)

    return status
