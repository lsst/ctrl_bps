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

"""Driver for submitting a prepared WMS-specific workflow
"""

import getpass
import logging
import os
import pickle
import time

from lsst.utils import doImport
from lsst.obs.base import Instrument

from .bps_draw import draw_networkx_dot
from .pre_transform import pre_transform, cluster_quanta
from .prepare import prepare
from .transform import transform


# Config section search order
BPS_SEARCH_ORDER = ["payload", "pipetask", "site", "bps_defined"]

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""

_LOG = logging.getLogger()


def submit(config, wms_workflow, wms_service=None):
    """Convert generic workflow to a workflow for a particular WMS.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        Configuration values to be used by submission.
    wms_workflow : `~lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        The workflow to submit.
    wms_service : `~lsst.ctrl.bps.wms_service.BaseWmsService`, optional
        The workflow management service to which the workflow should be submitted.

    Returns
    -------
    wms_workflow : `~lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        WMS-specific workflow
    """
    if wms_service is None:
        wms_service_class = doImport(config["wmsServiceClass"])
        wms_service = wms_service_class(config)
    return wms_service.submit(wms_workflow)


def init_runtime_env(config):
    """Initilize runtime environment.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.BpsConfig`
        Initial Batch Processing Service configuration.

    Returns
    -------
    config : `~lsst.ctrl.bps.BpsConfig`
        Updated Batch Processing Service configuration.
    """
    config[".bps_defined.timestamp"] = Instrument.makeCollectionTimestamp()
    if "uniqProcName" not in config:
        config[".bps_defined.uniqProcName"] = config["outCollection"].replace("/", "_")
    if "operator" not in config:
        config[".bps_defined.operator"] = getpass.getuser()

    # make submit directory to contain all outputs
    submit_path = config["submitPath"]
    os.makedirs(submit_path, exist_ok=True)
    config[".bps_defined.submit_path"] = submit_path
    return config


def create_clustered_qgraph(config):
    """Cluster quanta into groups.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.BpsConfig`
        Batch Processing service configuration.

    Returns
    -------
    config : `~lsst.ctrl.bps.BpsConfig`
        Updated configuration.
    clustered_qgraph : `~lsst.ctrl.bps.ClusteredQuantumGraph`
        A graph representing clustered quanta.
    """
    stime = time.time()
    submit_path = config[".bps_defined.submit_path"]
    _LOG.info("Pre-transform steps (includes QuantumGraph generation if needed)")
    qgraph_file, qgraph = pre_transform(config, out_prefix=submit_path)
    _LOG.info("Run QuantumGraph file %s", qgraph_file)
    config['.bps_defined.run_qgraph_file'] = qgraph_file
    clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])
    _LOG.info("Pre-transform steps took %.2f seconds", time.time() - stime)
    _, save_clustered_qgraph = config.search("saveClusteredQgraph", opt={"default": False})
    if save_clustered_qgraph:
        with open(os.path.join(submit_path, "bps_clustered_qgraph.pickle"), 'wb') as outfh:
            pickle.dump(clustered_qgraph, outfh)
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        draw_networkx_dot(clustered_qgraph, os.path.join(submit_path, "bps_clustered_qgraph.dot"))
    return config, clustered_qgraph


def create_generic_workflow(config, clustered_qgraph):
    """Create a generic representation of the workflow.

    Parameters
    ----------
    config : `~lsst.ctrl.bps.BpsConfig`
        Batch Processing service configuration.
    clustered_qgraph : `~lsst.ctrl.bps.ClusteredQuantumGraph`
        A graph representing clustered quanta.

    Returns
    -------
    generic_workflow_config : `~lsst.ctrl.bps.BpsConfig`
        Generic workflow configuration.
    generic_workflow : `~lsst.ctrl.bps.GenericWorkflow`
        A representation an generic workflow.
    """
    stime = time.time()
    submit_path = config[".bps_defined.submit_path"]
    _LOG.info("Creating Generic Workflow")
    generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
    _LOG.info("Creating Generic Workflow took %.2f seconds", time.time() - stime)
    _LOG.info("Generic Workflow name %s", generic_workflow.name)
    _, save_workflow = config.search("saveGenericWorkflow", opt={"default": False})
    if save_workflow:
        with open(os.path.join(submit_path, "bps_generic_workflow.pickle"), 'wb') as outfh:
            generic_workflow.save(outfh, 'pickle')
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        with open(os.path.join(submit_path, "bps_generic_workflow.dot"), 'w') as outfh:
            generic_workflow.draw(outfh, 'dot')
    return generic_workflow_config, generic_workflow


def create_wms_workflow(generic_workflow_config, generic_workflow):
    """Create a representation of the workflow specific to a given WMS.

    Parameters
    ----------
    generic_workflow_config : `~lsst.ctrl.bps.BpsConfig`
        Generic workflow configuration.
    generic_workflow : `~lsst.ctrl.bps.GenericWorkflow`
        A graph representing an abstract/scientific workflow.

    Returns
    -------
    wms_workflow_config : `~lsst.ctrl.bps.BpsConfig`
        Configuration of the workflow specific to a given WMS.
    wms_workflow_config : `~lsst.ctrl.bps.BaseWmsWorkflow`
        A representation of the generic workflow specific to a given WMS.
    """
    stime = time.time()
    submit_path = generic_workflow_config[".bps_defined.submit_path"]
    _LOG.info("Creating specific implementation of workflow")
    wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)
    wms_workflow_config = generic_workflow_config
    _LOG.info("Creating specific implementation of workflow took %.2f seconds", time.time() - stime)
    return wms_workflow_config, wms_workflow
