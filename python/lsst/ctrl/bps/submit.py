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
import datetime
import logging
import os
import pickle
import time

from lsst.utils import doImport

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


def create_submission(config):
    """Create submission files but don't actually submit
    """
    subtime = time.time()

    config[".bps_defined.timestamp"] = "{:%Y%m%dT%Hh%Mm%Ss}".format(datetime.datetime.now())
    if "uniqProcName" not in config:
        config[".bps_defined.uniqProcName"] = config["outCollection"].replace("/", "_")
    if "operator" not in config:
        config[".bps_defined.operator"] = getpass.getuser()

    # make submit directory to contain all outputs
    submit_path = config["submitPath"]
    os.makedirs(submit_path, exist_ok=True)
    config[".bps_defined.submit_path"] = submit_path

    stime = time.time()
    _LOG.info("Pre-transform steps (includes QuantumGraph generation if needed)")
    qgraph_file, qgraph = pre_transform(config, out_prefix=submit_path)
    _LOG.info("Run QuantumGraph file %s", qgraph_file)
    config['.bps_defined.run_qgraph_file'] = qgraph_file
    clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])
    _LOG.info("Pre-transform steps took %.2f seconds", time.time() - stime)
    with open(os.path.join(submit_path, "bps_clustered_qgraph.pickle"), 'wb') as outfh:
        pickle.dump(clustered_qgraph, outfh)
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        draw_networkx_dot(clustered_qgraph, os.path.join(submit_path, "bps_clustered_qgraph.dot"))

    stime = time.time()
    _LOG.info("Creating Generic Workflow")
    generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
    _LOG.info("Creating Generic Workflow took %.2f seconds", time.time() - stime)
    _LOG.info("Generic Workflow name %s", generic_workflow.name)
    with open(os.path.join(submit_path, "bps_generic_workflow.pickle"), 'wb') as outfh:
        generic_workflow.save(outfh, 'pickle')
    if save_dot:
        with open(os.path.join(submit_path, "bps_generic_workflow.dot"), 'wb') as outfh:
            generic_workflow.draw(outfh, 'dot')

    stime = time.time()
    _LOG.info("Creating specific implementation of workflow")
    wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)
    _LOG.info("Creating specific implementation of workflow took %.2f seconds", time.time() - stime)

    _LOG.info("Total submission creation time = %.2f", time.time() - subtime)
    return wms_workflow
