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

"""Driver for preparing a WMS-specific workflow.
"""

import logging

from lsst.utils import doImport
from lsst.utils.logging import VERBOSE
from lsst.utils.timer import time_this, timeMethod

from .bps_utils import (
    WhenToSaveQuantumGraphs,
    _create_execution_butler,
    create_job_quantum_graph_filename,
    save_qg_subgraph,
)

_LOG = logging.getLogger(__name__)


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def prepare(config, generic_workflow, out_prefix):
    """Convert generic workflow to a workflow for a particular WMS.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Contains configuration for BPS.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Contains generic workflow.
    out_prefix : `str`
        Contains directory to which any WMS-specific files should be written.

    Returns
    -------
    wms_workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
        WMS-specific workflow.
    """
    search_opt = {"searchobj": config["executionButler"]}
    _, when_create = config.search("whenCreate", opt=search_opt)
    if when_create.upper() == "PREPARE":
        _, execution_butler_dir = config.search(".bps_defined.executionButlerDir", opt=search_opt)
        _LOG.info("Creating execution butler in '%s'", execution_butler_dir)
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Creating execution butler completed"):
            _create_execution_butler(config, config["runQgraphFile"], execution_butler_dir, out_prefix)

    found, wms_class = config.search("wmsServiceClass")
    if not found:
        raise KeyError("Missing wmsServiceClass in bps config.  Aborting.")

    wms_service_class = doImport(wms_class)
    wms_service = wms_service_class(config)
    wms_workflow = wms_service.prepare(config, generic_workflow, out_prefix)

    # Save QuantumGraphs (putting after call to prepare so don't write a
    # bunch of files if prepare fails)
    found, when_save = config.search("whenSaveJobQgraph")
    if found and WhenToSaveQuantumGraphs[when_save.upper()] == WhenToSaveQuantumGraphs.PREPARE:
        for job_name in generic_workflow.nodes():
            job = generic_workflow.get_job(job_name)
            save_qg_subgraph(job.quantum_graph, create_job_quantum_graph_filename(job, out_prefix))

    return wms_workflow
