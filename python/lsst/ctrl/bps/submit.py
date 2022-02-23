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
import logging

from lsst.ctrl.bps.bps_utils import _create_execution_butler
from lsst.utils import doImport
from lsst.utils.logging import VERBOSE
from lsst.utils.timer import time_this, timeMethod

_LOG = logging.getLogger(__name__)


@timeMethod(logger=_LOG, logLevel=VERBOSE)
def submit(config, wms_workflow, wms_service=None):
    """Convert generic workflow to a workflow for a particular WMS.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration values to be used by submission.
    wms_workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
        The workflow to submit.
    wms_service : `lsst.ctrl.bps.BaseWmsService`, optional
        The workflow management service to which the workflow should be
        submitted.

    Returns
    -------
    wms_workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
        WMS-specific workflow.
    """
    if wms_service is None:
        wms_service_class = doImport(config["wmsServiceClass"])
        wms_service = wms_service_class(config)

    _, when_create = config.search(".executionButler.whenCreate")
    if when_create.upper() == "SUBMIT":
        _, execution_butler_dir = config.search(".bps_defined.executionButlerDir")
        _LOG.info("Creating execution butler in '%s'", execution_butler_dir)
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed creating execution butler"):
            _create_execution_butler(
                config, config["runQgraphFile"], execution_butler_dir, config["submitPath"]
            )

    _LOG.info("Submitting run to a workflow management system for execution")
    with time_this(
        log=_LOG, level=logging.INFO, prefix=None, msg="Completed submitting to a workflow management system"
    ):
        workflow = wms_service.submit(wms_workflow)
    return workflow
