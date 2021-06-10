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
from lsst.utils import doImport


# Config section search order
BPS_SEARCH_ORDER = ["payload", "pipetask", "site", "bps_defined"]


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
