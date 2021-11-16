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

from lsst.utils import doImport


_LOG = logging.getLogger(__name__)


def restart(wms_service, run_id):
    """Restart a failed workflow.

    Parameters
    ----------
    wms_service : `str` or `lsst.ctrl.bps.BaseWmsService`
        Name of the Workload Management System service class.
    run_id : `str`, optional
        Id or path of workflow that need to be restarted.

    Returns
    -------
    run_id : `str`
        Id of the restarted workflow if restart was successfull and None
        otherwise.
    message : `str`
        Error message if the restart failed.
    """
    if isinstance(wms_service, str):
        wms_service_class = doImport(wms_service)
        service = wms_service_class({})
    else:
        service = wms_service
    run_id, message = service.restart(run_id)
    return run_id, message
