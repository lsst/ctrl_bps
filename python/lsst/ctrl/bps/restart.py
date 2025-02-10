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

"""Driver for submitting a prepared WMS-specific workflow."""

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
    wms_id : `str`
        Id of the restarted workflow. If restart failed, it is set to None.
    run_name : `str`
        Name of the restarted workflow.
    message : `str`
        A message describing any issues encountered during the restart.
        If there were no issue, an empty string is returned.
    """
    if isinstance(wms_service, str):
        wms_service_class = doImport(wms_service)
        service = wms_service_class({})
    else:
        service = wms_service
    return service.restart(run_id)
