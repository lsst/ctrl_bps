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

"""API for cancelling runs submitted to a WMS.
"""

import logging

from lsst.utils import doImport

_LOG = logging.getLogger()


def cancel(wms_service, wms_id, user, require_bps, pass_thru):
    """Cancel submitted workflows.

    Parameters
    ----------
    wms_service : `str` or `~lsst.ctrl.bps.wms_service.WmsService`
        Name of the Workload Management System service class.
    user : `str`
        Cancel all submitted workflows for specified user.
    require_bps : `bool`
        Require given run_id/user to be a bps submitted job.
    wms_id : `str`
        Cancel submitted jobs matching specified WMS id.
    pass_thru : `str`
        A string to pass directly to the WMS service class.
    """
    _LOG.debug("Cancel params: wms_id=%s, user=%s, require_bps=%s, pass_thru=%s",
               wms_id, user, require_bps, pass_thru)

    if isinstance(wms_service, str):
        wms_service_class = doImport(wms_service)
        service = wms_service_class({})
    else:
        service = wms_service

    jobs = service.list_submitted_jobs(wms_id, user, require_bps, pass_thru)
    if len(jobs) == 0:
        print("0 jobs found matching arguments.")
    else:
        for job_id in sorted(jobs):
            results = service.cancel(job_id, pass_thru)
            if results[0]:
                print(f"Successfully canceled: {job_id}")
            else:
                print(f"Couldn't cancel job with id = {job_id} ({results[1]})")
