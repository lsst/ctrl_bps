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

"""API for cancelling runs submitted to a WMS.
"""

import logging

from lsst.utils import doImport

_LOG = logging.getLogger(__name__)


def cancel(wms_service, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False):
    """Cancel submitted workflows.

    Parameters
    ----------
    wms_service : `str` or `lsst.ctrl.bps.BaseWmsService`
        Name of the Workload Management System service class.
    wms_id : `str`, optional
        ID or path of job that should be canceled.
    user : `str`, optional
        User whose submitted jobs should be canceled.
    require_bps : `bool`, optional
        Whether to require given run_id/user to be a bps submitted job.
    pass_thru : `str`, optional
        Information to pass through to WMS.
    is_global : `bool`, optional
        If set, all available job queues will be checked for jobs to cancel.
        Defaults to False which means that only a local job queue will be
        checked.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).
    """
    _LOG.debug(
        "Cancel params: wms_id=%s, user=%s, require_bps=%s, pass_thru=%s, is_global=%s",
        wms_id,
        user,
        require_bps,
        pass_thru,
        is_global,
    )

    if isinstance(wms_service, str):
        wms_service_class = doImport(wms_service)
        service = wms_service_class({})
    else:
        service = wms_service

    jobs = service.list_submitted_jobs(wms_id, user, require_bps, pass_thru, is_global)
    if len(jobs) == 0:
        print(
            "No job matches the search criteria. "
            "Hints: Double check id, and/or use --global to search all job queues."
        )
    else:
        for job_id in sorted(jobs):
            results = service.cancel(job_id, pass_thru)
            if results[0]:
                print(f"Successfully canceled job with id '{job_id}'")
            else:
                print(f"Couldn't cancel job with id '{job_id}' ({results[1]})")
