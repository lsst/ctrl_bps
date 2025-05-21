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

"""Supporting functions for reporting basic status of a single run
submitted to a WMS.
"""

__all__ = ["status"]

import logging

from lsst.utils import doImportType
from lsst.utils.timer import time_this

from . import DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT
from .wms_service import BaseWmsService, WmsStates

_LOG = logging.getLogger(__name__)


def status(
    wms_service_class_name: str,
    run_id: str,
    hist: float = 0,
    is_global: bool = False,
) -> tuple[WmsStates, str]:
    """Quickly retrieve status of run submitted for execution.

    Parameters
    ----------
    wms_service_class_name : `str`
        Name of the WMS service class.
    run_id : `str`
        A run id for which to get the status.
    hist : `float`, optional
        Include runs from the given number of past days. Defaults to 0.
    is_global : `bool`, optional
        If set, all available job queues will be queried for job information.
        Defaults to False which means that only a local job queue will be
        queried for information.

        Only applicable in the context of a WMS using distributed job queues
        (e.g., HTCondor).

    Returns
    -------
    state : `WmsStates`
        Status of the run.
    message : `str`
        Errors that happened during status retrieval.
        Empty if no issues were encountered.
    """
    with time_this(
        log=_LOG,
        level=logging.DEBUG,
        prefix=None,
        msg="WMS service class imported.",
        mem_usage=False,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_service_class = doImportType(wms_service_class_name)

    wms_service: BaseWmsService = wms_service_class({})

    with time_this(
        log=_LOG,
        level=logging.DEBUG,
        prefix=None,
        msg=f"Got status for workflow {run_id}",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        state, message = wms_service.get_status(wms_workflow_id=run_id, hist=hist, is_global=is_global)

    return state, message
