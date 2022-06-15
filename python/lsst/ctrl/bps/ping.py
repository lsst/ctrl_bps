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

"""Supporting functions for pinging WMS service.
"""

__all__ = ["ping"]

import logging

from lsst.utils import doImport

_LOG = logging.getLogger(__name__)


def ping(wms_service, pass_thru=None):
    """Checks whether WMS services are up, reachable, and any authentication,
    if needed, succeeds.

    The services to be checked are those needed for submit, report, cancel,
    restart, but ping cannot guarantee whether jobs would actually run
    successfully.

    Parameters
    ----------
    wms_service : `str`
        Name of the class.
    pass_thru : `str`
        A string to pass directly to the WMS service class.

    Returns
    -------
    status : `int`
        Services are available (0) or problems (not 0)
    message : `str`
        Any message from WMS (e.g., error details).
    """
    wms_service_class = doImport(wms_service)
    wms_service = wms_service_class({})

    return wms_service.ping(pass_thru)
