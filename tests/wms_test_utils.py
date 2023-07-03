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
import logging

from lsst.ctrl.bps.wms_service import BaseWmsService

_LOG = logging.getLogger(__name__)


class WmsServiceSuccess(BaseWmsService):
    """WMS service class with working ping."""

    def ping(self, pass_thru):
        _LOG.info(f"Success {pass_thru}")
        return 0, ""


class WmsServiceFailure(BaseWmsService):
    """WMS service class with non-functional ping."""

    def ping(self, pass_thru):
        _LOG.warning("service failure")
        return 64, "Couldn't contact service X"


class WmsServicePassThru(BaseWmsService):
    """WMS service class with pass through ping."""

    def ping(self, pass_thru):
        _LOG.info(pass_thru)
        return 0, pass_thru


class WmsServiceDefault(BaseWmsService):
    """WMS service class with default ping."""

    def ping(self, pass_thru):
        _LOG.info(f"DEFAULT {pass_thru}")
        return 0, "default"
