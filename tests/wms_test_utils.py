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
import dataclasses
import logging

from lsst.ctrl.bps.wms_service import BaseWmsService, WmsJobReport, WmsRunReport, WmsStates

_LOG = logging.getLogger(__name__)


TEST_REPORT = WmsRunReport(
    wms_id="1.0",
    global_wms_id="foo#1.0",
    path="/path/to/run",
    label="label",
    run="run",
    project="dev",
    campaign="testing",
    payload="test",
    operator="tester",
    run_summary="foo:1",
    state=WmsStates.SUCCEEDED,
    jobs=[WmsJobReport(wms_id="1.0", name="", label="foo", state=WmsStates.SUCCEEDED)],
    total_number_jobs=1,
    job_state_counts={state: 1 if state == WmsStates.SUCCEEDED else 0 for state in WmsStates},
    job_summary={
        "foo": {state: 1 if state == WmsStates.SUCCEEDED else 0 for state in WmsStates},
    },
)


class WmsServiceSuccess(BaseWmsService):
    """WMS service class with working ping and get_status."""

    def ping(self, pass_thru):
        _LOG.info(f"Success {pass_thru}")
        return 0, ""

    def report(
        self,
        wms_workflow_id=None,
        user=None,
        hist=0,
        pass_thru=None,
        is_global=False,
        return_exit_codes=False,
    ):
        report = dataclasses.replace(TEST_REPORT)
        return [report], []

    def get_status(
        self,
        wms_workflow_id,
        hist=0,
        pass_thru=None,
        is_global=False,
    ):
        return WmsStates.SUCCEEDED, ""


class WmsServiceFailure(BaseWmsService):
    """WMS service class with non-functional ping and
    get_status for failed run.
    """

    def ping(self, pass_thru):
        _LOG.warning("service failure")
        return 64, "Couldn't contact service X"

    def report(
        self,
        wms_workflow_id=None,
        user=None,
        hist=0,
        pass_thru=None,
        is_global=False,
        return_exit_codes=False,
    ):
        report = WmsRunReport()
        return [report], []

    def get_status(
        self,
        wms_workflow_id,
        hist=0,
        pass_thru=None,
        is_global=False,
    ):
        return WmsStates.FAILED, "Dummy error message."


class WmsServicePassThru(BaseWmsService):
    """WMS service class with pass through ping."""

    def ping(self, pass_thru):
        _LOG.info(pass_thru)
        return 0, pass_thru


class WmsServiceDefault(BaseWmsService):
    """WMS service class with default ping and get_status."""

    def ping(self, pass_thru):
        _LOG.info(f"DEFAULT {pass_thru}")
        return 0, "default"

    def get_status(
        self,
        wms_workflow_id=None,
        hist=0,
        pass_thru=None,
        is_global=False,
    ):
        return WmsStates.RUNNING, ""


class WmsServiceFromCmdline(BaseWmsService):
    """WMS service class with its own default settings."""

    @property
    def defaults(self):
        return {"corge": "cmdline"}

    @property
    def defaults_path(self):
        return "/wms/class/from/cmdline"


class WmsServiceFromConfig(BaseWmsService):
    """WMS service class with its own default settings."""

    @property
    def defaults(self):
        return {"corge": "config"}

    @property
    def defaults_path(self):
        return "/wms/class/from/config"


class WmsServiceFromEnv(BaseWmsService):
    """WMS service class with its own default settings."""

    @property
    def defaults(self):
        return {"corge": "env"}

    @property
    def defaults_path(self):
        return "/wms/class/from/env"


class WmsServiceFromDefaults(BaseWmsService):
    """WMS service class with its own default settings."""

    @property
    def defaults(self):
        return {"corge": "defaults"}

    @property
    def defaults_path(self):
        return "/wms/class/from/defaults"
