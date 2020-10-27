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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import logging
from lsst.daf.butler.cli.cliLog import CliLog
from ...submit import submit


def _submit(config, workflow, log_level, **kwargs):
    """Submit workflow for execution.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when submitting the workflow for execution.
    workflow : `lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    log_level : `list` of `tuple`
        Per-component logging levels, each item in the list is a tuple
        (component, level), `component` is a logger name or an empty string
        or `None` for root logger, `level` is a logging level name, one of
        CRITICAL, ERROR, WARNING, INFO, DEBUG (case insensitive).
    """
    if log_level is not None:
        CliLog.setLogLevels(log_level)
    submit(config, workflow)
    print(f"Run Id: {workflow.run_id}")
