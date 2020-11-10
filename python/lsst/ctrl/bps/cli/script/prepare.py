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
from lsst.daf.butler.cli.cliLog import CliLog
from ...bps_config import BpsConfig
from ...bpsub_driver import BPS_SEARCH_ORDER, create_submission


def _prepare(config_file, log_level, **kwargs):
    """Create a workflow for a specific workflow management system.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.
    log_level : `str`
        Per-component logging levels, each item in the list is a tuple
        (component, level), `component` is a logger name or an empty string
        or `None` for root logger, `level` is a logging level name, one of
        CRITICAL, ERROR, WARNING, INFO, DEBUG (case insensitive).

    Returns
    -------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    workflow : `lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    """
    if log_level is not None:
        CliLog.setLogLevels(log_level)
    config = BpsConfig(config_file, BPS_SEARCH_ORDER)
    workflow = create_submission(config)
    print(f"Submit dir: {workflow.submit_path}")
    return config, workflow
