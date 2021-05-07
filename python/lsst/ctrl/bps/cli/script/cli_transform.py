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
"""Driver function for transform subcommand.
"""
from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.submit import (
    BPS_SEARCH_ORDER,
    init_runtime_env,
    create_clustered_qgraph,
    create_generic_workflow,
)


def cli_transform(config_file, **kwargs):
    """Create a workflow for a specific workflow management system.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.

    Returns
    -------
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    generic_workflow : `lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    """
    config = BpsConfig(config_file, BPS_SEARCH_ORDER)
    config = init_runtime_env(config)
    clustered_qgraph_config, clustered_qgraph = create_clustered_qgraph(config)
    generic_workflow_config, generic_workflow = create_generic_workflow(clustered_qgraph_config,
                                                                        clustered_qgraph)
    return generic_workflow_config, generic_workflow
