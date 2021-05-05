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
from lsst.ctrl.bps.submit import create_wms_workflow


def cli_prepare(generic_workflow_config, generic_workflow, **kwargs):
    """Create a workflow for a specific workflow management system.

    Parameters
    ----------
    generic_workflow_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    generic_workflow : `lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    wms_config : `lsst.ctrl.bps.BpsConfig`
        Configuration to use when creating the workflow.
    workflow : `lsst.ctrl.bps.wms_workflow.BaseWmsWorkflow`
        Representation of the abstract/scientific workflow specific to a given
        workflow management system.
    """
    wms_workflow_config, wms_workflow = create_wms_workflow(generic_workflow_config, generic_workflow)
    return wms_workflow_config, wms_workflow
