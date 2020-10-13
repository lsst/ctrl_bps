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

"""Support for workflow engines"""

from abc import ABCMeta


class WorkflowEngine:
    """Support for generating WMS workflow
    """
    def __init__(self, config):
        self.config = config

    def implement_workflow(self, gen_workflow):
        """Create submission for a generic workflow
        in a specific WMS
        """
        raise NotImplementedError


class Workflow(metaclass=ABCMeta):
    """Interface for single workflow specific to a WMS

    Parameters
    ----------
    config : `BpsConfig`
        Generic workflow config
    gen_workflow : `networkx.DiGraph`
        Generic workflow graph
    """
    def __init__(self, config, gen_workflow):
        self.workflow_config = config
        self.workflow_graph = gen_workflow
        self.run_id = None

    def submit(self):
        """Submit workflow to WMS
        """
        raise NotImplementedError

    def get_id(self):
        """Return run id
        """
        return self.run_id
