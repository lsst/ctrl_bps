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

"""Base classes for working with a specific WMS"""

from abc import ABCMeta


class BaseWmsService:
    """Interface for interactions with a specific WMS
    """
    def __init__(self, config):
        self.config = config
        self.run_id = None
        self.submit_path = None

    def prepare(self, config, generic_workflow, path=None):
        """Create submission for a generic workflow
        in a specific WMS
        """
        raise NotImplementedError

    def submit(self, wms_workflow):
        """Submit a single WMS workflow
        """
        raise NotImplementedError

    def status(self, wms_workflow_id=None):
        """Query WMS for status of submitted WMS
        """
        raise NotImplementedError

    def history(self, wms_workflow_id=None):
        """Query WMS for status of completed WMS
        """
        raise NotImplementedError


class BaseWmsWorkflow(metaclass=ABCMeta):
    """Interface for single workflow specific to a WMS

    Parameters
    ----------
    config : `BpsConfig`
        Generic workflow config
    gen_workflow : `networkx.DiGraph`
        Generic workflow graph
    """
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.run_id = None
        self.submit_path = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow):
        """Create a WMS-specific workflow from a GenericWorkflow
        """
        raise NotImplementedError

    def write(self, out_prefix):
        """ Write WMS files for this particular workflow.
        """
        raise NotImplementedError
