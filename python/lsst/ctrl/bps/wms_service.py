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

import logging
import dataclasses
from abc import ABCMeta
from enum import Enum

_LOG = logging.getLogger()


class WmsStates(Enum):
    """Run and job states
    """
    UNKNOWN = 0         # Can't determine state
    MISFIT = 1          # Determined state, but doesn't fit other states
    UNREADY = 2         # Still waiting for parents to finish
    READY = 3           # All of its parents have finished successfully
    PENDING = 4         # Ready to run, visible in batch queue
    RUNNING = 5         # Currently running
    DELETED = 6         # In the process of being deleted or already deleted
    HELD = 7            # In a hold state
    SUCCEEDED = 8       # Have completed with success status
    FAILED = 9          # Have completed with non-success status


@dataclasses.dataclass
class WmsJobReport:
    """WMS job information to be included in detailed report output
    """
    wms_id: str
    name: str
    label: str
    state: WmsStates

    __slots__ = ('wms_id', 'name', 'label', 'state')


@dataclasses.dataclass
class WmsRunReport:
    """WMS run information to be included in detailed report output
    """
    wms_id: str
    path: str
    label: str
    run: str
    project: str
    campaign: str
    payload: str
    operator: str
    run_summary: str
    state: WmsStates
    jobs: list
    total_number_jobs: int
    job_state_counts: dict

    __slots__ = ('wms_id', 'path', 'label', 'run', 'project', 'campaign', 'payload', 'operator',
                 'run_summary', 'state', 'total_number_jobs', 'jobs', 'job_state_counts')


class BaseWmsService:
    """Interface for interactions with a specific WMS
    """
    def __init__(self, config):
        self.config = config
        self.run_id = None
        self.submit_path = None

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Create submission for a generic workflow for a specific WMS.

        Parameters
        ----------
        config : `~lsst.ctrl.bps.bps_config.BpsConfig`
            BPS configuration.
        generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow
        out_prefix : `str`
            Prefix for all WMS output files

        Returns
        -------
        wms_workflow : `BaseWmsWorkflow`
            Prepared WMS Workflow to submit for execution
        """
        raise NotImplementedError

    def submit(self, workflow):
        """Submit a single WMS workflow
        Parameters
        ----------
        workflow : `.BaseWmsWorkflow`
            Prepared WMS Workflow to submit for execution
        """
        raise NotImplementedError

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """Query WMS for status of submitted WMS workflows
        Parameters
        ----------
        wms_workflow_id : `int` or `str`, optional
            Id that can be used by WMS service to look up status.
        user : `str`, optional
            Limit report to submissions by this particular user
        hist : `int`, optional
            Number of days to expand report to include finished WMS workflows.
        pass_thru : `str`, optional
            Additional arguments to pass through to the specific WMS service.

        Returns
        -------
        run_reports: `dict` of `BaseWmsReport`
            Status information for submitted WMS workflows
        message: `str`
            Message to user on how to find more status information specific to WMS
        """
        raise NotImplementedError


class BaseWmsWorkflow(metaclass=ABCMeta):
    """Interface for single workflow specific to a WMS

    Parameters
    ----------
    name : `str`
        Unique name of workflow.
    config : `BpsConfig`
        Generic workflow config.
    """
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.service_class = None
        self.run_id = None
        self.submit_path = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        """Create a WMS-specific workflow from a GenericWorkflow

        Parameters
        ----------
        config : `BpsConfig`
            Configuration values needed for generating a WMS specific workflow
        generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic workflow
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        service_class : `str`
            Full module name of WMS service class that created this workflow.

        Returns
        -------
            A WMS specific workflow
        """

        raise NotImplementedError

    def write(self, out_prefix):
        """Write WMS files for this particular workflow.

        Parameters
        ----------
        out_prefix: `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        """
        raise NotImplementedError
