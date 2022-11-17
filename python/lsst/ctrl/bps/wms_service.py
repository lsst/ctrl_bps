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


__all__ = [
    "BaseWmsService",
    "BaseWmsWorkflow",
    "WmsJobReport",
    "WmsRunReport",
    "WmsStates",
]


import dataclasses
import logging
from abc import ABCMeta
from enum import Enum

_LOG = logging.getLogger(__name__)


class WmsStates(Enum):
    """Run and job states"""

    UNKNOWN = 0
    """Can't determine state."""

    MISFIT = 1
    """Determined state, but doesn't fit other states."""

    UNREADY = 2
    """Still waiting for parents to finish."""

    READY = 3
    """All of its parents have finished successfully."""

    PENDING = 4
    """Ready to run, visible in batch queue."""

    RUNNING = 5
    """Currently running."""

    DELETED = 6
    """In the process of being deleted or already deleted."""

    HELD = 7
    """In a hold state."""

    SUCCEEDED = 8
    """Have completed with success status."""

    FAILED = 9
    """Have completed with non-success status."""

    PRUNED = 10
    """At least one of the parents failed or can't be run."""


@dataclasses.dataclass(slots=True)
class WmsJobReport:
    """WMS job information to be included in detailed report output"""

    wms_id: str
    """Job id assigned by the workflow management system."""

    name: str
    """A name assigned automatically by BPS."""

    label: str
    """A user-facing label for a job. Multiple jobs can have the same label."""

    state: WmsStates
    """Job's current execution state."""


@dataclasses.dataclass(slots=True)
class WmsRunReport:
    """WMS run information to be included in detailed report output"""

    wms_id: str = None
    """Id assigned to the run by the WMS.
    """

    global_wms_id: str = None
    """Global run identification number.

    Only applicable in the context of a WMS using distributed job queues
    (e.g., HTCondor).
    """

    path: str = None
    """Path to the submit directory."""

    label: str = None
    """Run's label."""

    run: str = None
    """Run's name."""

    project: str = None
    """Name of the project run belongs to."""

    campaign: str = None
    """Name of the campaign the run belongs to."""

    payload: str = None
    """Name of the payload."""

    operator: str = None
    """Username of the operator who submitted the run."""

    run_summary: str = None
    """Job counts per label."""

    state: WmsStates = None
    """Run's execution state."""

    jobs: list[WmsJobReport] = None
    """Information about individual jobs in the run."""

    total_number_jobs: int = None
    """Total number of jobs in the run."""

    job_state_counts: dict[WmsStates, int] = None
    """Job counts per state."""

    job_summary: dict[str, dict[WmsStates, int]] = None
    """Job counts per label and per state.
    """


class BaseWmsService:
    """Interface for interactions with a specific WMS.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration needed by the WMS service.
    """

    def __init__(self, config):
        self.config = config

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Create submission for a generic workflow for a specific WMS.

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
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
        workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
            Prepared WMS Workflow to submit for execution
        """
        raise NotImplementedError

    def restart(self, wms_workflow_id):
        """Restart a workflow from the point of failure.

        Parameters
        ----------
        wms_workflow_id : `str`
            Id that can be used by WMS service to identify workflow that
            need to be restarted.

        Returns
        -------
        wms_id : `str`
            Id of the restarted workflow. If restart failed, it will be set
            to None.
        run_name : `str`
            Name of the restarted workflow. If restart failed, it will be set
            to None.
        message : `str`
            A message describing any issues encountered during the restart.
            If there were no issue, an empty string is returned.
        """
        raise NotImplementedError

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False):
        """Query WMS for list of submitted WMS workflows/jobs.

        This should be a quick lookup function to create list of jobs for
        other functions.

        Parameters
        ----------
        wms_id : `int` or `str`, optional
            Id or path that can be used by WMS service to look up job.
        user : `str`, optional
            User whose submitted jobs should be listed.
        require_bps : `bool`, optional
            Whether to require jobs returned in list to be bps-submitted jobs.
        pass_thru : `str`, optional
            Information to pass through to WMS.
        is_global : `bool`, optional
            If set, all available job queues will be queried for job
            information.  Defaults to False which means that only a local job
            queue will be queried for information.

            Only applicable in the context of a WMS using distributed job
            queues (e.g., HTCondor). A WMS with a centralized job queue
            (e.g. PanDA) can safely ignore it.

        Returns
        -------
        job_ids : `list` [`Any`]
            Only job ids to be used by cancel and other functions.  Typically
            this means top-level jobs (i.e., not children jobs).
        """
        raise NotImplementedError

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None, is_global=False):
        """Query WMS for status of submitted WMS workflows.

        Parameters
        ----------
        wms_workflow_id : `int` or `str`, optional
            Id that can be used by WMS service to look up status.
        user : `str`, optional
            Limit report to submissions by this particular user.
        hist : `int`, optional
            Number of days to expand report to include finished WMS workflows.
        pass_thru : `str`, optional
            Additional arguments to pass through to the specific WMS service.
        is_global : `bool`, optional
            If set, all available job queues will be queried for job
            information.  Defaults to False which means that only a local job
            queue will be queried for information.

            Only applicable in the context of a WMS using distributed job
            queues (e.g., HTCondor). A WMS with a centralized job queue
            (e.g. PanDA) can safely ignore it.

        Returns
        -------
        run_reports : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Status information for submitted WMS workflows.
        message : `str`
            Message to user on how to find more status information specific to
            this particular WMS.
        """
        raise NotImplementedError

    def cancel(self, wms_id, pass_thru=None):
        """Cancel submitted workflows/jobs.

        Parameters
        ----------
        wms_id : `str`
            ID or path of job that should be canceled.
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        -------
        deleted : `bool`
            Whether successful deletion or not.  Currently, if any doubt or any
            individual jobs not deleted, return False.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        raise NotImplementedError

    def run_submission_checks(self):
        """Checks to run at start if running WMS specific submission steps.

        Any exception other than NotImplementedError will halt submission.
        Submit directory may not yet exist when this is called.
        """
        raise NotImplementedError

    def ping(self, pass_thru):
        """Checks whether WMS services are up, reachable, and can authenticate
        if authentication is required.

        The services to be checked are those needed for submit, report, cancel,
        restart, but ping cannot guarantee whether jobs would actually run
        successfully.

        Parameters
        ----------
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        -------
        status : `int`
            0 for success, non-zero for failure
        message : `str`
            Any message from WMS (e.g., error details).
        """
        raise NotImplementedError


class BaseWmsWorkflow(metaclass=ABCMeta):
    """Interface for single workflow specific to a WMS.

    Parameters
    ----------
    name : `str`
        Unique name of workflow.
    config : `lsst.ctrl.bps.BpsConfig`
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
        config : `lsst.ctrl.bps.BpsConfig`
            Configuration values needed for generating a WMS specific workflow.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
            Generic workflow from which to create the WMS-specific one.
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        service_class : `str`
            Full module name of WMS service class that created this workflow.

        Returns
        -------
        wms_workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
            A WMS specific workflow.
        """

        raise NotImplementedError

    def write(self, out_prefix):
        """Write WMS files for this particular workflow.

        Parameters
        ----------
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        """
        raise NotImplementedError
