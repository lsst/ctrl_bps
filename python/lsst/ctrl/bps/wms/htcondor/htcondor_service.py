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
"""Interface between generic workflow to HTCondor workflow system
"""
import os
import logging
from datetime import datetime, timedelta

from lsst.ctrl.bps.bps_utils import chdir
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from lsst.ctrl.bps.generic_workflow import GenericWorkflow
from .lssthtc import (htc_submit_dag, read_node_status, read_dag_node_log, condor_q, condor_history,
                      HTCDag, HTCJob, htc_jobs_to_wms_report)

_LOG = logging.getLogger()


class HTCondorService(BaseWmsService):
    """HTCondor version of WMS service
    """
    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an HTCondor DAG ready for submission

        Parameters
        ----------
        config : `~lsst.ctrl.bps.BPSConfig`
            BPS configuration that includes necessary submit/runtime information
        generic_workflow :  `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments)
        out_prefix : `str`
            The root directory into which all WMS-specific files are written

        Returns
        ----------
        workflow : `~lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorWorkflow`
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        workflow = HTCondorWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                          f"{self.__class__.__module__}."
                                                          f"{self.__class__.__name__}")
        workflow.write(out_prefix)
        return workflow

    def submit(self, workflow):
        """Submit a single HTCondor workflow

        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single HTCondor workflow to submit
        """
        # For workflow portability, internal paths are all relative
        # Need to submit to HTCondor from inside the
        with chdir(workflow.submit_path):
            _LOG.info("Submitting from directory: %s", os.getcwd())
            htc_submit_dag(workflow.dag, dict())
            workflow.run_id = workflow.dag.run_id

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """Return run information based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `int` or `str`
            Limit to specific run based on id.
        user : `str`
            Limit results to runs for this user.
        hist : `float`
            Limit history search to this many days.
        pass_thru : `str`
            Constraints to pass through to HTCondor.

        Returns
        -------
        runs : `dict` of `~lsst.ctrl.bps.wms_service.WmsRunReport`
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be pointers to documentation or
            to WMS specific commands.
        """
        message = ""

        if wms_workflow_id and isinstance(wms_workflow_id, str) and not wms_workflow_id.isnumeric():
            # assume wms_workflow_id is actually the path
            # Try to read from logs
            jobs = read_node_status(wms_workflow_id)
            jobs.update(read_dag_node_log(wms_workflow_id))
        else:
            if wms_workflow_id:
                constraint = f'(DAGManJobId == {int(wms_workflow_id)} || ClusterId == ' \
                             f'{int(wms_workflow_id)})'
            elif pass_thru:
                constraint = pass_thru
            else:
                constraint = 'bps_isjob == "True"'
                if user:
                    constraint += f' && (Owner == "{user}" || bps_operator == "{user}")'
            # check runs in queue
            jobs = condor_q(constraint)

            if hist:
                epoch = (datetime.now() - timedelta(days=hist)).timestamp()
                constraint += f' && (CompletionDate >= {epoch})'
                hist_jobs = condor_history(constraint)
                jobs.update(hist_jobs)

        run_reports = htc_jobs_to_wms_report(jobs)
        return run_reports, message


class HTCondorWorkflow(BaseWmsWorkflow):
    """Single HTCondor workflow

    Parameters
    ----------
    name : `str`
        Unique name for Workflow used when naming files
    config : `~lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.dag = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        """Convert workflow into whatever needed for submission to workflow system
        """
        htc_workflow = cls(generic_workflow.name, config)
        htc_workflow.dag = HTCDag(name=generic_workflow.name)

        _LOG.debug("htcondor dag attribs %s", generic_workflow.run_attrs)
        htc_workflow.dag.add_attribs(generic_workflow.run_attrs)
        htc_workflow.dag.add_attribs({'bps_wms_service': service_class,
                                      'bps_wms_workflow': f"{cls.__module__}.{cls.__name__}"})

        # Create all DAG jobs
        for job_name in generic_workflow:
            gwf_job = generic_workflow.get_job(job_name)
            htc_job = HTCondorWorkflow._create_job(generic_workflow, gwf_job, generic_workflow.run_attrs,
                                                   out_prefix)
            htc_workflow.dag.add_job(htc_job)

        # Add job dependencies to the DAG
        for job_name in generic_workflow:
            htc_workflow.dag.add_job_relationships([job_name], generic_workflow.successors(job_name))
        return htc_workflow

    @staticmethod
    def _create_job(generic_workflow, gwf_job, run_attrs, out_prefix):
        """Convert GenericWorkflow job nodes to DAG jobs
        """
        htc_job = HTCJob(gwf_job.name, label=gwf_job.label)

        htc_job_cmds = {
            "universe": "vanilla",
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "transfer_executable": "False",
            "getenv": "True",
        }

        htc_job_cmds.update(translate_job_cmds(gwf_job))

        # job stdout, stderr, htcondor user log.
        htc_job_cmds["output"] = f"{gwf_job.name}.$(Cluster).out"
        htc_job_cmds["error"] = f"{gwf_job.name}.$(Cluster).err"
        htc_job_cmds["log"] = f"{gwf_job.name}.$(Cluster).log"
        for key in ("output", "error", "log"):
            htc_job_cmds[key] = f"{gwf_job.name}.$(Cluster).{key[:3]}"
            if gwf_job.label:
                htc_job_cmds[key] = os.path.join(gwf_job.label, htc_job_cmds[key])
            htc_job_cmds[key] = os.path.join("jobs", htc_job_cmds[key])
            _LOG.debug("HTCondor %s = %s", key, htc_job_cmds[key])

        htc_job_cmds.update(handle_job_inputs(generic_workflow, gwf_job.name, out_prefix))

        # Add the job cmds dict to the job object.
        htc_job.add_job_cmds(htc_job_cmds)

        # Add run level attributes to job.
        htc_job.add_job_attrs(run_attrs)

        # Add job attributes to job.
        _LOG.debug("gwf_job.attrs = %s", gwf_job.attrs)
        htc_job.add_job_attrs(gwf_job.attrs)
        htc_job.add_job_attrs({"bps_job_name": gwf_job.name,
                               "bps_job_label": gwf_job.label,
                               "bps_job_quanta": gwf_job.quanta_summary})

        return htc_job

    def write(self, out_prefix):
        """Output HTCondor DAGMan files needed for workflow submission.
        """
        self.submit_path = out_prefix
        os.makedirs(out_prefix, exist_ok=True)

        # Write down the workflow in HTCondor format.
        self.dag.write(out_prefix, "jobs/{self.label}")


def translate_job_cmds(generic_workflow_job):
    """Translate the job data that are one to one mapping

    Parameters
    ----------
    generic_workflow_job

    Returns
    -------
    htc_job_commands : `dict`
        Contains commands which can appear in the HTCondor submit description file.
    """
    jobcmds = {}

    if generic_workflow_job.mail_to:
        jobcmds['notify_user'] = generic_workflow_job.mail_to

    if generic_workflow_job.when_to_mail:
        jobcmds['notification'] = generic_workflow_job.when_to_mail

    if generic_workflow_job.request_disk:
        jobcmds['request_disk'] = f"{generic_workflow_job.request_disk}MB"

    if generic_workflow_job.request_memory:
        jobcmds['request_memory'] = f"{generic_workflow_job.request_memory}MB"

    if generic_workflow_job.priority:
        jobcmds['priority'] = generic_workflow_job.priority

    cmd_parts = generic_workflow_job.cmdline.split(' ', 1)
    jobcmds["executable"] = cmd_parts[0]
    if len(cmd_parts) > 1:
        jobcmds["arguments"] = cmd_parts[1]

    # Add extra "pass-thru" job commands
    if generic_workflow_job.profile:
        for key, val in generic_workflow_job.profile.items():
            jobcmds[key] = val

    return jobcmds


def handle_job_inputs(generic_workflow: GenericWorkflow, job_name: str, out_prefix):
    """Add job input files from generic workflow to job

    Parameters
    ----------
    generic_workflow : `.GenericWorkflow`
        The generic workflow (e.g., has executable name and arguments)
    job_name : `str`
        Unique name for the job
    out_prefix : `str`
        The root directory into which all WMS-specific files are written
    """
    htc_commands = {}
    inputs = []
    for gwf_file in generic_workflow.get_job_inputs(job_name, data=True, transfer_only=True):
        inputs.append(os.path.relpath(gwf_file.src_uri, out_prefix))

    if inputs:
        htc_commands["transfer_input_files"] = ",".join(inputs)
        _LOG.debug("transfer_input_files=%s", htc_commands['transfer_input_files'])
    return htc_commands
