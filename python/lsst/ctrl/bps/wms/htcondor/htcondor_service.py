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

__all__ = ["HTCondorService", "HTCondorWorkflow"]

import os
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path

from lsst.ctrl.bps.bps_utils import chdir
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService, WmsRunReport, WmsJobReport, WmsStates
from lsst.ctrl.bps.generic_workflow import GenericWorkflow
from .lssthtc import (htc_submit_dag, read_node_status, read_dag_log, condor_q, condor_history, HTCDag,
                      HTCJob, JobStatus, NodeStatus, htc_escape, read_dag_status, summary_from_dag,
                      htc_check_dagman_output, MISSING_ID, pegasus_name_to_label)

_LOG = logging.getLogger()
COUNT_ERROR = -99999


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
            HTCondor workflow ready to be run.
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

        if wms_workflow_id:
            try:
                float(wms_workflow_id)
                is_float = True
            except ValueError:  # Don't need TypeError here as None goes to else branch.
                is_float = False

            if is_float:
                run_reports, message = _report_from_id(float(wms_workflow_id), hist)
            else:
                run_reports, message = _report_from_path(wms_workflow_id)
        else:
            run_reports, message = summary_report(user, hist, pass_thru)
        _LOG.debug("report: %s, %s", run_reports, message)

        return list(run_reports.values()), message


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
        # Docstring inherited
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

        Parameters
        ----------
        generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic workflow that is being converted.
        gwf_job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
            The generic job to convert to a Pegasus job.
        run_attrs : `dict` [`str`: `str`]
            Attributes common to entire run that should be added to job.
        out_prefix : `str`
            Directory prefix for HTCondor files.

        Returns
        -------
        htc_job : `~lsst.ctrl.bps.wms.htcondor.lssthtc.HTCJob`
            The HTCondor job equivalent to the given generic job.
        """
        htc_job = HTCJob(gwf_job.name, label=gwf_job.label)

        htc_job_cmds = {
            "universe": "vanilla",
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "transfer_executable": "False",
            "getenv": "True",
        }

        htc_job_cmds.update(_translate_job_cmds(gwf_job))

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

        htc_job_cmds.update(_handle_job_inputs(generic_workflow, gwf_job.name, out_prefix))

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

        Parameters
        ----------
        out_prefix : `str`
            Directory prefix for HTCondor files.
        """
        self.submit_path = out_prefix
        os.makedirs(out_prefix, exist_ok=True)

        # Write down the workflow in HTCondor format.
        self.dag.write(out_prefix, "jobs/{self.label}")


def _translate_job_cmds(generic_workflow_job):
    """Translate the job data that are one to one mapping

    Parameters
    ----------
    generic_workflow_job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
       Generic workflow job that is being converted.

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
            jobcmds[key] = htc_escape(val)

    return jobcmds


def _handle_job_inputs(generic_workflow: GenericWorkflow, job_name: str, out_prefix):
    """Add job input files from generic workflow to job.

    Parameters
    ----------
    generic_workflow : `.GenericWorkflow`
        The generic workflow (e.g., has executable name and arguments).
    job_name : `str`
        Unique name for the job.
    out_prefix : `str`
        The root directory into which all WMS-specific files are written.

    Returns
    -------
    htc_commands : `dict` [`str`: `str`]
        HTCondor commands for the job submission.
    """
    htc_commands = {}
    inputs = []
    for gwf_file in generic_workflow.get_job_inputs(job_name, data=True, transfer_only=True):
        inputs.append(os.path.relpath(gwf_file.src_uri, out_prefix))

    if inputs:
        htc_commands["transfer_input_files"] = ",".join(inputs)
        _LOG.debug("transfer_input_files=%s", htc_commands['transfer_input_files'])
    return htc_commands


def _report_from_path(wms_path):
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_path : `str`
        The directory containing the submit side files (e.g., HTCondor files).

    Returns
    -------
    run_reports : `dict` [`str`, `WmsRunReport`]
        Run information for the detailed report.  The key is the HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    wms_workflow_id, jobs, message = _get_info_from_path(wms_path)
    if wms_workflow_id == MISSING_ID:
        run_reports = {}
    else:
        run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
    return run_reports, message


def _report_from_id(wms_workflow_id, hist):
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_workflow_id : `int` or `str`
        Limit to specific run based on id.
    hist : `float`
        Limit history search to this many days.

    Returns
    -------
    run_reports : `dict` [`str`, `WmsRunReport`]
        Run information for the detailed report.  The key is the HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    constraint = f'(DAGManJobId == {int(float(wms_workflow_id))} || ClusterId == ' \
                 f'{int(float(wms_workflow_id))})'
    jobs = condor_q(constraint)
    if hist:
        epoch = (datetime.now() - timedelta(days=hist)).timestamp()
        constraint += f' && (CompletionDate >= {epoch} || JobFinishedHookDone >= {epoch})'
        hist_jobs = condor_history(constraint)
        update_jobs(jobs, hist_jobs)

    # keys in dictionary will be strings of format "ClusterId.ProcId"
    wms_workflow_id = str(wms_workflow_id)
    if not wms_workflow_id.endswith(".0"):
        wms_workflow_id += ".0"

    if wms_workflow_id in jobs:
        _, path_jobs, message = _get_info_from_path(jobs[wms_workflow_id]['Iwd'])
        update_jobs(jobs, path_jobs)
        run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
    else:
        run_reports = {}
        message = f"Found 0 records for run id {wms_workflow_id}"
    return run_reports, message


def _get_info_from_path(wms_path):
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_path : `str`
        Directory containing HTCondor files.

    Returns
    -------
    wms_workflow_id : `str`
        The run id which is a DAGman job id.
    jobs : `dict` [`str`, `dict` [`str`, `Any`]]
        Information about jobs read from files in the given directory.
        The key is the HTCondor id and the value is a dictionary of HTCondor
        keys and values.
    message : `str`
        Message to be printed with the summary report.
    """
    try:
        wms_workflow_id, jobs = read_dag_log(wms_path)
        _LOG.debug("_get_info_from_path: from dag log %s = %s", wms_workflow_id, jobs)
        update_jobs(jobs, read_node_status(wms_path))
        _LOG.debug("_get_info_from_path: after node status %s = %s", wms_workflow_id, jobs)

        # Add more info for DAGman job
        job = jobs[wms_workflow_id]
        job.update(read_dag_status(wms_path))
        job['total_jobs'], job['state_counts'] = _get_job_state_counts(job)
        if "bps_run" not in job:
            _add_run_info(wms_path, job)

        message = htc_check_dagman_output(wms_path)
        _LOG.debug("_get_info: id = %s, total_jobs = %s", wms_workflow_id,
                   jobs[wms_workflow_id]["total_jobs"])
    except StopIteration:
        message = f"Could not find HTCondor files in {wms_path}"
        _LOG.warning(message)
        wms_workflow_id = MISSING_ID
        jobs = {}

    return wms_workflow_id, jobs, message


def _create_detailed_report_from_jobs(wms_workflow_id, jobs):
    """Gather run information to be used in generating summary reports.

    Parameters
    ----------
    wms_workflow_id : `str`
        Run lookup restricted to given user.
    jobs : `float`
        How many previous days to search for run information.

    Returns
    -------
    run_reports : `dict` [`str`, `WmsRunReport`]
        Run information for the detailed report.  The key is the given HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    _LOG.debug("_create_detailed_report: id = %s, job = %s", wms_workflow_id, jobs[wms_workflow_id])
    dag_job = jobs[wms_workflow_id]
    if "total_jobs" not in dag_job or "DAGNodeName" in dag_job:
        _LOG.error("Job ID %s is not a DAG job.", wms_workflow_id)
        return {}
    report = WmsRunReport(wms_id=wms_workflow_id,
                          path=dag_job["Iwd"],
                          label=dag_job.get("bps_job_label", "MISS"),
                          run=dag_job.get("bps_run", "MISS"),
                          project=dag_job.get("bps_project", "MISS"),
                          campaign=dag_job.get("bps_campaign", "MISS"),
                          payload=dag_job.get("bps_payload", "MISS"),
                          operator=_get_owner(dag_job),
                          run_summary=_get_run_summary(dag_job),
                          state=_htc_status_to_wms_state(dag_job),
                          jobs=[],
                          total_number_jobs=dag_job['total_jobs'],
                          job_state_counts=dag_job['state_counts'])

    try:
        for job in jobs.values():
            if job["ClusterId"] != int(float(wms_workflow_id)):
                job_report = WmsJobReport(wms_id=job["ClusterId"],
                                          name=job["DAGNodeName"],
                                          label=job.get("bps_job_label",
                                                        pegasus_name_to_label(job["DAGNodeName"])),
                                          state=_htc_status_to_wms_state(job))
                if job_report.label == "init":
                    job_report.label = "pipetaskInit"
                report.jobs.append(job_report)
    except KeyError as ex:
        _LOG.error("Job missing key '%s': %s", str(ex), job)
        raise

    run_reports = {report.wms_id: report}
    _LOG.debug("_create_detailed_report: run_reports = %s", run_reports)
    return run_reports


def summary_report(user, hist, pass_thru):
    """Gather run information to be used in generating summary reports.

    Parameters
    ----------
    user : `str`
        Run lookup restricted to given user.
    hist : `float`
        How many previous days to search for run information.
    pass_thru : `str`
        Advanced users can define the HTCondor constraint to be used
        when searching queue and history.

    Returns
    -------
    run_reports : `dict` [`str`, `WmsRunReport`]
        Run information for the summary report.  The keys are HTCondor ids and
        the values are collections of report information for each run.
    message : `str`
        Message to be printed with the summary report.
    """
    # only doing summary report so only look for dagman jobs
    if pass_thru:
        constraint = pass_thru
    else:
        # Note: bps_isjob == 'True' isn't getting set for DAG jobs that are manually restarted
        #       Any job with DAGManJobID isn't a DAG job
        constraint = 'bps_isjob == "True" && JobUniverse == 7'
        if user:
            constraint += f' && (Owner == "{user}" || bps_operator == "{user}")'

        # check runs in queue
        jobs = condor_q(constraint)

    if hist:
        epoch = (datetime.now() - timedelta(days=hist)).timestamp()
        constraint += f' && (CompletionDate >= {epoch} || JobFinishedHookDone >= {epoch})'
        hist_jobs = condor_history(constraint)
        update_jobs(jobs, hist_jobs)

    _LOG.debug("Job ids from queue and history %s", jobs.keys())

    # Have list of DAGMan jobs, need to get run_report info
    run_reports = {}
    for job in jobs.values():
        total_jobs, state_counts = _get_job_state_counts(job)
        # if didn't get from queue information (e.g., Kerberos bug),
        # try reading from file
        if total_jobs == COUNT_ERROR:
            try:
                job.update(read_dag_status(job['Iwd']))
                total_jobs, state_counts = _get_job_state_counts(job)
            except StopIteration:
                pass   # don't kill report can't find htcondor files

        if "bps_run" not in job:
            _add_run_info(job['Iwd'], job)
        report = WmsRunReport(wms_id=job.get("ClusterId", MISSING_ID),
                              path=job["Iwd"],
                              label=job.get("bps_job_label", "MISS"),
                              run=job.get("bps_run", "MISS"),
                              project=job.get("bps_project", "MISS"),
                              campaign=job.get("bps_campaign", "MISS"),
                              payload=job.get("bps_payload", "MISS"),
                              operator=_get_owner(job),
                              run_summary=_get_run_summary(job),
                              state=_htc_status_to_wms_state(job),
                              jobs=[],
                              total_number_jobs=total_jobs,
                              job_state_counts=state_counts)

        run_reports[report.wms_id] = report

    return run_reports, ""


def _add_run_info(wms_path, job):
    """Find BPS run information elsewhere for runs without bps attributes.

    Parameters
    ----------
    wms_path : `str`
        Path to submit files for the run.
    job : `dict`
        HTCondor dag job information.

    Returns
    -------
    owner : `str`
        Owner of the dag job.

    Raises
    ------
    StopIteration
        If cannot find file it is looking for.  Permission errors are
        caught and job's run is marked with error.
    """
    path = Path(wms_path) / "jobs"
    try:
        jobdir = next(path.glob("*"), Path(wms_path))
        try:
            subfile = next(jobdir.glob("*.sub"))
            _LOG.debug("_add_run_info: subfile = %s", subfile)
            with open(subfile, "r") as fh:
                for line in fh:
                    if line.startswith("+bps_"):
                        m = re.match(r"\+(bps_[^\s]+)\s*=\s*(.+)$", line)
                        if m:
                            _LOG.debug("Matching line: %s", line)
                            job[m.group(1)] = m.group(2).replace('"', '')
                        else:
                            _LOG.debug("Could not parse attribute: %s", line)
        except StopIteration:
            job["bps_run"] = "Missing"

    except PermissionError:
        job["bps_run"] = "PermissionError"
    _LOG.debug("After adding job = %s", job)


def _get_owner(job):
    """Get the owner of a dag job.

    Parameters
    ----------
    job : `dict`
        HTCondor dag job information.

    Returns
    -------
    owner : `str`
        Owner of the dag job.
    """
    owner = job.get("bps_operator", None)
    if not owner:
        owner = job.get("Owner", None)
        if not owner:
            _LOG.warning("Could not get Owner from htcondor job: %s", job)
            owner = "MISS"
    return owner


def _get_run_summary(job):
    """Get the run summary for a job.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    summary : `str`
        Number of jobs per PipelineTask label in approximate pipeline order.
        Format: <label>:<count>[;<label>:<count>]+
    """
    summary = job.get("bps_run_summary", None)
    if not summary:
        summary, _ = summary_from_dag(job['Iwd'])
        if not summary:
            _LOG.warning("Could not get run summary for htcondor job: %s", job)
    _LOG.debug("_get_run_summary: summary=%s", summary)

    # Workaround sometimes using init vs pipetaskInit
    summary = summary.replace('init:', 'pipetaskInit')

    if "pegasus_version" in job and "pegasus" not in summary:
        summary += ";pegasus:0"

    return summary


def _get_job_state_counts(job):
    """Count number of jobs per WMS state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    total_count : `int`
        Total number of dag nodes.
    state_counts : `dict` [`WmsStates`, `int`]
        Keys are the different WMS states and values are counts of jobs
        that are in that WMS state.
    """
    _LOG.debug("_get_job_state_counts: job = %s %s", type(job), len(job))
    state_counts = {}
    if 'DAG_NodesReady' in job:
        state_counts = {
            WmsStates.UNREADY: job.get('DAG_NodesUnready', COUNT_ERROR),
            WmsStates.READY: job.get('DAG_NodesReady', COUNT_ERROR),
            WmsStates.HELD: job.get('JobProcsHeld', COUNT_ERROR),
            WmsStates.SUCCEEDED: job.get('DAG_NodesDone', COUNT_ERROR),
            WmsStates.FAILED: job.get('DAG_NodesFailed', COUNT_ERROR),
            WmsStates.MISFIT: job.get('DAG_NodesPre', 0) + job.get('DAG_NodesPost', 0)}
        total_jobs = job.get('DAG_NodesTotal')
        _LOG.debug("_get_job_state_counts: from DAG_* keys, total_jobs = %s", total_jobs)
    elif 'NodesFailed' in job:
        state_counts = {
            WmsStates.UNREADY: job.get('NodesUnready', COUNT_ERROR),
            WmsStates.READY: job.get('NodesReady', COUNT_ERROR),
            WmsStates.HELD: job.get('JobProcsHeld', COUNT_ERROR),
            WmsStates.SUCCEEDED: job.get('NodesDone', COUNT_ERROR),
            WmsStates.FAILED: job.get('NodesFailed', COUNT_ERROR),
            WmsStates.MISFIT: job.get('NodesPre', 0) + job.get('NodesPost', 0)}
        try:
            total_jobs = job.get('NodesTotal')
        except KeyError as ex:
            _LOG.error("Job missing %s. job = %s", str(ex), job)
            raise
        _LOG.debug("_get_job_state_counts: from NODES* keys, total_jobs = %s", total_jobs)
    else:
        # With Kerberos job auth and Kerberos bug, if warning would be printed for every DAG
        _LOG.debug("Can't get job state counts %s", job['Iwd'])
        for state in WmsStates:
            if state not in state_counts:
                state_counts[state] = COUNT_ERROR
        total_jobs = COUNT_ERROR

    _LOG.debug("state_counts: %s", state_counts)

    # Fill in missing states
    for state in WmsStates:
        if state not in state_counts:
            state_counts[state] = 0

    _LOG.debug("total_jobs = %s, state_counts: %s", total_jobs, state_counts)
    return total_jobs, state_counts


def _htc_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `WmsStates`
        The equivalent WmsState to given job's status.
    """
    wms_state = WmsStates.MISFIT
    if "JobStatus" in job:
        wms_state = _htc_job_status_to_wms_state(job)
    elif "NodeStatus" in job:
        wms_state = _htc_node_status_to_wms_state(job)
    return wms_state


def _htc_job_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `WmsStates`
        The equivalent WmsState to given job's status.
    """
    _LOG.debug("htc_job_status_to_wms_state: %s=%s, %s", job["ClusterId"], job['JobStatus'],
               type(job['JobStatus']))
    job_status = int(job['JobStatus'])
    wms_state = WmsStates.MISFIT

    _LOG.debug("htc_job_status_to_wms_state: job_status = %s", job_status)
    if job_status == JobStatus.IDLE:
        wms_state = WmsStates.PENDING
    elif job_status == JobStatus.RUNNING:
        wms_state = WmsStates.RUNNING
    elif job_status == JobStatus.REMOVED:
        wms_state = WmsStates.DELETED
    elif job_status == JobStatus.COMPLETED:
        if job.get('ExitBySignal', False) or job.get('ExitCode', 0) or \
                job.get('ExitSignal', 0) or job.get('DAG_Status', 0) or \
                job.get('ReturnValue', 0):
            wms_state = WmsStates.FAILED
        else:
            wms_state = WmsStates.SUCCEEDED
    elif job_status == JobStatus.HELD:
        wms_state = WmsStates.HELD

    return wms_state


def _htc_node_status_to_wms_state(job):
    """Convert HTCondor status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `WmsStates`
        The equivalent WmsState to given node's status.
    """
    wms_state = WmsStates.MISFIT

    status = job["NodeStatus"]
    if status == NodeStatus.NOT_READY:
        wms_state = WmsStates.UNREADY
    elif status == NodeStatus.READY:
        wms_state = WmsStates.READY
    elif status == NodeStatus.PRERUN:
        wms_state = WmsStates.MISFIT
    elif status == NodeStatus.SUBMITTED:
        if job["JobProcsHeld"]:
            wms_state = WmsStates.HELD
        elif job["StatusDetails"] == "not_idle":
            wms_state = WmsStates.RUNNING
        elif job["JobProcsQueued"]:
            wms_state = WmsStates.PENDING
    elif status == NodeStatus.POSTRUN:
        wms_state = WmsStates.MISFIT
    elif status == NodeStatus.DONE:
        wms_state = WmsStates.SUCCEEDED
    elif status == NodeStatus.ERROR:
        wms_state = WmsStates.FAILED

    return wms_state


def update_jobs(jobs1, jobs2):
    """Update jobs1 with info in jobs2.

    (Basically an update for nested dictionaries.)

    Parameters
    ----------
    jobs1 : `dict` [`str`, `dict` [`str`, `Any`]]
        HTCondor job information to be updated.
    jobs2 : `dict` [`str`, `dict` [`str`, `Any`]]
        Additional HTCondor job information.
    """
    for jid, jinfo in jobs2.items():
        if jid in jobs1:
            jobs1[jid].update(jinfo)
        else:
            jobs1[jid] = jinfo
