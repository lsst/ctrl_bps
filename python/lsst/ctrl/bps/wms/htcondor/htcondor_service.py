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

"""Interface between generic workflow to HTCondor workflow system.
"""

__all__ = ["HTCondorService", "HTCondorWorkflow"]


import logging
import os
import re
from collections import defaultdict
from enum import IntEnum, auto
from pathlib import Path

import htcondor
from lsst.utils.timer import time_this
from packaging import version

from ... import (
    BaseWmsService,
    BaseWmsWorkflow,
    GenericWorkflow,
    GenericWorkflowJob,
    WmsJobReport,
    WmsRunReport,
    WmsStates,
)
from ...bps_utils import chdir, create_count_summary
from .lssthtc import (
    MISSING_ID,
    HTCDag,
    HTCJob,
    JobStatus,
    NodeStatus,
    condor_q,
    condor_search,
    condor_status,
    htc_backup_files,
    htc_check_dagman_output,
    htc_create_submit_from_cmd,
    htc_create_submit_from_dag,
    htc_create_submit_from_file,
    htc_escape,
    htc_submit_dag,
    htc_version,
    pegasus_name_to_label,
    read_dag_info,
    read_dag_log,
    read_dag_status,
    read_node_status,
    summary_from_dag,
    write_dag_info,
)


class WmsIdType(IntEnum):
    """Type of valid WMS ids."""

    UNKNOWN = auto()
    """The type of id cannot be determined.
    """

    LOCAL = auto()
    """The id is HTCondor job's ClusterId (with optional '.ProcId').
    """

    GLOBAL = auto()
    """Id is a HTCondor's global job id.
    """

    PATH = auto()
    """Id is a submission path.
    """


DEFAULT_HTC_EXEC_PATT = ".*worker.*"
"""Default pattern for searching execute machines in an HTCondor pool.
"""

_LOG = logging.getLogger(__name__)


class HTCondorService(BaseWmsService):
    """HTCondor version of WMS service."""

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an HTCondor DAG ready for submission.

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration that includes necessary submit/runtime
            information.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments).
        out_prefix : `str`
            The root directory into which all WMS-specific files are written.

        Returns
        ----------
        workflow : `lsst.ctrl.bps.wms.htcondor.HTCondorWorkflow`
            HTCondor workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed HTCondor workflow creation"):
            workflow = HTCondorWorkflow.from_generic_workflow(
                config,
                generic_workflow,
                out_prefix,
                f"{self.__class__.__module__}." f"{self.__class__.__name__}",
            )

        with time_this(
            log=_LOG, level=logging.INFO, prefix=None, msg="Completed writing out HTCondor workflow"
        ):
            workflow.write(out_prefix)
        return workflow

    def submit(self, workflow):
        """Submit a single HTCondor workflow.

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWorkflow`
            A single HTCondor workflow to submit.  run_id is updated after
            successful submission to WMS.
        """
        dag = workflow.dag

        ver = version.parse(htc_version())
        if ver >= version.parse("8.9.3"):
            sub = htc_create_submit_from_dag(dag.graph["dag_filename"], {})
        else:
            sub = htc_create_submit_from_cmd(dag.graph["dag_filename"], {})

        # For workflow portability, internal paths are all relative. Hence
        # the DAG needs to be submitted to HTCondor from inside the submit
        # directory.
        with chdir(workflow.submit_path):
            _LOG.info("Submitting from directory: %s", os.getcwd())
            schedd_dag_info = htc_submit_dag(sub)
            if schedd_dag_info:
                write_dag_info(f"{dag.name}.info.json", schedd_dag_info)

                _, dag_info = schedd_dag_info.popitem()
                _, dag_ad = dag_info.popitem()

                dag.run_id = f"{dag_ad['ClusterId']}.{dag_ad['ProcId']}"
                workflow.run_id = dag.run_id
            else:
                raise RuntimeError("Submission failed: unable to retrieve DAGMan job information")

    def restart(self, wms_workflow_id):
        """Restart a failed DAGMan workflow.

        Parameters
        ----------
        wms_workflow_id : `str`
            The directory with HTCondor files.

        Returns
        -------
        run_id : `str`
            HTCondor id of the restarted DAGMan job. If restart failed, it will
            be set to None.
        run_name : `str`
            Name of the restarted workflow. If restart failed, it will be set
            to None.
        message : `str`
            A message describing any issues encountered during the restart.
            If there were no issues, an empty string is returned.
        """
        wms_path = Path(wms_workflow_id)
        if not wms_path.is_dir():
            return None, None, f"Directory '{wms_path}' not found"

        _LOG.info("Restarting workflow from directory '%s'", wms_path)
        rescue_dags = list(wms_path.glob("*.dag.rescue*"))
        if not rescue_dags:
            return None, None, f"HTCondor rescue DAG(s) not found in '{wms_path}'"

        _LOG.info("Verifying that the workflow is not already in the job queue")
        schedd_dag_info = condor_q(constraint=f'regexp("dagman$", Cmd) && Iwd == "{wms_workflow_id}"')
        if schedd_dag_info:
            _, dag_info = schedd_dag_info.popitem()
            _, dag_ad = dag_info.popitem()
            id_ = dag_ad["GlobalJobId"]
            return None, None, f"Workflow already in the job queue (global job id: '{id_}')"

        _LOG.info("Checking execution status of the workflow")
        warn = False
        dag_ad = read_dag_status(str(wms_path))
        if dag_ad:
            nodes_total = dag_ad.get("NodesTotal", 0)
            if nodes_total != 0:
                nodes_done = dag_ad.get("NodesDone", 0)
                if nodes_total == nodes_done:
                    return None, None, "All jobs in the workflow finished successfully"
            else:
                warn = True
        else:
            warn = True
        if warn:
            _LOG.warning(
                "Cannot determine the execution status of the workflow, continuing with restart regardless"
            )

        _LOG.info("Backing up select HTCondor files from previous run attempt")
        htc_backup_files(wms_path, subdir="backups")

        # For workflow portability, internal paths are all relative. Hence
        # the DAG needs to be resubmitted to HTCondor from inside the submit
        # directory.
        _LOG.info("Adding workflow to the job queue")
        run_id, run_name, message = None, None, ""
        with chdir(wms_path):
            try:
                dag_path = next(wms_path.glob("*.dag.condor.sub"))
            except StopIteration:
                message = f"DAGMan submit description file not found in '{wms_path}'"
            else:
                sub = htc_create_submit_from_file(dag_path.name)
                schedd_dag_info = htc_submit_dag(sub)

                # Save select information about the DAGMan job to a file. Use
                # the run name (available in the ClassAd) as the filename.
                if schedd_dag_info:
                    dag_info = next(iter(schedd_dag_info.values()))
                    dag_ad = next(iter(dag_info.values()))
                    write_dag_info(f"{dag_ad['bps_run']}.info.json", schedd_dag_info)
                    run_id = f"{dag_ad['ClusterId']}.{dag_ad['ProcId']}"
                    run_name = dag_ad["bps_run"]
                else:
                    message = "DAGMan job information unavailable"

        return run_id, run_name, message

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
            If set, all job queues (and their histories) will be queried for
            job information. Defaults to False which means that only the local
            job queue will be queried.

        Returns
        -------
        job_ids : `list` [`Any`]
            Only job ids to be used by cancel and other functions.  Typically
            this means top-level jobs (i.e., not children jobs).
        """
        _LOG.debug(
            "list_submitted_jobs params: wms_id=%s, user=%s, require_bps=%s, pass_thru=%s, is_global=%s",
            wms_id,
            user,
            require_bps,
            pass_thru,
            is_global,
        )

        # Determine which Schedds will be queried for job information.
        coll = htcondor.Collector()

        schedd_ads = []
        if is_global:
            schedd_ads.extend(coll.locateAll(htcondor.DaemonTypes.Schedd))
        else:
            schedd_ads.append(coll.locate(htcondor.DaemonTypes.Schedd))

        # Construct appropriate constraint expression using provided arguments.
        constraint = "False"
        if wms_id is None:
            if user is not None:
                constraint = f'(Owner == "{user}")'
        else:
            schedd_ad, cluster_id, id_type = _wms_id_to_cluster(wms_id)
            if cluster_id is not None:
                constraint = f"(DAGManJobId == {cluster_id} || ClusterId == {cluster_id})"

                # If provided id is either a submission path or a global id,
                # make sure the right Schedd will be queried regardless of
                # 'is_global' value.
                if id_type in {WmsIdType.GLOBAL, WmsIdType.PATH}:
                    schedd_ads = [schedd_ad]
        if require_bps:
            constraint += ' && (bps_isjob == "True")'
        if pass_thru:
            if "-forcex" in pass_thru:
                pass_thru_2 = pass_thru.replace("-forcex", "")
                if pass_thru_2 and not pass_thru_2.isspace():
                    constraint += f" && ({pass_thru_2})"
            else:
                constraint += f" && ({pass_thru})"

        # Create a list of scheduler daemons which need to be queried.
        schedds = {ad["Name"]: htcondor.Schedd(ad) for ad in schedd_ads}

        _LOG.debug("constraint = %s, schedds = %s", constraint, ", ".join(schedds))
        results = condor_q(constraint=constraint, schedds=schedds)

        # Prune child jobs where DAG job is in queue (i.e., aren't orphans).
        job_ids = []
        for schedd_name, job_info in results.items():
            for job_id, job_ad in job_info.items():
                _LOG.debug("job_id=%s DAGManJobId=%s", job_id, job_ad.get("DAGManJobId", "None"))
                if "DAGManJobId" not in job_ad:
                    job_ids.append(job_ad.get("GlobalJobId", job_id))
                else:
                    _LOG.debug("Looking for %s", f"{job_ad['DAGManJobId']}.0")
                    _LOG.debug("\tin jobs.keys() = %s", job_info.keys())
                    if f"{job_ad['DAGManJobId']}.0" not in job_info:  # orphaned job
                        job_ids.append(job_ad.get("GlobalJobId", job_id))

        _LOG.debug("job_ids = %s", job_ids)
        return job_ids

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None, is_global=False):
        """Return run information based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `str`, optional
            Limit to specific run based on id.
        user : `str`, optional
            Limit results to runs for this user.
        hist : `float`, optional
            Limit history search to this many days. Defaults to 0.
        pass_thru : `str`, optional
            Constraints to pass through to HTCondor.
        is_global : `bool`, optional
            If set, all job queues (and their histories) will be queried for
            job information. Defaults to False which means that only the local
            job queue will be queried.

        Returns
        -------
        runs : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be pointers
            to documentation or to WMS specific commands.
        """
        if wms_workflow_id:
            id_type = _wms_id_type(wms_workflow_id)
            if id_type == WmsIdType.LOCAL:
                schedulers = _locate_schedds(locate_all=is_global)
                run_reports, message = _report_from_id(wms_workflow_id, hist, schedds=schedulers)
            elif id_type == WmsIdType.GLOBAL:
                schedulers = _locate_schedds(locate_all=True)
                run_reports, message = _report_from_id(wms_workflow_id, hist, schedds=schedulers)
            elif id_type == WmsIdType.PATH:
                run_reports, message = _report_from_path(wms_workflow_id)
            else:
                run_reports, message = {}, "Invalid job id"
        else:
            schedulers = _locate_schedds(locate_all=is_global)
            run_reports, message = _summary_report(user, hist, pass_thru, schedds=schedulers)
        _LOG.debug("report: %s, %s", run_reports, message)

        return list(run_reports.values()), message

    def cancel(self, wms_id, pass_thru=None):
        """Cancel submitted workflows/jobs.

        Parameters
        ----------
        wms_id : `str`
            Id or path of job that should be canceled.
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        --------
        deleted : `bool`
            Whether successful deletion or not.  Currently, if any doubt or any
            individual jobs not deleted, return False.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        _LOG.debug("Canceling wms_id = %s", wms_id)

        schedd_ad, cluster_id, _ = _wms_id_to_cluster(wms_id)

        if cluster_id is None:
            deleted = False
            message = "invalid id"
        else:
            _LOG.debug(
                "Canceling job managed by schedd_name = %s with cluster_id = %s",
                cluster_id,
                schedd_ad["Name"],
            )
            schedd = htcondor.Schedd(schedd_ad)

            constraint = f"ClusterId == {cluster_id}"
            if pass_thru is not None and "-forcex" in pass_thru:
                pass_thru_2 = pass_thru.replace("-forcex", "")
                if pass_thru_2 and not pass_thru_2.isspace():
                    constraint += f"&& ({pass_thru_2})"
                _LOG.debug("JobAction.RemoveX constraint = %s", constraint)
                results = schedd.act(htcondor.JobAction.RemoveX, constraint)
            else:
                if pass_thru:
                    constraint += f"&& ({pass_thru})"
                _LOG.debug("JobAction.Remove constraint = %s", constraint)
                results = schedd.act(htcondor.JobAction.Remove, constraint)
            _LOG.debug("Remove results: %s", results)

            if results["TotalSuccess"] > 0 and results["TotalError"] == 0:
                deleted = True
                message = ""
            else:
                deleted = False
                if results["TotalSuccess"] == 0 and results["TotalError"] == 0:
                    message = "no such bps job in batch queue"
                else:
                    message = f"unknown problems deleting: {results}"

        _LOG.debug("deleted: %s; message = %s", deleted, message)
        return deleted, message


class HTCondorWorkflow(BaseWmsWorkflow):
    """Single HTCondor workflow.

    Parameters
    ----------
    name : `str`
        Unique name for Workflow used when naming files.
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information.
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
        htc_workflow.dag.add_attribs(
            {
                "bps_wms_service": service_class,
                "bps_wms_workflow": f"{cls.__module__}.{cls.__name__}",
                "bps_run_quanta": create_count_summary(generic_workflow.quanta_counts),
                "bps_job_summary": create_count_summary(generic_workflow.job_counts),
            }
        )

        _, tmp_template = config.search("subDirTemplate", opt={"replaceVars": False, "default": ""})
        if isinstance(tmp_template, str):
            subdir_template = defaultdict(lambda: tmp_template)
        else:
            subdir_template = tmp_template

        # Create all DAG jobs
        site_values = {}  # cache compute site specific values to reduce config lookups
        for job_name in generic_workflow:
            gwjob = generic_workflow.get_job(job_name)
            if gwjob.compute_site not in site_values:
                site_values[gwjob.compute_site] = _gather_site_values(config, gwjob.compute_site)
            htc_job = _create_job(
                subdir_template[gwjob.label],
                site_values[gwjob.compute_site],
                generic_workflow,
                gwjob,
                out_prefix,
            )
            htc_workflow.dag.add_job(htc_job)

        # Add job dependencies to the DAG
        for job_name in generic_workflow:
            htc_workflow.dag.add_job_relationships([job_name], generic_workflow.successors(job_name))

        # If final job exists in generic workflow, create DAG final job
        final = generic_workflow.get_final()
        if final and isinstance(final, GenericWorkflowJob):
            if final.compute_site and final.compute_site not in site_values:
                site_values[final.compute_site] = _gather_site_values(config, final.compute_site)
            final_htjob = _create_job(
                subdir_template[final.label],
                site_values[final.compute_site],
                generic_workflow,
                final,
                out_prefix,
            )
            if "post" not in final_htjob.dagcmds:
                final_htjob.dagcmds["post"] = (
                    f"{os.path.dirname(__file__)}/final_post.sh" f" {final.name} $DAG_STATUS $RETURN"
                )
            htc_workflow.dag.add_final_job(final_htjob)
        elif final and isinstance(final, GenericWorkflow):
            raise NotImplementedError("HTCondor plugin does not support a workflow as the final job")
        elif final:
            return TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final)})")

        return htc_workflow

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


def _create_job(subdir_template, site_values, generic_workflow, gwjob, out_prefix):
    """Convert GenericWorkflow job nodes to DAG jobs.

    Parameters
    ----------
    subdir_template : `str`
        Template for making subdirs.
    site_values : `dict`
        Site specific values
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that is being converted.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        The generic job to convert to a HTCondor job.
    out_prefix : `str`
        Directory prefix for HTCondor files.

    Returns
    -------
    htc_job : `lsst.ctrl.bps.wms.htcondor.HTCJob`
        The HTCondor job equivalent to the given generic job.
    """
    htc_job = HTCJob(gwjob.name, label=gwjob.label)

    curvals = defaultdict(str)
    curvals["label"] = gwjob.label
    if gwjob.tags:
        curvals.update(gwjob.tags)

    subdir = subdir_template.format_map(curvals)
    htc_job.subfile = Path("jobs") / subdir / f"{gwjob.name}.sub"

    htc_job_cmds = {
        "universe": "vanilla",
        "should_transfer_files": "YES",
        "when_to_transfer_output": "ON_EXIT_OR_EVICT",
        "transfer_output_files": '""',  # Set to empty string to disable
        "transfer_executable": "False",
        "getenv": "True",
        # Exceeding memory sometimes triggering SIGBUS or SIGSEGV error. Tell
        # htcondor to put on hold any jobs which exited by a signal.
        "on_exit_hold": "ExitBySignal == true",
        "on_exit_hold_reason": 'strcat("Job raised a signal ", string(ExitSignal), ". ", '
        '"Handling signal as if job has gone over memory limit.")',
        "on_exit_hold_subcode": "34",
    }

    htc_job_cmds.update(_translate_job_cmds(site_values, generic_workflow, gwjob))

    # job stdout, stderr, htcondor user log.
    for key in ("output", "error", "log"):
        htc_job_cmds[key] = htc_job.subfile.with_suffix(f".$(Cluster).{key[:3]}")
        _LOG.debug("HTCondor %s = %s", key, htc_job_cmds[key])

    htc_job_cmds.update(
        _handle_job_inputs(generic_workflow, gwjob.name, site_values["bpsUseShared"], out_prefix)
    )

    # Add the job cmds dict to the job object.
    htc_job.add_job_cmds(htc_job_cmds)

    htc_job.add_dag_cmds(_translate_dag_cmds(gwjob))

    # Add job attributes to job.
    _LOG.debug("gwjob.attrs = %s", gwjob.attrs)
    htc_job.add_job_attrs(gwjob.attrs)
    htc_job.add_job_attrs(site_values["attrs"])
    htc_job.add_job_attrs({"bps_job_quanta": create_count_summary(gwjob.quanta_counts)})
    htc_job.add_job_attrs({"bps_job_name": gwjob.name, "bps_job_label": gwjob.label})

    return htc_job


def _translate_job_cmds(cached_vals, generic_workflow, gwjob):
    """Translate the job data that are one to one mapping

    Parameters
    ----------
    cached_vals : `dict` [`str`, `Any`]
        Config values common to jobs with same label.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
       Generic workflow that contains job to being converted.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
       Generic workflow job to be converted.

    Returns
    -------
    htc_job_commands : `dict` [`str`, `Any`]
        Contains commands which can appear in the HTCondor submit description
        file.
    """
    # Values in the job script that just are name mappings.
    job_translation = {
        "mail_to": "notify_user",
        "when_to_mail": "notification",
        "request_cpus": "request_cpus",
        "priority": "priority",
        "category": "category",
        "accounting_group": "accounting_group",
        "accounting_user": "accounting_group_user",
    }

    jobcmds = {}
    for gwkey, htckey in job_translation.items():
        jobcmds[htckey] = getattr(gwjob, gwkey, None)

    # If accounting info was not set explicitly, use site settings if any.
    if not gwjob.accounting_group:
        jobcmds["accounting_group"] = cached_vals.get("accountingGroup")
    if not gwjob.accounting_user:
        jobcmds["accounting_group_user"] = cached_vals.get("accountingUser")

    # job commands that need modification
    if gwjob.number_of_retries:
        jobcmds["max_retries"] = f"{gwjob.number_of_retries}"

    if gwjob.retry_unless_exit:
        jobcmds["retry_until"] = f"{gwjob.retry_unless_exit}"

    if gwjob.request_disk:
        jobcmds["request_disk"] = f"{gwjob.request_disk}MB"

    if gwjob.request_memory:
        jobcmds["request_memory"] = f"{gwjob.request_memory}"

    if gwjob.memory_multiplier:
        # Do not use try-except! At the moment, BpsConfig returns an empty
        # string if it does not contain the key.
        memory_limit = cached_vals["memoryLimit"]
        if not memory_limit:
            raise RuntimeError(
                "Memory autoscaling enabled, but automatic detection of the memory limit "
                "failed; setting it explicitly with 'memoryLimit' or changing worker node "
                "search pattern 'executeMachinesPattern' might help."
            )

        # Set maximal amount of memory job can ask for.
        #
        # The check below assumes that 'memory_limit' was set to a value which
        # realistically reflects actual physical limitations of a given compute
        # resource.
        memory_max = memory_limit
        if gwjob.request_memory_max and gwjob.request_memory_max < memory_limit:
            memory_max = gwjob.request_memory_max

        # Make job ask for more memory each time it failed due to insufficient
        # memory requirements.
        jobcmds["request_memory"] = _create_request_memory_expr(
            gwjob.request_memory, gwjob.memory_multiplier, memory_max
        )

        # Periodically release jobs which are being held due to exceeding
        # memory. Stop doing that (by removing the job from the HTCondor queue)
        # after the maximal number of retries has been reached or the job was
        # already run at maximal allowed memory.
        jobcmds["periodic_release"] = _create_periodic_release_expr(
            gwjob.request_memory, gwjob.memory_multiplier, memory_max
        )
        jobcmds["periodic_remove"] = _create_periodic_remove_expr(
            gwjob.request_memory, gwjob.memory_multiplier, memory_max
        )

    # Assume concurrency_limit implemented using HTCondor concurrency limits.
    # May need to move to special site-specific implementation if sites use
    # other mechanisms.
    if gwjob.concurrency_limit:
        jobcmds["concurrency_limit"] = gwjob.concurrency_limit

    # Handle command line
    if gwjob.executable.transfer_executable:
        jobcmds["transfer_executable"] = "True"
        jobcmds["executable"] = os.path.basename(gwjob.executable.src_uri)
    else:
        jobcmds["executable"] = _fix_env_var_syntax(gwjob.executable.src_uri)

    if gwjob.arguments:
        arguments = gwjob.arguments
        arguments = _replace_cmd_vars(arguments, gwjob)
        arguments = _replace_file_vars(cached_vals["bpsUseShared"], arguments, generic_workflow, gwjob)
        arguments = _fix_env_var_syntax(arguments)
        jobcmds["arguments"] = arguments

    # Add extra "pass-thru" job commands
    if gwjob.profile:
        for key, val in gwjob.profile.items():
            jobcmds[key] = htc_escape(val)
    for key, val in cached_vals["profile"].items():
        jobcmds[key] = htc_escape(val)

    return jobcmds


def _translate_dag_cmds(gwjob):
    """Translate job values into DAGMan commands.

    Parameters
    ----------
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job containing values to be translated.

    Returns
    -------
    dagcmds : `dict` [`str`, `Any`]
        DAGMan commands for the job.
    """
    # Values in the dag script that just are name mappings.
    dag_translation = {"abort_on_value": "abort_dag_on", "abort_return_value": "abort_exit"}

    dagcmds = {}
    for gwkey, htckey in dag_translation.items():
        dagcmds[htckey] = getattr(gwjob, gwkey, None)

    # Still to be coded: vars "pre_cmdline", "post_cmdline"
    return dagcmds


def _fix_env_var_syntax(oldstr):
    """Change ENV place holders to HTCondor Env var syntax.

    Parameters
    ----------
    oldstr : `str`
        String in which environment variable syntax is to be fixed.

    Returns
    -------
    newstr : `str`
        Given string with environment variable syntax fixed.
    """
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", f"$ENV({key})")
    return newstr


def _replace_file_vars(use_shared, arguments, workflow, gwjob):
    """Replace file placeholders in command line arguments with correct
    physical file names.

    Parameters
    ----------
    use_shared : `bool`
        Whether HTCondor can assume shared filesystem.
    arguments : `str`
        Arguments string in which to replace file placeholders.
    workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains file information.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        The job corresponding to the arguments.

    Returns
    -------
    arguments : `str`
        Given arguments string with file placeholders replaced.
    """
    # Replace input file placeholders with paths.
    for gwfile in workflow.get_job_inputs(gwjob.name, data=True, transfer_only=False):
        if not gwfile.wms_transfer:
            # Must assume full URI if in command line and told WMS is not
            # responsible for transferring file.
            uri = gwfile.src_uri
        elif use_shared:
            if gwfile.job_shared:
                # Have shared filesystems and jobs can share file.
                uri = gwfile.src_uri
            else:
                # Taking advantage of inside knowledge.  Not future-proof.
                # Temporary fix until have job wrapper that pulls files
                # within job.
                if gwfile.name == "butlerConfig" and Path(gwfile.src_uri).suffix != ".yaml":
                    uri = "butler.yaml"
                else:
                    uri = os.path.basename(gwfile.src_uri)
        else:  # Using push transfer
            uri = os.path.basename(gwfile.src_uri)
        arguments = arguments.replace(f"<FILE:{gwfile.name}>", uri)

    # Replace output file placeholders with paths.
    for gwfile in workflow.get_job_outputs(gwjob.name, data=True, transfer_only=False):
        if not gwfile.wms_transfer:
            # Must assume full URI if in command line and told WMS is not
            # responsible for transferring file.
            uri = gwfile.src_uri
        elif use_shared:
            if gwfile.job_shared:
                # Have shared filesystems and jobs can share file.
                uri = gwfile.src_uri
            else:
                uri = os.path.basename(gwfile.src_uri)
        else:  # Using push transfer
            uri = os.path.basename(gwfile.src_uri)
        arguments = arguments.replace(f"<FILE:{gwfile.name}>", uri)
    return arguments


def _replace_cmd_vars(arguments, gwjob):
    """Replace format-style placeholders in arguments.

    Parameters
    ----------
    arguments : `str`
        Arguments string in which to replace placeholders.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job containing values to be used to replace placeholders
        (in particular gwjob.cmdvals).

    Returns
    -------
    arguments : `str`
        Given arguments string with placeholders replaced.
    """
    try:
        arguments = arguments.format(**gwjob.cmdvals)
    except (KeyError, TypeError):  # TypeError in case None instead of {}
        _LOG.error(
            "Could not replace command variables:\narguments: %s\ncmdvals: %s", arguments, gwjob.cmdvals
        )
        raise
    return arguments


def _handle_job_inputs(generic_workflow: GenericWorkflow, job_name: str, use_shared: bool, out_prefix: str):
    """Add job input files from generic workflow to job.

    Parameters
    ----------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow (e.g., has executable name and arguments).
    job_name : `str`
        Unique name for the job.
    use_shared : `bool`
        Whether job has access to files via shared filesystem.
    out_prefix : `str`
        The root directory into which all WMS-specific files are written.

    Returns
    -------
    htc_commands : `dict` [`str`, `str`]
        HTCondor commands for the job submission script.
    """
    htc_commands = {}
    inputs = []
    for gwf_file in generic_workflow.get_job_inputs(job_name, data=True, transfer_only=True):
        _LOG.debug("src_uri=%s", gwf_file.src_uri)

        uri = Path(gwf_file.src_uri)

        # Note if use_shared and job_shared, don't need to transfer file.

        if not use_shared:  # Copy file using push to job
            inputs.append(str(uri.relative_to(out_prefix)))
        elif not gwf_file.job_shared:  # Jobs require own copy

            # if using shared filesystem, but still need copy in job. Use
            # HTCondor's curl plugin for a local copy.

            # Execution butler is represented as a directory which the
            # curl plugin does not handle. Taking advantage of inside
            # knowledge for temporary fix until have job wrapper that pulls
            # files within job.
            if gwf_file.name == "butlerConfig":
                # The execution butler directory doesn't normally exist until
                # the submit phase so checking for suffix instead of using
                # is_dir().  If other non-yaml file exists they would have a
                # different gwf_file.name.
                if uri.suffix == ".yaml":  # Single file, so just copy.
                    inputs.append(f"file://{uri}")
                else:
                    inputs.append(f"file://{uri / 'butler.yaml'}")
                    inputs.append(f"file://{uri / 'gen3.sqlite3'}")
            elif uri.is_dir():
                raise RuntimeError(
                    "HTCondor plugin cannot transfer directories locally within job " f"{gwf_file.src_uri}"
                )
            else:
                inputs.append(f"file://{uri}")

    if inputs:
        htc_commands["transfer_input_files"] = ",".join(inputs)
        _LOG.debug("transfer_input_files=%s", htc_commands["transfer_input_files"])
    return htc_commands


def _report_from_path(wms_path):
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_path : `str`
        The directory containing the submit side files (e.g., HTCondor files).

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
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


def _report_from_id(wms_workflow_id, hist, schedds=None):
    """Gather run information using workflow id.

    Parameters
    ----------
    wms_workflow_id : `str`
        Limit to specific run based on id.
    hist : `float`
        Limit history search to this many days.
    schedds : `dict` [ `str`, `htcondor.Schedd` ], optional
        HTCondor schedulers which to query for job information. If None
        (default), all queries will be run against the local scheduler only.

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the detailed report.  The key is the HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    dag_constraint = 'regexp("dagman$", Cmd)'
    try:
        cluster_id = int(float(wms_workflow_id))
    except ValueError:
        dag_constraint += f' && GlobalJobId == "{wms_workflow_id}"'
    else:
        dag_constraint += f" && ClusterId == {cluster_id}"

    # With the current implementation of the condor_* functions the query will
    # always return only one match per Scheduler.
    #
    # Even in the highly unlikely situation where HTCondor history (which
    # condor_search queries too) is long enough to have jobs from before the
    # cluster ids were rolled over (and as a result there is more then one job
    # with the same cluster id) they will not show up in the results.
    schedd_dag_info = condor_search(constraint=dag_constraint, hist=hist, schedds=schedds)
    if len(schedd_dag_info) == 0:
        run_reports = {}
        message = ""
    elif len(schedd_dag_info) == 1:
        _, dag_info = schedd_dag_info.popitem()
        dag_id, dag_ad = dag_info.popitem()

        # Create a mapping between jobs and their classads. The keys will be
        # of format 'ClusterId.ProcId'.
        job_info = {dag_id: dag_ad}

        # Find jobs (nodes) belonging to that DAGMan job.
        job_constraint = f"DAGManJobId == {int(float(dag_id))}"
        schedd_job_info = condor_search(constraint=job_constraint, hist=hist, schedds=schedds)
        if schedd_job_info:
            _, node_info = schedd_job_info.popitem()
            job_info.update(node_info)

        # Collect additional pieces of information about jobs using HTCondor
        # files in the submission directory.
        _, path_jobs, message = _get_info_from_path(dag_ad["Iwd"])
        _update_jobs(job_info, path_jobs)

        run_reports = _create_detailed_report_from_jobs(dag_id, job_info)
    else:
        ids = [ad["GlobalJobId"] for dag_info in schedd_dag_info.values() for ad in dag_info.values()]
        run_reports = {}
        message = (
            f"More than one job matches id '{wms_workflow_id}', "
            f"their global ids are: {', '.join(ids)}. Rerun with one of the global ids"
        )
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
    messages = []
    try:
        wms_workflow_id, jobs = read_dag_log(wms_path)
        _LOG.debug("_get_info_from_path: from dag log %s = %s", wms_workflow_id, jobs)
        _update_jobs(jobs, read_node_status(wms_path))
        _LOG.debug("_get_info_from_path: after node status %s = %s", wms_workflow_id, jobs)

        # Add more info for DAGman job
        job = jobs[wms_workflow_id]
        job.update(read_dag_status(wms_path))

        job["total_jobs"], job["state_counts"] = _get_state_counts_from_jobs(wms_workflow_id, jobs)
        if "bps_run" not in job:
            _add_run_info(wms_path, job)

        message = htc_check_dagman_output(wms_path)
        if message:
            messages.append(message)
        _LOG.debug(
            "_get_info: id = %s, total_jobs = %s", wms_workflow_id, jobs[wms_workflow_id]["total_jobs"]
        )

        # Add extra pieces of information which cannot be found in HTCondor
        # generated files like 'GlobalJobId'.
        #
        # Do not treat absence of this file as a serious error. Neither runs
        # submitted with earlier versions of the plugin nor the runs submitted
        # with Pegasus plugin will have it at the moment. However, once enough
        # time passes and Pegasus plugin will have its own report() method
        # (instead of sneakily using HTCondor's one), the lack of that file
        # should be treated as seriously as lack of any other file.
        try:
            job_info = read_dag_info(wms_path)
        except FileNotFoundError as exc:
            message = f"Warn: Some information may not be available: {exc}"
            messages.append(message)
        else:
            schedd_name = next(iter(job_info))
            job_ad = next(iter(job_info[schedd_name].values()))
            job.update(job_ad)
    except FileNotFoundError:
        message = f"Could not find HTCondor files in '{wms_path}'"
        _LOG.warning(message)
        messages.append(message)
        wms_workflow_id = MISSING_ID
        jobs = {}

    message = "\n".join([msg for msg in messages if msg])
    return wms_workflow_id, jobs, message


def _create_detailed_report_from_jobs(wms_workflow_id, jobs):
    """Gather run information to be used in generating summary reports.

    Parameters
    ----------
    wms_workflow_id : `str`
        The run id to create the report for.
    jobs : `dict` [`str`, `dict` [`str`, Any]]
        Mapping HTCondor job id to job information.

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the detailed report.  The key is the given HTCondor
        id and the value is a collection of report information for that run.
    """
    _LOG.debug("_create_detailed_report: id = %s, job = %s", wms_workflow_id, jobs[wms_workflow_id])
    dag_job = jobs[wms_workflow_id]
    report = WmsRunReport(
        wms_id=f"{dag_job['ClusterId']}.{dag_job['ProcId']}",
        global_wms_id=dag_job.get("GlobalJobId", "MISS"),
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
        total_number_jobs=dag_job["total_jobs"],
        job_state_counts=dag_job["state_counts"],
    )

    for job_id, job_info in jobs.items():
        try:
            if job_info["ClusterId"] != int(float(wms_workflow_id)):
                job_report = WmsJobReport(
                    wms_id=job_id,
                    name=job_info.get("DAGNodeName", job_id),
                    label=job_info.get("bps_job_label", pegasus_name_to_label(job_info["DAGNodeName"])),
                    state=_htc_status_to_wms_state(job_info),
                )
                if job_report.label == "init":
                    job_report.label = "pipetaskInit"
                report.jobs.append(job_report)
        except KeyError as ex:
            _LOG.error("Job missing key '%s': %s", str(ex), job_info)
            raise

    run_reports = {report.wms_id: report}
    _LOG.debug("_create_detailed_report: run_reports = %s", run_reports)
    return run_reports


def _summary_report(user, hist, pass_thru, schedds=None):
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
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the summary report.  The keys are HTCondor ids and
        the values are collections of report information for each run.
    message : `str`
        Message to be printed with the summary report.
    """
    # only doing summary report so only look for dagman jobs
    if pass_thru:
        constraint = pass_thru
    else:
        # Notes:
        # * bps_isjob == 'True' isn't getting set for DAG jobs that are
        #   manually restarted.
        # * Any job with DAGManJobID isn't a DAG job
        constraint = 'bps_isjob == "True" && JobUniverse == 7'
        if user:
            constraint += f' && (Owner == "{user}" || bps_operator == "{user}")'

    job_info = condor_search(constraint=constraint, hist=hist, schedds=schedds)

    # Have list of DAGMan jobs, need to get run_report info.
    run_reports = {}
    for jobs in job_info.values():
        for job_id, job in jobs.items():
            total_jobs, state_counts = _get_state_counts_from_dag_job(job)
            # If didn't get from queue information (e.g., Kerberos bug),
            # try reading from file.
            if total_jobs == 0:
                try:
                    job.update(read_dag_status(job["Iwd"]))
                    total_jobs, state_counts = _get_state_counts_from_dag_job(job)
                except StopIteration:
                    pass  # don't kill report can't find htcondor files

            if "bps_run" not in job:
                _add_run_info(job["Iwd"], job)
            report = WmsRunReport(
                wms_id=job_id,
                global_wms_id=job["GlobalJobId"],
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
                job_state_counts=state_counts,
            )
            run_reports[report.global_wms_id] = report

    return run_reports, ""


def _add_run_info(wms_path, job):
    """Find BPS run information elsewhere for runs without bps attributes.

    Parameters
    ----------
    wms_path : `str`
        Path to submit files for the run.
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Raises
    ------
    StopIteration
        If cannot find file it is looking for.  Permission errors are
        caught and job's run is marked with error.
    """
    path = Path(wms_path) / "jobs"
    try:
        subfile = next(path.glob("**/*.sub"))
    except (StopIteration, PermissionError):
        job["bps_run"] = "Unavailable"
    else:
        _LOG.debug("_add_run_info: subfile = %s", subfile)
        try:
            with open(subfile, "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("+bps_"):
                        m = re.match(r"\+(bps_[^\s]+)\s*=\s*(.+)$", line)
                        if m:
                            _LOG.debug("Matching line: %s", line)
                            job[m.group(1)] = m.group(2).replace('"', "")
                        else:
                            _LOG.debug("Could not parse attribute: %s", line)
        except PermissionError:
            job["bps_run"] = "PermissionError"
    _LOG.debug("After adding job = %s", job)


def _get_owner(job):
    """Get the owner of a dag job.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
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
    summary = job.get("bps_job_summary", job.get("bps_run_summary", None))
    if not summary:
        summary, _ = summary_from_dag(job["Iwd"])
        if not summary:
            _LOG.warning("Could not get run summary for htcondor job: %s", job)
    _LOG.debug("_get_run_summary: summary=%s", summary)

    # Workaround sometimes using init vs pipetaskInit
    summary = summary.replace("init:", "pipetaskInit:")

    if "pegasus_version" in job and "pegasus" not in summary:
        summary += ";pegasus:0"

    return summary


def _get_state_counts_from_jobs(wms_workflow_id, jobs):
    """Count number of jobs per WMS state.

    Parameters
    ----------
    wms_workflow_id : `str`
        HTCondor job id.
    jobs : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    total_count : `int`
        Total number of dag nodes.
    state_counts : `dict` [`lsst.ctrl.bps.WmsStates`, `int`]
        Keys are the different WMS states and values are counts of jobs
        that are in that WMS state.
    """
    state_counts = dict.fromkeys(WmsStates, 0)

    for jid, jinfo in jobs.items():
        if jid != wms_workflow_id:
            state_counts[_htc_status_to_wms_state(jinfo)] += 1

    total_counted = sum(state_counts.values())
    if "NodesTotal" in jobs[wms_workflow_id]:
        total_count = jobs[wms_workflow_id]["NodesTotal"]
    else:
        total_count = total_counted

    state_counts[WmsStates.UNREADY] += total_count - total_counted

    return total_count, state_counts


def _get_state_counts_from_dag_job(job):
    """Count number of jobs per WMS state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    total_count : `int`
        Total number of dag nodes.
    state_counts : `dict` [`lsst.ctrl.bps.WmsStates`, `int`]
        Keys are the different WMS states and values are counts of jobs
        that are in that WMS state.
    """
    _LOG.debug("_get_state_counts_from_dag_job: job = %s %s", type(job), len(job))
    state_counts = dict.fromkeys(WmsStates, 0)
    if "DAG_NodesReady" in job:
        state_counts = {
            WmsStates.UNREADY: job.get("DAG_NodesUnready", 0),
            WmsStates.READY: job.get("DAG_NodesReady", 0),
            WmsStates.HELD: job.get("JobProcsHeld", 0),
            WmsStates.SUCCEEDED: job.get("DAG_NodesDone", 0),
            WmsStates.FAILED: job.get("DAG_NodesFailed", 0),
            WmsStates.MISFIT: job.get("DAG_NodesPre", 0) + job.get("DAG_NodesPost", 0),
        }
        total_jobs = job.get("DAG_NodesTotal")
        _LOG.debug("_get_state_counts_from_dag_job: from DAG_* keys, total_jobs = %s", total_jobs)
    elif "NodesFailed" in job:
        state_counts = {
            WmsStates.UNREADY: job.get("NodesUnready", 0),
            WmsStates.READY: job.get("NodesReady", 0),
            WmsStates.HELD: job.get("JobProcsHeld", 0),
            WmsStates.SUCCEEDED: job.get("NodesDone", 0),
            WmsStates.FAILED: job.get("NodesFailed", 0),
            WmsStates.MISFIT: job.get("NodesPre", 0) + job.get("NodesPost", 0),
        }
        try:
            total_jobs = job.get("NodesTotal")
        except KeyError as ex:
            _LOG.error("Job missing %s. job = %s", str(ex), job)
            raise
        _LOG.debug("_get_state_counts_from_dag_job: from NODES* keys, total_jobs = %s", total_jobs)
    else:
        # With Kerberos job auth and Kerberos bug, if warning would be printed
        # for every DAG.
        _LOG.debug("Can't get job state counts %s", job["Iwd"])
        total_jobs = 0

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
    wms_state : `lsst.ctrl.bps.WmsStates`
        The equivalent WmsState to given job's status.
    """
    _LOG.debug(
        "htc_job_status_to_wms_state: %s=%s, %s", job["ClusterId"], job["JobStatus"], type(job["JobStatus"])
    )
    job_status = int(job["JobStatus"])
    wms_state = WmsStates.MISFIT

    _LOG.debug("htc_job_status_to_wms_state: job_status = %s", job_status)
    if job_status == JobStatus.IDLE:
        wms_state = WmsStates.PENDING
    elif job_status == JobStatus.RUNNING:
        wms_state = WmsStates.RUNNING
    elif job_status == JobStatus.REMOVED:
        wms_state = WmsStates.DELETED
    elif job_status == JobStatus.COMPLETED:
        if (
            job.get("ExitBySignal", False)
            or job.get("ExitCode", 0)
            or job.get("ExitSignal", 0)
            or job.get("DAG_Status", 0)
            or job.get("ReturnValue", 0)
        ):
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
    wms_state : `lsst.ctrl.bps.WmsStates`
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
        # Use job exist instead of post script exit
        if "DAGMAN error 0" in job["StatusDetails"]:
            wms_state = WmsStates.SUCCEEDED
        else:
            wms_state = WmsStates.FAILED

    return wms_state


def _update_jobs(jobs1, jobs2):
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


def _wms_id_type(wms_id):
    """Determine the type of the WMS id.

    Parameters
    ----------
    wms_id : `str`
        WMS id identifying a job.

    Returns
    -------
    id_type : `lsst.ctrl.bps.htcondor.WmsIdType`
        Type of WMS id.
    """
    try:
        int(float(wms_id))
    except ValueError:
        wms_path = Path(wms_id)
        if wms_path.exists():
            id_type = WmsIdType.PATH
        else:
            id_type = WmsIdType.GLOBAL
    except TypeError:
        id_type = WmsIdType.UNKNOWN
    else:
        id_type = WmsIdType.LOCAL
    return id_type


def _wms_id_to_cluster(wms_id):
    """Convert WMS id to cluster id.

    Parameters
    ----------
    wms_id : `int` or `float` or `str`
        HTCondor job id or path.

    Returns
    -------
    schedd_ad : `classad.ClassAd`
        ClassAd describing the scheduler managing the job with the given id.
    cluster_id : `int`
        HTCondor cluster id.
    id_type : `lsst.ctrl.bps.wms.htcondor.IdType`
        The type of the provided id.
    """
    coll = htcondor.Collector()

    schedd_ad = None
    cluster_id = None
    id_type = _wms_id_type(wms_id)
    if id_type == WmsIdType.LOCAL:
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        cluster_id = int(float(wms_id))
    elif id_type == WmsIdType.GLOBAL:
        constraint = f'GlobalJobId == "{wms_id}"'
        schedd_ads = {ad["Name"]: ad for ad in coll.locateAll(htcondor.DaemonTypes.Schedd)}
        schedds = [htcondor.Schedd(ad) for ad in schedd_ads.values()]
        queries = [schedd.xquery(requirements=constraint, projection=["ClusterId"]) for schedd in schedds]
        results = {
            query.tag(): dict(ads[0])
            for query in htcondor.poll(queries)
            if (ads := query.nextAdsNonBlocking())
        }
        if results:
            schedd_name = next(iter(results))
            schedd_ad = schedd_ads[schedd_name]
            cluster_id = results[schedd_name]["ClusterId"]
    elif id_type == WmsIdType.PATH:
        try:
            job_info = read_dag_info(wms_id)
        except (FileNotFoundError, PermissionError, IOError):
            pass
        else:
            schedd_name = next(iter(job_info))
            job_id = next(iter(job_info[schedd_name]))
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
            cluster_id = int(float(job_id))
    else:
        pass
    return schedd_ad, cluster_id, id_type


def _create_periodic_release_expr(memory, multiplier, limit):
    """Construct an HTCondorAd expression for releasing held jobs.

    The expression instruct HTCondor to release any job which was put on hold
    due to exceeding memory requirements back to the job queue providing it
    satisfies all of the conditions below:

    * number of run attempts did not reach allowable number of retries,
    * the memory requirements in the last failed run attempt did not reach
      the specified memory limit.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float`
        Memory growth rate between retires.
    limit : `int`
        Memory limit.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression for releasing jobs
        which have been held due to exceeding the memory requirements.
    """
    is_retry_allowed = "NumJobStarts <= JobMaxRetries"
    was_below_limit = f"min({{int({memory} * pow({multiplier}, NumJobStarts - 1)), {limit}}}) < {limit}"

    # Job ClassAds attributes 'HoldReasonCode' and 'HoldReasonSubCode' are
    # UNDEFINED if job is not HELD (i.e. when 'JobStatus' is not 5).
    # The special comparison operators ensure that all comparisons below will
    # evaluate to FALSE in this case.
    #
    # Note:
    # May not be strictly necessary. Operators '&&' and '||' are not strict so
    # the entire expression should evaluate to FALSE when the job is not HELD.
    # According to ClassAd evaluation semantics FALSE && UNDEFINED is FALSE,
    # but better safe than sorry.
    was_mem_exceeded = (
        "JobStatus == 5 "
        "&& (HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 "
        "|| HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34)"
    )

    expr = f"{was_mem_exceeded} && {is_retry_allowed} && {was_below_limit}"
    return expr


def _create_periodic_remove_expr(memory, multiplier, limit):
    """Construct an HTCondorAd expression for removing jobs from the queue.

    The expression instruct HTCondor to remove any job which was put on hold
    due to exceeding memory requirements from the job queue providing it
    satisfies any of the conditions below:

    * allowable number of retries was reached,
    * the memory requirements during the last failed run attempt reached
      the specified memory limit.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float`
        Memory growth rate between retires.
    limit : `int`
        Memory limit.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression for removing jobs
        which were run at the maximal allowable memory and still exceeded
        the memory requirements.
    """
    is_retry_disallowed = "NumJobStarts > JobMaxRetries"
    was_limit_reached = f"min({{int({memory} * pow({multiplier}, NumJobStarts - 1)), {limit}}}) == {limit}"

    # Job ClassAds attributes 'HoldReasonCode' and 'HoldReasonSubCode' are
    # UNDEFINED if job is not HELD (i.e. when 'JobStatus' is not 5).
    # The special comparison operators ensure that all comparisons below will
    # evaluate to FALSE in this case.
    #
    # Note:
    # May not be strictly necessary. Operators '&&' and '||' are not strict so
    # the entire expression should evaluate to FALSE when the job is not HELD.
    # According to ClassAd evaluation semantics FALSE && UNDEFINED is FALSE,
    # but better safe than sorry.
    was_mem_exceeded = (
        "JobStatus == 5 "
        "&& (HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 "
        "|| HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34)"
    )

    expr = f"{was_mem_exceeded} && ({is_retry_disallowed} || {was_limit_reached})"
    return expr


def _create_request_memory_expr(memory, multiplier, limit):
    """Construct an HTCondor ClassAd expression for safe memory scaling.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float`
        Memory growth rate between retires.
    limit : `int`
        Memory limit.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression enabling safe
        memory scaling between job retries.
    """
    # The check if the job was held due to exceeding memory requirements
    # will be made *after* job was released back to the job queue (is in
    # the IDLE state), hence the need to use `Last*` job ClassAds instead of
    # the ones describing job's current state.
    #
    # Also, 'Last*' job ClassAds attributes are UNDEFINED when a job is
    # initially put in the job queue. The special comparison operators ensure
    # that all comparisons below will evaluate to FALSE in this case.
    was_mem_exceeded = (
        "LastJobStatus =?= 5 "
        "&& (LastHoldReasonCode =?= 34 && LastHoldReasonSubCode =?= 0 "
        "|| LastHoldReasonCode =?= 3 && LastHoldReasonSubCode =?= 34)"
    )

    # If job runs the first time or was held for reasons other than exceeding
    # the memory, set the required memory to the requested value or use
    # the memory value measured by HTCondor (MemoryUsage) depending on
    # whichever is greater.
    expr = (
        f"({was_mem_exceeded}) "
        f"? min({{int({memory} * pow({multiplier}, NumJobStarts)), {limit}}}) "
        f": max({{{memory}, MemoryUsage ?: 0}})"
    )
    return expr


def _locate_schedds(locate_all=False):
    """Find out Scheduler daemons in an HTCondor pool.

    Parameters
    ----------
    locate_all : `bool`, optional
        If True, all available schedulers in the HTCondor pool will be located.
        False by default which means that the search will be limited to looking
        for the Scheduler running on a local host.

    Returns
    -------
    schedds : `dict` [`str`, `htcondor.Schedd`]
        A mapping between Scheduler names and Python objects allowing for
        interacting with them.
    """
    coll = htcondor.Collector()

    schedd_ads = []
    if locate_all:
        schedd_ads.extend(coll.locateAll(htcondor.DaemonTypes.Schedd))
    else:
        schedd_ads.append(coll.locate(htcondor.DaemonTypes.Schedd))
    return {ad["Name"]: htcondor.Schedd(ad) for ad in schedd_ads}


def _gather_site_values(config, compute_site):
    """Gather values specific to given site.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime
        information.
    compute_site : `str`
        Compute site name.

    Returns
    -------
    site_values : `dict` [`str`, `Any`]
        Values specific to the given site.
    """
    site_values = {"attrs": {}, "profile": {}}
    search_opts = {}
    if compute_site:
        search_opts["curvals"] = {"curr_site": compute_site}

    # Determine the hard limit for the memory requirement.
    found, limit = config.search("memoryLimit", opt=search_opts)
    if not found:
        search_opts["default"] = DEFAULT_HTC_EXEC_PATT
        _, patt = config.search("executeMachinesPattern", opt=search_opts)
        del search_opts["default"]

        # To reduce the amount of data, ignore dynamic slots (if any) as,
        # by definition, they cannot have more memory than
        # the partitionable slot they are the part of.
        constraint = f'SlotType != "Dynamic" && regexp("{patt}", Machine)'
        pool_info = condor_status(constraint=constraint)
        try:
            limit = max(int(info["TotalSlotMemory"]) for info in pool_info.values())
        except ValueError:
            _LOG.debug("No execute machine in the pool matches %s", patt)
    if limit:
        config[".bps_defined.memory_limit"] = limit

    _, site_values["bpsUseShared"] = config.search("bpsUseShared", opt={"default": False})
    site_values["memoryLimit"] = limit

    found, value = config.search("accountingGroup", opt=search_opts)
    if found:
        site_values["accountingGroup"] = value
    found, value = config.search("accountingUser", opt=search_opts)
    if found:
        site_values["accountingUser"] = value

    key = f".site.{compute_site}.profile.condor"
    if key in config:
        for key, val in config[key].items():
            if key.startswith("+"):
                site_values["attrs"][key[1:]] = val
            else:
                site_values["profile"][key] = val

    return site_values
