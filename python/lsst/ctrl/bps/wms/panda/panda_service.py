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

import os
import logging
import binascii
import concurrent.futures

from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from lsst.ctrl.bps.wms.panda.idds_tasks import IDDSWorkflowGenerator
from lsst.daf.butler import ButlerURI
from idds.workflow.workflow import Workflow as IDDS_client_workflow, AndCondition
from idds.doma.workflow.domapandawork import DomaPanDAWork
import idds.common.constants as idds_constants
import idds.common.utils as idds_utils
import pandaclient.idds_api

_LOG = logging.getLogger(__name__)


class PanDAService(BaseWmsService):
    """PanDA version of WMS service
    """

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an PanDA iDDS ready for submission

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration that includes necessary submit/runtime
            information.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        out_prefix : `str`
            The root directory into which all WMS-specific files are written

        Returns
        ----------
        workflow : `lsst.ctrl.bps.wms.panda.panda_service.PandaBpsWmsWorkflow`
            PanDA workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        workflow = PandaBpsWmsWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                             f"{self.__class__.__module__}."
                                                             f"{self.__class__.__name__}")
        workflow.write(out_prefix)
        return workflow

    def convert_exec_string_to_hex(self, cmdline):
        """Convert the command line into hex representation.
        This step is currently involved because large blocks of command lines
        including special symbols passed to the pilot/container. To make sure
        the 1 to 1 matching and pass by the special symbol stripping
        performed by the Pilot we applied the hexing.

        Parameters
        ----------
        cmdline : `str`
            UTF-8 command line string

        Returns
        -------
        hex : `str`
            Hex representation of string
        """
        return binascii.hexlify(cmdline.encode()).decode("utf-8")

    def add_decoder_prefix(self, cmd_line, distribution_path, files):
        """
        Compose the command line sent to the pilot from the functional part
        (the actual SW running) and the middleware part (containers invocation)

        Parameters
        ----------
        cmd_line : `str`
            UTF-8 based functional part of the command line
        distribution_path : `str`
            URI of path where all files are located for distribution
        files `list` [`str`]
            File names needed for a task

        Returns
        -------
        decoder_prefix : `str`
            Full command line to be executed on the edge node
        """

        cmdline_hex = self.convert_exec_string_to_hex(cmd_line)
        _, decoder_prefix = self.config.search("runner_command", opt={"replaceEnvVars": False,
                                                                      "expandEnvVars": False})
        decoder_prefix = decoder_prefix.replace("_cmd_line_", str(cmdline_hex) + " ${IN/L} "
                                                + distribution_path + "  "
                                                + "+".join(f'{k}:{v}' for k, v in files[0].items())
                                                + " " + "+".join(files[1]))
        return decoder_prefix

    def submit(self, workflow):
        """Submit a single PanDA iDDS workflow

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWorkflow`
            A single PanDA iDDS workflow to submit
        """
        idds_client_workflow = IDDS_client_workflow()
        files = self.copy_files_for_distribution(workflow.generated_tasks,
                                                 self.config['fileDistributionEndPoint'])
        DAG_end_work = []
        DAG_final_work = None

        for idx, task in enumerate(workflow.generated_tasks):
            work = DomaPanDAWork(
                executable=self.add_decoder_prefix(task.executable, self.config['fileDistributionEndPoint'],
                                                   files),
                primary_input_collection={'scope': 'pseudo_dataset',
                                          'name': 'pseudo_input_collection#' + str(idx)},
                output_collections=[{'scope': 'pseudo_dataset',
                                     'name': 'pseudo_output_collection#' + str(idx)}],
                log_collections=[], dependency_map=task.dependencies,
                task_name=task.name,
                task_queue=task.queue,
                task_log={"destination": "local", "value": "log.tgz", "dataset": "PandaJob_#{pandaid}/",
                          "token": "local", "param_type": "log", "type": "template"},
                encode_command_line=True,
                task_rss=task.maxrss,
                task_cloud=task.cloud,
            )
            idds_client_workflow.add_work(work)
            if task.is_final:
                DAG_final_work = work
            if task.is_dag_end:
                DAG_end_work.append(work)

        if DAG_final_work:
            conditions = []
            for work in DAG_end_work:
                conditions.append(work.is_terminated)
            and_cond = AndCondition(conditions=conditions, true_works=[DAG_final_work])
            idds_client_workflow.add_condition(and_cond)

        idds_request = {
            'scope': 'workflow',
            'name': workflow.name,
            'requester': 'panda',
            'request_type': idds_constants.RequestType.Workflow,
            'transform_tag': 'workflow',
            'status': idds_constants.RequestStatus.New,
            'priority': 0,
            'lifetime': 30,
            'workload_id': idds_client_workflow.get_workload_id(),
            'request_metadata': {'workload_id': idds_client_workflow.get_workload_id(),
                                 'workflow': idds_client_workflow}
        }
        c = pandaclient.idds_api.get_api(idds_utils.json_dumps,
                                         idds_host=self.config.get('idds_server'), compress=True)
        request_id = c.add_request(**idds_request)
        _LOG.info("Submitted into iDDs with request id=%i", request_id)
        workflow.run_id = request_id

    @staticmethod
    def copy_files_for_distribution(tasks, file_distribution_uri):
        """
        Brings locally generated files into Cloud for further
        utilization them on the edge nodes.

        Parameters
        ----------
        local_pfns: `list` of `tasks`
            Tasks that input files needs to be placed for
            distribution
        file_distribution_uri: `str`
            Path on the edge node accessed storage,
            including access protocol, bucket name to place files

        Returns
        -------
        files_plc_hldr, direct_IO_files : `dict` [`str`, `str`], `set` of `str`
            First parameters is key values pairs
            of file placeholder - file name
            Second parameter is set of files which will be directly accessed.

        """

        local_pfns = {}
        direct_IO_files = set()
        for task in tasks:
            for file in task.files_used_by_task:
                if not file.delivered:
                    local_pfns[file.name] = file.submission_url
                    if file.direct_IO:
                        direct_IO_files.add(file.name)

        files_to_copy = {}

        # In case there are folders we iterate over its content
        for local_pfn in local_pfns.values():
            if os.path.isdir(local_pfn):
                files_in_folder = ButlerURI.findFileResources([local_pfn])
                folder_name = os.path.basename(local_pfn)
                for file in files_in_folder:
                    file_name = file.basename()
                    files_to_copy[file] = ButlerURI(os.path.join(file_distribution_uri,
                                                                 folder_name, file_name))
            else:
                files_to_copy[ButlerURI(local_pfn)] = ButlerURI(os.path.join(file_distribution_uri,
                                                                             os.path.basename(local_pfn)))

        copy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        future_file_copy = []
        for src, trgt in files_to_copy.items():

            # S3 clients explicitly instantiate here to overpass this
            # https://stackoverflow.com/questions/52820971/is-boto3-client-thread-safe
            trgt.exists()
            future_file_copy.append(copy_executor.submit(trgt.transfer_from, src, transfer="copy"))
        for future in concurrent.futures.as_completed(future_file_copy):
            if not future.result() is None:
                raise RuntimeError("Error of placing files to the distribution point")

        if len(direct_IO_files) == 0:
            direct_IO_files.add('cmdlineplaceholder')

        files_plc_hldr = {}
        for file_placeholder, src_path in local_pfns.items():
            files_plc_hldr[file_placeholder] = os.path.basename(src_path)
            if os.path.isdir(src_path):
                # this is needed to make isdir function working
                # properly in ButlerURL instance on the egde node
                files_plc_hldr[file_placeholder] += "/"

        return files_plc_hldr, direct_IO_files

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """Stub for future implementation of the report method
        Expected to return run information based upon given constraints.

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
        runs : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be
            pointers to documentation or to WMS specific commands.
        """
        message = ""
        run_reports = None
        return run_reports, message


class PandaBpsWmsWorkflow(BaseWmsWorkflow):
    """A single Panda based workflow
    Parameters
    ----------
    name : `str`
        Unique name for Workflow
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.generated_tasks = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow,
                              out_prefix, service_class):
        # Docstring inherited from parent class
        idds_workflow = cls(generic_workflow.name, config)
        workflow_generator = IDDSWorkflowGenerator(generic_workflow, config)
        idds_workflow.generated_tasks = workflow_generator.define_tasks()
        _LOG.debug("panda dag attribs %s", generic_workflow.run_attrs)
        return idds_workflow

    def write(self, out_prefix):
        """Not yet implemented
        """
