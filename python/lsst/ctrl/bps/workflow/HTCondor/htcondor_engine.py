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
"""Interface between generic workflow graph to HTCondor workflow system
"""
import os
import logging

from lsst.ctrl.bps.workflow.HTCondor.lssthtc import HTCDag, HTCJob
from lsst.ctrl.bps.workflowEngine import workflow, workflowEngine


FILENODE = 0
TASKNODE = 1
_LOG = logging.getLogger()


class HTCondorEngine(workflowEngine):
    """HTCondor version of workflow engine
    Parameters
    ----------
    config: `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, config):
        super().__init__()
        self.config = config

    def implementWorkflow(self, gen_workflow):
        """Convert generic workflow graph to an HTCondor DAG ready for submission
        Parameters
        ----------
        gen_workflow: `networkx.DiGraph`
            The generic workflow graph (e.g., has executable name and arguments)
        """
        return HTCondorWorkflow(gen_workflow, self.config)


class HTCondorWorkflow(workflow):
    """Interface to HTCondor workflow system
    Parameters
    ----------
    gen_workflow: `networkx.DiGraph`
        The generic workflow graph (e.g., has executable name and arguments)
    config: `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, gen_workflow, config):
        super().__init__(gen_workflow)
        self.gen_workflow = gen_workflow.copy()
        self.config = config
        self.workdir = self.config['workflowPath']
        self.files = set(n for n, d in self.gen_workflow.nodes(data=True) if d['nodeType'] == FILENODE)
        self.tasks = set(n for n, d in self.gen_workflow.nodes(data=True) if d['nodeType'] == TASKNODE)

        self.run_id = None
        self.dag = None
        self._implement()

    def _implement(self):
        """Convert workflow graph into whatever needed for submission to workflow system
        """
        self._handle_task_nodes()
        self._write_files()
        self._prepare_submission()


    def _handle_task_nodes(self):
        """Convert Task nodes to DAG jobs
        """
        def _handle_job_cmds(task_attrs, jobcmds):
            jobcmds['executable'] = task_attrs['exec_name']

            if 'exec_args' in task_attrs:
                jobcmds['arguments'] = task_attrs['exec_args']
            if 'request_cpus' in task_attrs:
                jobcmds['request_cpus'] = task_attrs['request_cpus']
            if 'request_memory' in task_attrs:
                jobcmds['request_memory'] = task_attrs['request_memory']

            # Add extra job attributes
            if 'jobProfile' in task_attrs:
                for key, val in task_attrs['jobProfile'].items():
                    jobcmds[key] = val

        def _handle_job_inputs(gen_workflow, jobcmds):
            """Add job input files from generic workflow to job
            Parameters
            ----------
            gen_workflow: `networkx.DiGraph`
                The generic workflow graph (e.g., has executable name and arguments)
            jobcmds: `RestrictedDict`
                Commands for a htcondor job, updated with transfer of job inputs
            """
            # inputs = [file_id for file_id in self.gen_workflow.predecessors(task_id)]
            transinputs = []
            # for file_id in inputs:
            for file_id in gen_workflow.predecessors(task_id):
                file_attrs = gen_workflow.nodes[file_id]
                is_ignored = file_attrs.get('ignore', False)
                if not is_ignored:
                    transinputs.append(file_attrs['pfn'])

            if len(transinputs) > 0:
                jobcmds['transfer_input_files'] = ','.join(transinputs)

        self.dag = HTCDag(name=self.config['uniqProcName'])
        self.dag.add_attribs(self.gen_workflow.graph['job_attrib'])

        # Add jobs to the DAG.
        id2name = {}
        for task_id in self.tasks:
            task_attrs = self.gen_workflow.nodes[task_id]
            try:
                label = task_attrs['taskAbbrev']
            except KeyError:
                msg = 'Mandatory attribute "%s" is missing.'
                raise AttributeError(msg.format('taskAbbrev'))
            id2name[task_id] = f"{label}_{task_id}"

            job = HTCJob(id2name[task_id], label)
            jobcmds = {'universe': 'vanilla', 'should_transfer_files': 'YES',
                       'when_to_transfer_output': 'ON_EXIT_OR_EVICT', 'notification': 'Never',
                       'transfer_executable': 'False', 'getenv': 'True'}

            _handle_job_cmds(task_attrs, jobcmds)

            # job stdout, stderr, htcondor user log
            jobcmds['output'] = os.path.join(label, f"{id2name[task_id]}.$(Cluster).out")
            jobcmds['error'] = os.path.join(label, f"{id2name[task_id]}.$(Cluster).err")
            jobcmds['log'] = os.path.join(label, f"{id2name[task_id]}.$(Cluster).log")

            _handle_job_inputs(self.gen_workflow, jobcmds)

            # Add the job cmds dict to the job object
            job.add_job_cmds(jobcmds)

            # Add job attributes to job
            for key in ['job_attrib', 'jobAttribs']:
                if key in task_attrs:
                    job.add_job_attrs(task_attrs[key])

            self.dag.add_job(job)

        # Add job dependencies to the DAX.
        for task_id in self.tasks:
            parents = set()
            for file_id in self.gen_workflow.predecessors(task_id):
                parents.update(self.gen_workflow.predecessors(file_id))
            for parent_id in parents:
                self.dag.add_job_relationship(parent=id2name[parent_id],
                                 child=id2name[task_id])

    def _write_files(self):
        """Output any files needed for workflow submission
        """
        os.makedirs(self.workdir, exist_ok=True)

        # Write down the workflow in HTCondor format.
        self.dag.write(self.workdir)

    def _prepare_submission(self):
        """Any steps between writing files and actual submission
        """
        os.chdir(self.workdir)

    def submit(self):
        """Submit workflow
        """
        self.dag.submit(dict())
        self.run_id = self.dag.run_id

    def getId(self):
        """Return the workflow id as recognized by workflow system
        Returns
        -------
        run_id:
            ID generated by workflow system at submission time
        """
        return self.run_id
