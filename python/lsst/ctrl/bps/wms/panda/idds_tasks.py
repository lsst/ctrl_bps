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
import os.path
from dataclasses import dataclass

from lsst.ctrl.bps import GenericWorkflow, GenericWorkflowJob
from lsst.ctrl.bps.wms.panda.cmd_line_embedder import CommandLineEmbedder


@dataclass
class FileDescriptor:
    """
    Holds parameters needed to define a file used by a job of task
    """

    name: str = None
    """Name of the file"""
    distribution_url: str = None
    """URL of the location where this file to be distributed
     to the edge node"""
    submission_url: str = None
    """Path to file on the submission node"""
    direct_IO: bool = False
    """Is the file to be used remotely"""
    delivered: bool = False
    """Is is this file has been delivered to the distribution endpoint"""


@dataclass
class RubinTask:
    """
    Holds parameters needed to define a PanDA task
    """

    name: str = None
    """Name of the task"""
    step: str = None
    """Processing step"""
    queue: str = None
    """Computing queue where the task to be submitted"""
    executable: str = None
    """The task command line to be executed"""
    max_walltime: int = None
    """Maximum allowed walltime in seconds"""
    max_attempt: int = None
    """Maximum number of jobs attempts in a task"""
    max_rss: int = None
    """Maximum size of RAM to be used by a job"""
    cloud: str = None
    """Computing cloud in CRIC registry where the task should
     be submitted to"""
    jobs_pseudo_inputs: list = None
    """Name of preudo input to be used by task and defining jobs"""
    files_used_by_task: list = None
    """List of physical files necessary for running a task"""
    dependencies: list = None
    """List of upstream tasks and its pseudoinput parameters
     needed for running jobs in this task"""
    is_final: bool = False
    """Is this a finalization task"""
    is_dag_end: bool = False
    """Is this task is on the end of the DAG"""


class IDDSWorkflowGenerator:
    """
    Class generates a iDDS workflow to be submitted into PanDA.
    Workflow includes definition of each task and
    definition of dependencies for each task input.

    Parameters
    ----------
    bps_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow constructed by BPS system

    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information,
        sufficiently defined in YAML file supplied in `submit` command
    """

    def __init__(self, bps_workflow, config):
        self.bps_workflow = bps_workflow
        self.bps_config = config
        self.jobs_steps = {}
        self.tasks_steps = {}
        self.tasks_cmd_lines = {}
        self.dag_end_tasks = set()
        self.number_of_retries = {}
        _, self.max_walltime = config.search("maxWalltime", opt={"default": 90000})
        _, self.max_jobs_per_task = config.search("maxJobsPerTask", opt={"default": 30000})

    def define_task_name(self, step):
        """Return task name as a combination of the workflow name (unique
        across workflows) and processing step name.

        Parameters
        ----------
        step : `str`
            Processing step name

        Returns
        -------
        Task name : `str`
            Computed task name
        """
        return self.bps_config["workflowName"] + "_" + step

    def fill_input_files(self, task_name):
        files = []
        jobs = [
            job_name
            for job_name in self.bps_workflow
            if self.bps_workflow.get_job(job_name).label == self.tasks_steps[task_name]
        ]
        for job in jobs:
            for gwfile in self.bps_workflow.get_job_inputs(job, transfer_only=True):
                file = FileDescriptor()
                file.name = gwfile.name
                file.submission_url = gwfile.src_uri
                file.distribution_url = os.path.join(
                    self.bps_config["fileDistributionEndPoint"], os.path.basename(gwfile.src_uri)
                )
                file.direct_IO = gwfile.job_access_remote
                files.append(file)
        return files

    def define_tasks(self):
        """Provide tasks definition sufficient for PanDA submission

        Returns
        -------
        tasks : `list` [`RubinTask`]
            Tasks filled with parameters provided in workflow configuration
            and generated pipeline.
        """
        tasks = []
        raw_dependency_map = self.create_raw_jobs_dependency_map()
        tasks_dependency_map = self.split_map_over_tasks(raw_dependency_map)
        tasks_dependency_map_chunked = self.split_tasks_into_chunks(tasks_dependency_map)
        for task_name, jobs in tasks_dependency_map_chunked.items():
            task = RubinTask()
            task.step = task_name
            task.name = task.step
            picked_job_name = next(
                filter(
                    lambda job_name: self.bps_workflow.get_job(job_name).label == self.tasks_steps[task_name],
                    self.bps_workflow,
                )
            )
            bps_node = self.bps_workflow.get_job(picked_job_name)
            task.queue = bps_node.queue
            task.cloud = bps_node.compute_site
            task.jobs_pseudo_inputs = list(jobs)
            task.max_attempt = self.number_of_retries.get(task_name, 3)
            task.max_walltime = self.max_walltime
            task.max_rss = bps_node.request_memory
            task.executable = self.tasks_cmd_lines[task_name]
            task.files_used_by_task = self.fill_input_files(task_name)
            task.is_final = False
            task.is_dag_end = self.tasks_steps[task_name] in self.dag_end_tasks
            tasks.append(task)
        self.add_dependencies(tasks, tasks_dependency_map_chunked)
        final_task = self.get_final_task()
        tasks.append(final_task)
        return tasks

    def split_tasks_into_chunks(self, tasks_dependency_map):
        """If a task is going to contain jobs whose number is above a
        threshold this function splits such a large tasks into chunks.

        Parameters
        ----------
        tasks_dependency_map : `dict`
            dependencies dictionary with task name in key and jobs
            dependencies in values. The latter dict has a job input
            parameters (PanDA pseudo file name) in the key and dict of
            pairs (upstream task name) - (its PanDA pseudo file name)
            which defines dependency for a job.

        Returns
        -------
        tasks_dependency_map : `dict`
            dependencies dictionary with chunked tasks where its needed.
        """
        tasks_dependency_map_chunked = {}
        tasks_chunked = {}

        """At this step only tasks names are updated to distribute
         tasks over chunks
        """
        for task_name, dependencies in tasks_dependency_map.items():
            n_jobs_in_task = len(dependencies)
            if n_jobs_in_task > self.max_jobs_per_task:
                n_chunks = -(-n_jobs_in_task // self.max_jobs_per_task)
                for pseudo_input, dependency in dependencies.items():
                    chunk_id = hash(pseudo_input) % n_chunks
                    task_name_chunked = self.get_task_name_with_chunk(task_name, chunk_id)
                    tasks_dependency_map_chunked.setdefault(task_name_chunked, {})[pseudo_input] = dependency
                    self.tasks_steps[task_name_chunked] = self.tasks_steps[task_name]
                    self.tasks_cmd_lines[task_name_chunked] = self.tasks_cmd_lines[task_name]
                tasks_chunked[task_name] = n_chunks
            else:
                tasks_dependency_map_chunked[task_name] = dependencies

        """This block propagates chunking over upstream dependencies
         records.
        """
        tasks_dependency_map_chunked_updated_dep = {}
        for task, dependencies in tasks_dependency_map_chunked.items():
            for pseudo_input, dependency in dependencies.items():
                updated_dependencies = {}
                for upstream_task_name, pseudo_inputs in dependency.items():
                    if upstream_task_name in tasks_chunked:
                        for upstream_pseudo_input in pseudo_inputs:
                            chunk_id = hash(upstream_pseudo_input) % tasks_chunked[upstream_task_name]
                            task_name_chunked = self.get_task_name_with_chunk(upstream_task_name, chunk_id)
                            chunked_task_name = task_name_chunked
                            updated_dependencies.setdefault(chunked_task_name, []).append(
                                upstream_pseudo_input
                            )
                    else:
                        updated_dependencies.setdefault(upstream_task_name, []).extend(pseudo_inputs)
                tasks_dependency_map_chunked_updated_dep.setdefault(task, {}).setdefault(
                    pseudo_input, {}
                ).update(updated_dependencies)
        return tasks_dependency_map_chunked_updated_dep

    def get_task_name_with_chunk(self, task_name, chunk_id):
        """Concatenates file name and chunk ID

        Parameters
        ----------
        task_name : `str`
            The name of the task

        chunk_id : `int`
            ID of the chunk

        Returns
        -------
        task_name : `str`
            Concatenated task name
        """
        return f"{task_name}_chunk_{chunk_id}"

    def get_final_task(self):
        """If final job exists in generic workflow, create DAG final task

        Returns
        -------
        task : `RubinTask`
            The final task for a workflow
        """
        final_job = self.bps_workflow.get_final()
        if final_job and isinstance(final_job, GenericWorkflowJob):
            task = RubinTask()
            bash_file = FileDescriptor()
            bash_file.submission_url = final_job.executable.src_uri
            bash_file.distribution_url = os.path.join(
                self.bps_config["fileDistributionEndPoint"], final_job.executable.name
            )
            task.executable = f"bash ./{final_job.executable.name} {final_job.arguments}"

            task.step = final_job.label
            task.name = self.define_task_name(final_job.label)
            task.queue = final_job.queue
            task.cloud = final_job.compute_site
            task.jobs_pseudo_inputs = []

            # This string implements empty pattern for dependencies
            task.dependencies = [
                {"name": "pure_pseudoinput+qgraphNodeId:+qgraphId:", "submitted": False, "dependencies": []}
            ]

            task.max_attempt = self.number_of_retries.get(task.name, 3)
            task.max_walltime = self.max_walltime
            task.max_rss = final_job.request_memory
            task.files_used_by_task = [bash_file]
            task.is_final = True
            task.is_dag_end = False
            return task
        elif final_job and isinstance(final_job, GenericWorkflow):
            raise NotImplementedError("PanDA plugin does not support a workflow as the final job")
        elif final_job:
            return TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final_job)})")

    def add_dependencies(self, tasks, tasks_dependency_map):
        """Add the dependency list to a task definition. This list defines all
        inputs of a task and how that inputs depend on upstream processing
        steps

        Parameters
        ----------
        tasks : `list` [`RubinTask`]
            Tasks to be filled with dependency information

        tasks_dependency_map : `dict` of dependencies dictionary

        Returns
        -------
        Method modifies items in the tasks list provided as an argument
        """
        for task in tasks:
            jobs = tasks_dependency_map[task.step]
            task.dependencies = []
            for job, job_dependency in jobs.items():
                job_dep = {
                    "name": job,
                    "submitted": False,
                }
                input_files_dependencies = []
                for taskname, files in job_dependency.items():
                    for file in files:
                        input_files_dependencies.append(
                            {"task": taskname, "inputname": file, "available": False}
                        )
                job_dep["dependencies"] = input_files_dependencies
                task.dependencies.append(job_dep)

    def create_raw_jobs_dependency_map(self):
        """Compute the DAG nodes dependency map (node - list of nodes) for each
        node in the workflow DAG

        Returns
        -------
        dependency_map : `dict` of `node-dependencies` pairs.
            For each node in workflow DAG computed its dependencies (other
            nodes).
        """

        dependency_map = {}
        cmd_line_embedder = CommandLineEmbedder(self.bps_config)

        for job_name in self.bps_workflow:
            gwjob = self.bps_workflow.get_job(job_name)
            cmd_line, pseudo_file_name = cmd_line_embedder.substitute_command_line(
                gwjob.executable.src_uri + " " + gwjob.arguments, gwjob.cmdvals, job_name
            )
            if len(pseudo_file_name) > 4000:
                raise NameError(
                    "job pseudo input file name contains more than 4000 symbols. Can not proceed."
                )

            self.tasks_cmd_lines[self.define_task_name(gwjob.label)] = cmd_line
            self.jobs_steps[pseudo_file_name] = gwjob.label
            self.number_of_retries[self.define_task_name(gwjob.label)] = gwjob.number_of_retries
            dependency_map[pseudo_file_name] = []
            predecessors = self.bps_workflow.predecessors(job_name)
            for parent_name in predecessors:
                parent_job = self.bps_workflow.get_job(parent_name)
                cmd_line_parent, pseudo_file_parent = cmd_line_embedder.substitute_command_line(
                    parent_job.executable.src_uri + " " + parent_job.arguments,
                    parent_job.cmdvals,
                    parent_name,
                )
                dependency_map.get(pseudo_file_name).append(pseudo_file_parent)

            successors = self.bps_workflow.successors(job_name)
            if next(successors, None) is None:
                self.dag_end_tasks.add(gwjob.label)
        return dependency_map

    def split_map_over_tasks(self, raw_dependency_map):
        """Group nodes performing same operations into tasks. For each task
        define inputs and its dependencies.

        This is a structure to be filled out in function
        taskname: dependencies = [
            {
                "name": "filename0",
                "dependencies": [
                    {
                        "task": "task1",
                        "inputname":"filename0",
                        "available": False"
                    },
                ],
                "submitted": False
            }
        ]

        Parameters
        ----------
        raw_dependency_map : `dict`
            Pairs node-list of directly connected upstream nodes

        Returns
        -------
        tasks_dependency_map : `dict` [`str`, `list`]
            Dict of tasks/correspondent dependencies
        """
        tasks_dependency_map = {}
        for job, dependency in raw_dependency_map.items():
            task_name = self.define_task_name(self.jobs_steps[job])
            tasks_dependency_map.setdefault(task_name, {})[job] = self.split_dependencies_by_tasks(dependency)
            self.tasks_steps[task_name] = self.jobs_steps[job]
        return tasks_dependency_map

    def get_task_by_job_name(self, job_name):
        return job_name.split("_")[1] if len(job_name.split("_")) > 1 else job_name

    def split_dependencies_by_tasks(self, dependencies):
        """Group the list of dependencies by tasks where dependencies comes
        from.

        Parameters
        ----------
        dependencies : `list` [`dicts`]
            Each dictionary in the list contains information about
            dependency: task,inputname,available

        Returns
        -------
        dependencies_by_tasks : `dict` [`str`, `list`]
            Dict of tasks/dependency files comes from that task.

        """
        dependencies_by_tasks = {}
        for dependency in dependencies:
            dependencies_by_tasks.setdefault(self.define_task_name(self.jobs_steps[dependency]), []).append(
                dependency
            )
        return dependencies_by_tasks

    def get_input_file(self, job_name):
        """Extract the quantum graph file needed for a job.

        Parameters
        ----------
        job_name: `str`
            The name of the node in workflow DAG.

        Returns
        -------
        quantum graph file name
        """
        return next(iter(self.bps_workflow.nodes.get(job_name).get("inputs")))
