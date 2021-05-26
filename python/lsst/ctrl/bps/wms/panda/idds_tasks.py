"""

"""
import ntpath
from dataclasses import dataclass


@dataclass
class RubinTask:
    """
    Holds parameters needed to define a PanDA task
    """
    name: str = None
    step: str = None
    queue: str = None
    executable: str = None
    maxwalltime: int = None
    maxattempt: int = None
    lfns: list = None
    local_pfns: list = None
    dependencies: list = None


class IDDSWorkflowGenerator:
    """
    Class generates a iDDS workflow to be submitted into PanDA. Workflow includes definition of each task and
    definition of dependencies for each task input.

    Parameters
    ----------
    bps_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            The generic workflow constructed by BPS system

    config : `~lsst.ctrl.bps.BPSConfig`
            BPS configuration that includes necessary submit/runtime information,
            sufficiently defined in YAML file
            supplied in `submit` command
    """
    def __init__(self, bps_workflow, config):
        self.bps_workflow = bps_workflow
        self.bps_config = config
        self.tasks_inputs = {}
        self.jobs_steps = {}
        self.tasks_steps = {}
        self.himem_tasks = set(config.get("himem_steps"))
        self.computing_queue = config.get("computing_queue")
        self.computing_queue_himem = config.get("computing_queue_himem")
        self.qgraph_file = ntpath.basename(config['bps_defined']['run_qgraph_file'])
        _, v = config.search("maxwalltime", opt={"default": 90000})
        self.maxwalltime = v
        _, v = config.search("maxattempt", opt={"default": 5})
        self.maxattempt = v

    def define_task_name(self, step):
        """Return task name as a combination of the workflow name (unique across workflows) and
        processing step name.

        Parameters
        ----------
        step : `str`
            Processing step name

        Returns
        -------
        Task name : `str`
            Computed task name
        """
        return self.bps_config['workflowName'] + '_' + step

    def pick_non_init_cmdline(self):
        """Returns a command line for a task other than initialization

        Returns
        -------
        Command Line : `str`
            Picked command line
        """
        for node_name in self.bps_workflow.nodes:
            if node_name != 'pipetaskInit':
                return self.bps_workflow.nodes[node_name]['job'].cmdline

    def define_tasks(self):
        """ Provide tasks definition sufficient for PanDA submission

        Returns
        -------
        tasks : `list` of `RubinTask`
            Tasks filled with parameters provided in workflow configuration and generated pipeline.
        """
        tasks = []
        raw_dependency_map = self.create_raw_jobs_dependency_map()
        tasks_dependency_map = self.split_map_over_tasks(raw_dependency_map)
        init_task_cmd_line = self.bps_workflow.nodes['pipetaskInit']['job'].cmdline
        other_task_cmd_line = self.pick_non_init_cmdline()
        for task_name, jobs in tasks_dependency_map.items():
            task = RubinTask()
            task.step = task_name
            task.name = task.step
            task.queue = self.computing_queue_himem if self.tasks_steps[task_name] \
                in self.himem_tasks else self.computing_queue
            task.lfns = list(jobs)
            task.maxattempt = self.maxattempt
            task.maxwalltime = self.maxwalltime

            # We take the commandline only from the first job because PanDA uses late binding and
            # command line for each job in task is equal to each other in exception to the processing
            # file name which is substituted by PanDA
            if self.tasks_steps[task_name] == 'pipetaskInit':
                task.executable = init_task_cmd_line
            else:
                task.executable = other_task_cmd_line
            tasks.append(task)
        self.add_dependencies(tasks, tasks_dependency_map)
        return tasks

    def add_dependencies(self, tasks, tasks_dependency_map):
        """ Add the dependency list to a task definition. This list defines all inputs of a task and how that
        inputs depend on upstream processing steps

        Parameters
        ----------
        tasks : `list` of `RubinTask`
            Tasks to be filled with dependency information

        tasks_dependency_map : `dict` of dependencies dictionary

        Returns
        -------
        Method modifies items in the tasks list provided as an argument
        """
        for task in tasks:
            jobs = tasks_dependency_map[task.step]
            task_dependencies = []
            for job, job_dependency in jobs.items():
                job_dep = {
                    "name": job,
                    "submitted": False,
                }
                input_files_dependencies = []
                for taskname, files in job_dependency.items():
                    for file in files:
                        input_files_dependencies.append({"task": taskname,
                                                         "inputname": file, "available": False})
                job_dep["dependencies"] = input_files_dependencies
                task_dependencies.append(job_dep)
            task.dependencies = task_dependencies

    def create_raw_jobs_dependency_map(self):
        """ Compute the DAG nodes dependency map (node - list of nodes) for each node in the workflow DAG

        Returns
        -------
        dependency_map : `dict` of `node-dependencies` pairs.
            For each node in workflow DAG computed its dependencies (other nodes).
        """
        dependency_map = {}
        for edge in self.bps_workflow.in_edges():
            dependency_map.setdefault(self.get_pseudo_input_file_name(edge[1]), []).\
                append(self.get_pseudo_input_file_name(edge[0]))
            self.jobs_steps[self.get_pseudo_input_file_name(edge[1])] = \
                self.bps_workflow.nodes.get(edge[1]).get('job').label
        all_nodes = list(self.bps_workflow.nodes())
        nodes_from_edges = set(list(dependency_map.keys()))
        extra_nodes = [node for node in all_nodes if node not in nodes_from_edges]
        for node in extra_nodes:
            dependency_map.setdefault(self.get_pseudo_input_file_name(node), [])
            self.jobs_steps[self.get_pseudo_input_file_name(node)] = \
                self.bps_workflow.nodes.get(node).get('job').label
        return dependency_map

    def split_map_over_tasks(self, raw_dependency_map):
        """ Groups nodes performing same operations into tasks. For each task define inputs and its
        dependencies.

        This is a structure to be filled out in function
        taskname: dependencies = [
                 {"name": "filename0",
                  "dependencies":[{"task": "task1", "inputname":"filename0", "available": False"},],
                  "submitted": False}
        ]

        Parameters
        ----------
        raw_dependency_map : `dict` of
            Pairs node-list of directly connected upstream nodes

        Returns
        -------
        tasks_dependency_map : `dict` of `str`: `list`
            Dict of tasks/correspondent dependencies
        """
        tasks_dependency_map = {}
        for job, dependency in raw_dependency_map.items():
            tasks_dependency_map.setdefault(self.define_task_name(self.jobs_steps[job]), {})[job] = \
                self.split_dependencies_by_tasks(dependency)
            self.tasks_inputs.setdefault(self.define_task_name(
                self.get_task_by_job_name(job)), []).append(job)
            self.tasks_steps[self.define_task_name(self.jobs_steps[job])] = self.jobs_steps[job]
        return tasks_dependency_map

    def get_task_by_job_name(self, job_name):
        return job_name.split("_")[1] if len(job_name.split("_")) > 1 else job_name

    def split_dependencies_by_tasks(self, dependencies):
        """ Group the list of dependencies by tasks where dependencies comes from.

        Parameters
        ----------
        dependencies : `list` of dicts
            Each dictionary in the list contains information about dependency: task,inputname,available

        Returns
        -------
        dependencies_by_tasks : `dict` of `str`: `list`
            Dict of tasks/dependency files comes from that task

        """
        dependencies_by_tasks = {}
        for dependency in dependencies:
            dependencies_by_tasks.setdefault(self.define_task_name(
                self.jobs_steps[dependency]), []).append(dependency)
        return dependencies_by_tasks

    def get_input_file(self, job_name):
        """ Extracts the quantum graph file needed for a job

        Parameters
        ----------
        job_name: `str`
            the name of the node in workflow DAG

        Returns
        -------
        quantum graph file name
        """
        return next(iter(self.bps_workflow.nodes.get(job_name).get("inputs")))

    def get_pseudo_input_file_name(self, jobname):
        qgraph_node_ids = self.bps_workflow.nodes.get(jobname).get("job").qgraph_node_ids
        if qgraph_node_ids:
            pseudo_input_file_name = self.qgraph_file+"+"+jobname + "+" + qgraph_node_ids[0].buildId + \
                "+" + str(qgraph_node_ids[0].number)
        else:
            pseudo_input_file_name = self.qgraph_file+"+"+jobname
        return pseudo_input_file_name
