"""

"""
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
        self.himem_tasks = set(config.get("himem_steps"))
        self.computing_queue = config.get("computing_queue")
        self.computing_queue_himem = config.get("computing_queue_himem")

        _, v = config.search("maxwalltime", opt={"default": 90000})
        self.maxwalltime = v
        _, v = config.search("maxattempt", opt={"default": 5})
        self.maxattempt = v

    def define_task_name(self, step):
        return self.bps_config.get('workflowName') + '_' + step

    def pick_non_init_cmdline(self):
        for node_name in self.bps_workflow.nodes:
            if node_name != 'pipetaskInit':
                return self.bps_workflow.nodes[node_name]['job'].cmdline

    def define_tasks(self):
        tasks = []
        raw_dependency_map = self.create_raw_jobs_dependency_map()
        tasks_dependency_map = self.split_map_over_tasks(raw_dependency_map)
        init_task_cmd_line = self.bps_workflow.nodes['pipetaskInit']['job'].cmdline
        other_task_cmd_line = self.pick_non_init_cmdline()

        for task_step, jobs in tasks_dependency_map.items():
            task = RubinTask()
            task.step = task_step
            task.name = self.define_task_name(task.step)
            task.queue = self.computing_queue_himem if task_step in self.himem_tasks else self.computing_queue
            task.lfns = list(jobs)
            task.local_pfns = self.tasks_inputs[task.name]
            task.maxattempt = self.maxattempt
            task.maxwalltime = self.maxwalltime

            # We take the commandline only from first job  because PanDA uses late binding and
            # command line for each job in task is equal to each other in exception to the processing
            # file name which is substituted by PanDA
            if task_step == 'pipetaskInit':
                task.executable = init_task_cmd_line
            else:
                task.executable = other_task_cmd_line
            tasks.append(task)
        self.add_dependencies(tasks, tasks_dependency_map)
        return tasks

    def add_dependencies(self, tasks, tasks_dependency_map):
        for task in tasks:
            jobs = tasks_dependency_map[task.step]
            dependencies = []
            for job, job_dependency in jobs.items():
                job_dep = {
                    "name": job,
                    "submitted": False,
                    "dependencies": [{"task": dependency[0], "inputname":
                                     dependency[1][0], "available": False} for dependency in
                                     job_dependency.items()]
                }
                dependencies.append(job_dep)
            task.dependencies = dependencies

    def create_raw_jobs_dependency_map(self):
        dependency_map = {}
        for edge in self.bps_workflow.in_edges():
            dependency_map.setdefault(edge[1], []).append(edge[0])
        all_nodes = list(self.bps_workflow.nodes())
        nodes_from_edges = set(list(dependency_map.keys()))
        extra_nodes = [node for node in all_nodes if node not in nodes_from_edges]
        for node in extra_nodes:
            dependency_map.setdefault(node, [])
        return dependency_map

    def split_map_over_tasks(self, raw_dependency_map):
        tasks_dependency_map = {}
        for job, dependency in raw_dependency_map.items():
            file_name = self.get_input_file(job)
            file_local_src = self.bps_workflow.nodes.get(job).get("inputs")[file_name].src_uri
            tasks_dependency_map.setdefault(self.get_task_by_job_name(job), {})[file_name] = \
                self.split_dependencies_by_tasks(dependency)
            self.tasks_inputs.setdefault(self.define_task_name(
                self.get_task_by_job_name(job)), []).append(file_local_src)
        return tasks_dependency_map

    def get_task_by_job_name(self, job_name):
        return job_name.split("_")[1] if len(job_name.split("_")) > 1 else job_name

    def split_dependencies_by_tasks(self, dependencies):
        """
        This is a structure to be filled out in function
        dependencies = [
                 {"name": "filename0",
                  "dependencies":[{"task": "task1", "inputname":"filename0", "available": False"},],
                  "submitted": False}
        ]
        """

        dependencies_by_tasks = {}
        for dependency in dependencies:
            dependencies_by_tasks.setdefault(self.define_task_name(
                self.get_task_by_job_name(dependency)), []).append(self.get_input_file(dependency))
        return dependencies_by_tasks

    def get_input_file(self, job_name):
        return next(iter(self.bps_workflow.nodes.get(job_name).get("inputs")))
