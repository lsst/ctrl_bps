import os
import re
import sys
import subprocess
import shlex
import logging

import htcondor
from lsst.ctrl.bps.workflow.HTCondor.htcondor import HTCDag, HTCJob, htcWriteAttribs

FILENODE=0
TASKNODE=1
_LOG = logging.getLogger()


class HTCondorEngine(object):
    def __init__(self, config):
        self.config = config

    def implementWorkflow(self, genWorkflow):
        return HTCondorWorkflow(genWorkflow, self.config)

    def statusID(self, id):
        pass

    def statusAll(self):
        pass

    def remove(self, jobid):
        result = schedd.act(htcondor.JobAction.Remove, [str(jobid)])
        if result['TotalError']:
            raise RuntimeError('Problems removing %s' % jobid)
        return result['TotalSuccess']

    # future def history()
    # future def pause()
    # future def continue()
    # future def edit()
    # future def setRestartPoint()
    # future def restart()


class HTCondorWorkflow(object):
    def __init__(self, genWorkflow, config):
        self.genWorkflow = genWorkflow.copy()
        self.config = config
        _, computeSite = self.config.search('computeSite')
        self.workdir = self.config['workflowPath']
        self.files = set(n for n, d in self.genWorkflow.nodes(data=True) if d['nodeType'] == FILENODE)
        self.tasks = set(n for n, d in self.genWorkflow.nodes(data=True) if d['nodeType'] == TASKNODE)

        self.runId = None
        self._implement()

    def _implement(self):
        self._handleFileNodes()
        self._handleTaskNodes()
        #self._defineSites()
        self._writeFiles()
        self._prepareSubmission()

    def _handleFileNodes(self):
        # Process file nodes.
        _, computeSite = self.config.search('computeSite')
        for file_id in self.files:
            attrs = self.genWorkflow.nodes[file_id]
            try:
                name = attrs['lfn']
            except KeyError:
                msg = 'Mandatory attribute "{}" is missing.'
                raise AttributeError(msg.format('lfn'))

            # Add physical file names, if any.
            urls = attrs.get('pfn')
            if urls is not None:
                urls = urls.split(',')
                sites = len(urls) * [computeSite]

    def _createJobName(self, label, id):
        jobname = '{name}_{id}'.format(name=label, id=id)
        return jobname

    def _handleTaskNodes(self):
        self.dag = HTCDag(name=self.config['uniqProcName'])
        self.dag.addAttribs(self.genWorkflow.graph['job_attrib'])

        # Add jobs to the DAG.
        id2name = {}
        for taskId in self.tasks:
            task_attrs = self.genWorkflow.nodes[taskId]
            print(task_attrs)
            print(task_attrs['job_attrib'])
            try:
                label = task_attrs['taskAbbrev']
            except KeyError:
                msg = 'Mandatory attribute "%s" is missing.'
                raise AttributeError(msg.format('taskAbbrev'))
            jobname = self._createJobName(label, taskId)
            id2name[taskId] = jobname

            job = HTCJob(jobname, label)
            jobcmds = {}
            jobcmds['universe'] = 'vanilla'
            jobcmds['should_transfer_files'] = 'YES'
            jobcmds['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
            jobcmds['notification'] = 'Never'
            jobcmds['transfer_executable'] = 'False'
            jobcmds['getenv'] = 'True'
            #job['transfer_input_files'] = ??
            #job['transfer_output_files'] = ??

            jobcmds['executable'] = task_attrs['exec_name']
            if 'exec_args' in task_attrs:
                jobcmds['arguments'] = task_attrs['exec_args']
            if 'request_cpus' in task_attrs:
                jobcmds['request_cpus'] = task_attrs['request_cpus']
            if 'request_memory' in task_attrs:
                jobcmds['request_memory'] = task_attrs['request_memory']

            # job stdout, stderr, htcondor user log
            jobcmds['output'] = os.path.join(label, "%s.$(Cluster).out" % jobname)
            jobcmds['error'] = os.path.join(label, "%s.$(Cluster).err" % jobname)
            jobcmds['log'] = os.path.join(label, "%s.$(Cluster).log" % jobname)

            # Add job command line arguments replacing any file name with
            # respective filename
            #args = task_attrs.get('exec_args', [])
            #if args:
            #    args = args.split()
            #    lfns = list(set(self.fileCatalog) & set(args))
            #    if lfns:
            #        indices = [args.index(lfn) for lfn in lfns]
            #        for idx, lfn in zip(indices, lfns):
            #            args[idx] = self.fileCatalog[lfn]
            #    job.addArguments(*args)

            # Add extra job attributes
            if 'jobProfile' in task_attrs:
                for k, v in task_attrs['jobProfile'].items():
                    print("jobcmds %s=%s" % (k,v))
                    jobcmds[k] = v

            #if 'jobEnv' in task_attrs:
            #    for k, v in task_attrs['jobEnv'].items():
            #        job.addProfile(Profile('env', k, v))


            # Specify job's inputs.
            inputs = [file_id for file_id in self.genWorkflow.predecessors(taskId)]
            transinputs = []
            for file_id in inputs:
                file_attrs = self.genWorkflow.nodes[file_id]
                is_ignored = file_attrs.get('ignore', False)
                if not is_ignored:
                    transinputs.append(file_attrs['pfn'])

            if len(transinputs) > 0:
                jobcmds['transfer_input_files'] = ','.join(transinputs)

            # Specify job's outputs
            #outputs = [file_id for file_id in self.genWorkflow.successors(taskId)]
            #for file_id in outputs:
            #    file_attrs = self.genWorkflow.nodes[file_id]
            #    is_ignored = file_attrs.get('ignore', False)
            #    if not is_ignored:
            #        file_ = self.fileCatalog[file_attrs['lfn']]
            #        job.uses(file_, link=Link.OUTPUT)

            job.addJobCmds(jobcmds)
            print(task_attrs)
            if 'job_attrib' in task_attrs:
                for k, v in task_attrs['job_attrib'].items():
                    print("job_attrib %s=%s" % (k,v))
                job.addJobAttrs(task_attrs['job_attrib'])
            if 'jobAttribs' in task_attrs:
                for k, v in task_attrs['jobAttribs'].items():
                    print("jobAttribs %s=%s" % (k,v))
                job.addJobAttrs(task_attrs['jobAttribs'])
            self.dag.addJob(job)

        # Add job dependencies to the DAX.
        for taskId in self.tasks:
            parents = set()
            for file_id in self.genWorkflow.predecessors(taskId):
                parents.update(self.genWorkflow.predecessors(file_id))
            for parent_id in parents:
                self.dag.addEdge(parent=id2name[parent_id],
                                 child=id2name[taskId])

    def _writeFiles(self):
        os.makedirs(self.workdir, exist_ok=True)

        # Write down the workflow in HTCondor format.
        self.dag.write(self.workdir)

    def _prepareSubmission(self):
        os.chdir(self.workdir)

        #cmd = ["condor_submit_dag"]
        #cmd.append("-no_submit")
        #cmd.append(self.dag.filename)

        #cmdStr = ' '.join(cmd)
        #bufsize = 5000
        #subout = "%s/prepare.out" % self.workdir
        #_LOG.info("condor_submit_dag -no_submit output in %s" % subout)
        #with open(subout, "w") as ppfh:
        #    process = subprocess.Popen(shlex.split(cmdStr), shell=False,
        #                               stdout=subprocess.PIPE,
        #                               stderr=subprocess.STDOUT)
        #    buf = os.read(process.stdout.fileno(), bufsize).decode()
        #    while process.poll is None or len(buf) != 0:
        #        ppfh.write(buf)
        #        #m = re.search(r"pegasus-run\s+(\S+)", buf)
        #        #if m:
        #        #    self.pegasusRunId = m.group(1)
        #        buf = os.read(process.stdout.fileno(), bufsize).decode()
        #    process.stdout.close()
        #    process.wait()

        #if process.returncode != 0:
        #    raise RuntimeError("condor_submit_dag -no_submit exited with non-zero exit code (%s)" %
        #                       process.returncode)

    def submit(self):
        self.dag.submit(dict())
        self.runId = self.dag.runId

    def getId(self):
        return self.runId
