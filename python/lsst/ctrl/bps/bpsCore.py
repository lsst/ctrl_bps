import logging
import subprocess
import warnings
import os
from os.path import expandvars
import re
import pickle
import shlex
import networkx
import sys
import yaml

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from lsst.pipe.base.graph import QuantumGraph
from lsst.pipe.base.graph import QuantumGraphTaskNodes
#from lsst.pipe.base.graph import QuantumGraphNodes

from lsst.ctrl.bps.bpsConfig import BpsConfig, FILENODE, TASKNODE
from lsst.ctrl.mpexec.cmdLineFwk import CmdLineFwk
from lsst.daf.butler.core.config import Loader


from lsst.ctrl.bps.bpsDraw import draw_networkx_dot, draw_qgraph_html

_LOG = logging.getLogger()


def pretty_dataset_label(origName):
    newName = re.sub(r": ", "=", origName)
    newName = re.sub(r"\+", "\n", newName)
    newName = re.sub(r",", "\n", newName)
    newName = re.sub(r"[\{\}]", "", newName)
    return newName


def save_single_qgnode(qgnode, outFilename):
    """Save single quantum to file

    Parameters
    ----------
    qgnode : QuantumGraph Node
        Single quantum to save
    outFilename : `str`
        Name of the output file
    """
    os.makedirs(os.path.dirname(outFilename), exist_ok=True)
    qgraph2 = QuantumGraph()
    qgraph2.append(qgnode)
    with open(outFilename, "wb") as pickleFile:
        pickle.dump(qgraph2, pickleFile)


def countQuantum(qgraph):
    cnt = 0
    for task_id, nodes in enumerate(qgraph):
        _LOG.debug("%d task has %s quanta" % (task_id, len(nodes.quanta)))
        cnt += len(nodes.quanta)

    _LOG.debug("Total number of quanta = %d" % (cnt))
    return cnt


class BpsCore(object):
    @staticmethod
    def configLog(longlog, logLevels):
        """Configure logging system.

        Parameters
        ----------
        longlog : `bool`
            If True then make log messages appear in "long format"
        logLevels : `list` of `tuple`
            per-component logging levels, each item in the list is a tuple
            (component, level), `component` is a logger name or `None` for root
            logger, `level` is a logging level name ('DEBUG', 'INFO', etc.)
        """
        if longlog:
            message_fmt = "%-5p %d{yyyy-MM-ddThh:mm:ss.sss} %c (%X{LABEL})(%F:%L)- %m%n"
        else:
            message_fmt = "%c %p: %m%n"


    def __init__(self, configFile, **kwargs):
        self.configLog(False, [])
        self.config = BpsConfig(configFile)
        _LOG.debug("Core kwargs = '%s'" % kwargs)

        if len(kwargs.get('overrides', {})) > 0:
            fd = StringIO(kwargs['overrides'])
            dct = yaml.load(fd, Loader)
            self.config.update(dct)

        self.submitPath = self.config['submitPath']

        # make directories
        os.makedirs(self.submitPath, exist_ok=True)

        if self.config.get('saveDot', {'default': False}):
            os.makedirs("%s/draw" % self.submitPath, exist_ok=True)

    def _createQuantumGraph(self):
        # create cmdline 
        qGraphGenExec = 'pipetask'
        cmd = [qGraphGenExec]

        found, dataQuery = self.config.search('dataQuery')
        if found:
            cmd.append('-d "%s"' % dataQuery)
        found, butlerConfig = self.config.search('butlerConfig')
        if found:
            cmd.append('-b %s' % (expandvars(butlerConfig)))

        if "packageSearch" in self.config:
            for p in self.config["packageSearch"].split(','):
                cmd.append('-p %s' % p.strip())

        cmd.append('-i %s' % (self.config["inCollection"]))
        cmd.append('-o %s' % (self.config["outCollection"]))
        cmd.append('qgraph')  # pipetask subcommand
        for taskAbbrev in [x.strip() for x in self.config['pipeline'].split(',')]:
            pipetask = self.config['pipetask'][taskAbbrev]
            cmd.append('-t %s:%s' % (pipetask['module'], taskAbbrev))
            if "configFile" in pipetask:
                cmd.append("-C %s:%s" % (taskAbbrev, expandvars(pipetask['configFile'])))
            if "configOverride" in pipetask:
                cmd.append("-c %s:%s" % (taskAbbrev, expandvars(pipetask['configOverride'])))

        _LOG.debug("submitPath = '%s'" % self.submitPath)
        self.qgraphFilename = '%s/qgraph.pickle' % (self.submitPath)
        cmd.append('-q %s' % (self.qgraphFilename))

        if self.config.get('saveDot', {'default': False}):
            cmd.append('--pipeline-dot %s/draw/pipetask_pipeline.dot' % (self.submitPath))
            cmd.append('--qgraph-dot %s/draw/pipetask_qgraph.dot' % (self.submitPath))

        cmdstr = ' '.join(cmd)
        _LOG.info(cmdstr)

        #with warnings.catch_warnings():
        #    warnings.simplefilter("error", UserWarning)
        #    CmdLineFwk().parseAndRun(shlex.split(cmdstr))
        bufsize = 5000
        with open("%s/quantumGraphGeneration.out" % self.submitPath, "w") as qqgfh:
            process = subprocess.Popen(shlex.split(cmdstr), shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            buf = os.read(process.stdout.fileno(), bufsize).decode()
            while process.poll is None or len(buf) != 0:
                qqgfh.write(buf)
                buf = os.read(process.stdout.fileno(), bufsize).decode()
            process.stdout.close()
            process.wait()

        if process.returncode != 0:
            raise RuntimeError("%s exited with non-zero exit code (%s)"
                               % (qGraphGenExec, process.returncode))

        with open(self.qgraphFilename, 'rb') as pickleFile:
            self.qgraph = pickle.load(pickleFile)

        if countQuantum(self.qgraph) == 0:
            raise RuntimeError("QuantumGraph is empty")

        if self.config.get('saveDot', {'default': False}):
            draw_qgraph_html(self.qgraph,
                             os.path.join(self.submitPath, "draw", "bpsgraph_quantum.dot"))

    def _createScienceGraph(self):
        """Create expanded graph from the QuantumGraph that has explicit dependencies
        and has individual nodes for each input/output dataset

        Parameters
        ----------
        qgraph : QuantumGraph
            QuantumGraph for the pipeline (as generated by the QuantumGraph Generator)
        """
        _LOG.info("creating explicit science graph")

        self.sciGraph = networkx.DiGraph()
        ncnt = 0
        tcnt = 0
        dcnt = 0

        mapId = {}
        self.qgnodes = {}
        for taskId, nodes in enumerate(self.qgraph):
            _LOG.debug(taskId)
            taskDef = nodes.taskDef
            _LOG.debug("dir=%s", dir(taskDef))
            _LOG.debug("config=%s", taskDef.config)
            _LOG.debug("taskClass=%s", taskDef.taskClass)
            _LOG.debug("taskName=%s", taskDef.taskName)
            _LOG.debug("label=%s", taskDef.label)
            for qId, quantum in enumerate(nodes.quanta):
                # _LOG.debug("dir=%s",dir(quantum))
                _LOG.debug('actualInputs=%s', quantum.actualInputs)
                _LOG.debug('id=%s', quantum.id)
                _LOG.debug('run=%s', quantum.run)
                _LOG.debug('task=%s', quantum.task)
                ncnt += 1
                tcnt += 1
                # tnodeName = "task%d (%s)" % (ncnt, taskDef.taskName)
                tnodeName = "%06d" % (ncnt)
                self.sciGraph.add_node(tnodeName, nodeType=TASKNODE, task_def_id=taskId,
                                       taskAbbrev=taskDef.label, shape='box', fillcolor='gray',
                                       #style='"filled,bold"',
                                       style='filled',
                                       label='.'.join(taskDef.taskName.split('.')[-2:]))
                quanta2 = [quantum]
                self.qgnodes[tnodeName] = QuantumGraphTaskNodes(taskDef, quanta2)

                # Make nodes for inputs
                for dsRefs in quantum.predictedInputs.values():
                    for dsRef in dsRefs:
                        dsName = "%s+%s" % (dsRef.datasetType.name, dsRef.dataId)
                        if dsName not in mapId:
                            ncnt += 1
                            dcnt += 1
                            mapId[dsName] = ncnt
                        fnodeName = "%06d" % mapId[dsName]
                        fnodeDesc = pretty_dataset_label(dsName)
                        self.sciGraph.add_node(fnodeName, nodeType=FILENODE, label=fnodeDesc,
                                               shape='box', style='rounded')
                        self.sciGraph.add_edge(fnodeName, tnodeName)
                # Make nodes for outputs
                for dsRefs in quantum.outputs.values():
                    for dsRef in dsRefs:
                        dsName = "%s+%s" % (dsRef.datasetType.name, dsRef.dataId)
                        if dsName not in mapId:
                            ncnt += 1
                            dcnt += 1
                            mapId[dsName] = ncnt
                        fnodeName = "%06d" % mapId[dsName]
                        fnodeDesc = pretty_dataset_label(dsName)
                        self.sciGraph.add_node(fnodeName, nodeType=FILENODE,
                                               label=fnodeDesc, shape='box', style='rounded')
                        self.sciGraph.add_edge(tnodeName, fnodeName)

        _LOG.info("Number of sciGraph nodes: tasks=%d files=%d", tcnt, dcnt)

    def _updateTask(self, taskAbbrev, tnode, qlfn):
        taskOpt = {'curvals': {'curr_pipetask': taskAbbrev, 'qlfn': qlfn}, 'required': True}
        _, tnode['exec_name'] = self.config.search('runQuantumExec', opt=taskOpt)
        _, tnode['exec_args'] = self.config.search('runQuantumArgs', opt=taskOpt)
        _, computeSite = self.config.search('computeSite', opt=taskOpt)

        taskOpt['required'] = False
        jobProfile = {}
        found, val = self.config.search('requestMemory', opt=taskOpt)
        if found:
            jobProfile['request_memory'] = val

        found, val = self.config.search('requestCpus', opt=taskOpt)
        if found:
            jobProfile['request_cpus'] = val

        if len(jobProfile) > 0:
            tnode['jobProfile'] = jobProfile

    def _linkSchemaNodes(self, schemaNodes):
        print(schemaNodes)
        taskAbbrevList = [x.strip() for x in self.config['pipeline'].split(',')]
        for abbrevId, taskAbbrev in enumerate(taskAbbrevList, 0):
            if abbrevId != 0:
                # get current task's schema task node
                stNodeName = schemaNodes[taskAbbrev][TASKNODE]
                stNode = self.genWFGraph.nodes[stNodeName]

                # get previous task's schema output file node
                prevAbbrev = taskAbbrevList[abbrevId - 1]
                print("%s->%s" % (taskAbbrev, prevAbbrev))
                sfNodeName = schemaNodes[prevAbbrev][FILENODE]
                sfNode = self.genWFGraph.nodes[sfNodeName]
                print("%s->%s" % (sfNodeName, stNodeName))
                print("%s->%s" % (sfNode['label'], stNode['label']))

                # add edge from prev output schema to current task node
                self.genWFGraph.add_edge(sfNodeName, stNodeName)

    def _createWorkflowGraph(self):
        """Create workflow graph from the Science Graph that has information
        needed for WMS (e.g., filenames, command line arguments, etc)

        Parameters
        ----------
        args :
            Command line arguments
        sciGraph : `networkx.DiGraph`
            Science Graph for the pipeline
        taskDefs : `dict`
            Dictionary of taskDefs
        """

        _LOG.info("creating workflow graph")
        self.genWFGraph = self.sciGraph.copy()
        ncnt = networkx.number_of_nodes(self.genWFGraph)
        qcnt = 0
        schemaNodes = {}
        nodelist = list(self.genWFGraph.nodes())
        for nodename in nodelist:
            node = self.genWFGraph.node[nodename]
            if node['nodeType'] == FILENODE:   # data/file
                node['lfn'] = nodename
                node['ignore'] = True
                node['data_type'] = "science"
            elif node['nodeType'] == TASKNODE:  # task
                taskAbbrev = node['taskAbbrev']

                # add quantum pickle input data node
                ncnt += 1
                qcnt += 1
                qNodeName = "%06d" % ncnt
                qlfn = "quantum%s.pickle" % nodename
                qFileName = os.path.join(self.submitPath, 'input', qlfn)
                lfn = os.path.basename(qFileName)
                self.genWFGraph.add_node(qNodeName, nodeType=FILENODE, lfn=lfn, label=lfn, pfn=qFileName,
                                         ignore=False, data_type="quantum", shape='box', style='rounded')
                save_single_qgnode(self.qgnodes[nodename], qFileName)

                self._updateTask(taskAbbrev, node, qlfn)
                self.genWFGraph.add_edge(qNodeName, nodename)

                # add schema job to setup graph
                if self.config.get('createSchemas', '{default: False}'):
                    if taskAbbrev in schemaNodes:
                        stNodeName = schemaNodes[taskAbbrev][TASKNODE]
                    else:
                        schemaNodes[taskAbbrev] = {}
                        ncnt += 1
                        stNodeName = "%06d" % ncnt
                        lfn = "%s_schema" % taskAbbrev
                        self.genWFGraph.add_node(stNodeName, nodeType=TASKNODE,
                                                 task_def_id=node['task_def_id'],
                                                 taskAbbrev=taskAbbrev, shape='box', fillcolor='gray',
                                                 #style='"filled,bold"',
                                                 style='filled',
                                                 label=lfn)
                        _LOG.info("creating schema task: %s", taskAbbrev)
                        stNode = self.genWFGraph.node[stNodeName]
                        schemaNodes[taskAbbrev][TASKNODE] = stNodeName
                        self._updateTask('createSchemas', stNode, qlfn)
                        ncnt += 1
                        sfNodeName = "%06d" % ncnt
                        self.genWFGraph.add_node(sfNodeName, nodeType=FILENODE, lfn=lfn, label=lfn,
                                                 ignore=True, data_type=lfn,
                                                 shape='box', style='rounded')
                        schemaNodes[taskAbbrev][FILENODE] = sfNodeName
                        self.genWFGraph.add_edge(stNodeName, sfNodeName)
                        self.genWFGraph.add_edge(qNodeName, stNodeName)
                    self.genWFGraph.add_edge(sfNodeName, nodename)
            else:
                raise ValueError("Invalid nodeType (%s)" % node['nodeType'])
        if self.config.get('createSchemas', '{default: False}'):
            self._linkSchemaNodes(schemaNodes)


    def _createGenericWorkflow(self):
        # first convert LSST-specific graph implementation to networkX graph
        self._createScienceGraph()
        if self.config.get('saveDot', {'default': False}):
            draw_networkx_dot(self.sciGraph, os.path.join(self.submitPath, 'draw', 'bpsgraph_sci.dot'))

        # Create workflow graph
        self._createWorkflowGraph()
        if self.config.get('saveWFGraph', {'default': False}):
            with open(os.path.join(self.submitPath, "wfgraph.pickle"), "wb") as pickleFile:
                pickle.dump(self.genWFGraph, pickleFile)
        if self.config.get('saveDot', {'default': False}):
            draw_networkx_dot(self.genWFGraph, os.path.join(self.submitPath, 'draw', 'bpsgraph_wf.dot'))

    def _createGenericWorkflowConfig(self):
        self.genWFConfig = BpsConfig(self.config)
        self.genWFConfig['workflowName'] = self.config['uniqProcName']
        self.genWFConfig['workflowPath'] = self.submitPath
        _, computeSite = self.config.search('computeSite')
        _, computeSite = self.genWFConfig.search('computeSite')

    def _implement_workflow(self):
        # import workflow engine class
        modparts = self.config['.global.workflowEngineClass'].split('.')
        fromname = '.'.join(modparts[0:-1])
        importname = modparts[-1]
        _LOG.info("%s %s", fromname, importname)
        mod = __import__(fromname, fromlist=[importname])
        dynclass = getattr(mod, importname)
        self.workflow_engine = dynclass(self.genWFConfig)
        self.workflow = self.workflow_engine.implementWorkflow(self.genWFGraph)

    def createSubmission(self):
        self._createQuantumGraph()
        self._createGenericWorkflow()
        self._createGenericWorkflowConfig()
        self._implement_workflow()

    def submit(self):
        self.workflow.submit()

    def getId(self):
        return self.workflow.getId()
