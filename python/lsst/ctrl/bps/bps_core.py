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

"""Core functionality of BPS
"""

__all__ = ("BpsCore",)

import logging
import subprocess
import itertools
import os
import datetime
from os.path import expandvars, basename
import re
import pickle
import shlex
import shutil
import time
import networkx
from networkx import bipartite
import yaml

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import lsst.log
from lsst.daf.butler import DimensionUniverse
from lsst.pipe.base.graph import QuantumGraph
from lsst.ctrl.bps.bps_config import BpsConfig
from lsst.daf.butler.core.config import Loader
from lsst.ctrl.bps.bps_draw import draw_networkx_dot

# Config section search order
BPS_SEARCH_ORDER = ["payload", "pipetask", "site", "global"]

# Graph property
FILENODE = 0
TASKNODE = 1

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""

_LOG = logging.getLogger()


def execute(command, filename):
    """Execute a command.

    Parameters
    ----------
    command : `str`
        String representing the command to execute.
    filename : `str`
        A file to which both stderr and stdout will be written to.

    Returns
    -------
    exit_code : `int`
        The exit code the command being executed finished with.
    """
    buffer_size = 5000
    with open(filename, "w") as f:
        f.write(command)
        f.write("\n")
        process = subprocess.Popen(
            shlex.split(command), shell=False, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        while process.poll is None or len(buffer) != 0:
            f.write(buffer)
            buffer = os.read(process.stdout.fileno(), buffer_size).decode()
        process.stdout.close()
        process.wait()
    return process.returncode


def pretty_dataset_label(orig_name):
    """Tweak dataset for a label

    Parameters
    ----------
    orig_name : `str`
        dataset as str

    Returns
    -------
    new_name : `str`
        reformatted dataset for label
    """
    new_name = re.sub(r": ", "=", orig_name)
    new_name = re.sub(r"\+", "\n", new_name)
    new_name = re.sub(r",", "\n", new_name)
    new_name = re.sub(r"[\{\}]", "", new_name)
    return new_name


def save_qg_subgraph(qnodes, qgraph, out_filename):
    """Save subgraph to file

    Parameters
    ----------
    qnodes : `lsst.pipe.base.graph.quantumNode.QuantumNode` or
             iterable of `lsst.pipe.base.graph.quantumNode.QuantumNode`
        QuantumNodes for Quanta inside given qgraph to save
    out_filename : `str`
        Name of the output file
    """

    # create subgraph
    subgraph = qgraph.subset(qnodes)

    # output to file
    os.makedirs(os.path.dirname(out_filename), exist_ok=True)
    with open(out_filename, "wb") as outfh:
        subgraph.save(outfh)


class BpsCore():
    """Contains information needed for submitting a run
    """
    @staticmethod
    def config_log(longlog):
        """Configure logging system.

        Parameters
        ----------
        longlog : `bool`
            If True then make log messages appear in "long format"
        """
        if longlog:
            message_fmt = "%-5p %d{yyyy-MM-ddThh:mm:ss.sss} %c (%X{LABEL})(%F:%L)- %m%n"
        else:
            message_fmt = "%c %p: %m%n"

        lsst.log.configure_prop(_LOG_PROP.format(message_fmt))

    def __init__(self, configFile, **kwargs):
        self.config_log(False)
        self.config = BpsConfig(configFile, BPS_SEARCH_ORDER)
        _LOG.debug("Core kwargs = '%s'", kwargs)
        self.config[".global.timestamp"] = "{:%Y%m%dT%Hh%Mm%Ss}".format(datetime.datetime.now())
        if "uniqProcName" not in self.config:
            self.config[".global.uniqProcName"] = self.config["outCollection"].replace("/", "_")

        if len(kwargs.get("overrides", {})) > 0:
            overrides_io = StringIO(kwargs["overrides"])
            dct = yaml.load(overrides_io, Loader)
            self.config.update(dct)

        self.submit_path = self.config["submitPath"]
        _LOG.info("submit_path = '%s'", self.submit_path)

        # make directories
        os.makedirs(self.submit_path, exist_ok=True)

        if self.config.get("saveDot", {"default": False}):
            os.makedirs("%s/draw" % self.submit_path, exist_ok=True)

        self.butler = None
        self.pipeline_labels = []
        self.qgraph_filename = None
        self.qgraph = None
        self.sci_graph = None
        self.gen_wf_graph = None
        self.gen_wf_config = None
        self.workflow = None

    def _create_cmdline_building_qgraph(self):
        """Create the command for generating QuantumGraph from scratch.

        Returns
        -------
        cmd : `str`
            String representing the command to generate QuantumGraph.
        """
        cmd = ["pipetask"]
        cmd.append("qgraph")  # pipetask subcommand

        found, data_query = self.config.search("dataQuery")
        if found:
            cmd.append('-d "%s"' % data_query)
        found, butler_config = self.config.search("butlerConfig")
        if found:
            cmd.append("-b %s" % (expandvars(butler_config)))

        if "packageSearch" in self.config:
            for pkg in self.config["packageSearch"].split(","):
                cmd.append("-p %s" % pkg.strip())

        cmd.append("-i %s" % (self.config["inCollection"]))
        cmd.append("-o notused")
        # cmd.append('--output-run %s' % (self.config["outCollection"]))
        if "pipelineYaml" in self.config:
            cmd.append("-p %s" % (self.config["pipelineYaml"]))
        else:
            for task_abbrev in [x.strip() for x in self.pipeline_labels]:
                pipetask = self.config["pipetask"][task_abbrev]
                cmd.append("-t %s:%s" % (pipetask["module"], task_abbrev))
                if "configFile" in pipetask:
                    cmd.append("-C %s:%s" % (task_abbrev, expandvars(pipetask["configFile"])))
                if "configOverride" in pipetask:
                    cmd.append("-c %s:%s" % (task_abbrev, expandvars(pipetask["configOverride"])))

        cmd.append("-q %s" % (self.qgraph_filename))

        if self.config.get("saveDot", {"default": False}):
            cmd.append("--pipeline-dot %s/draw/pipetask_pipeline.dot" % (self.submit_path))
            cmd.append("--qgraph-dot %s/draw/pipetask_qgraph.dot" % (self.submit_path))

        return " ".join(cmd)

    def _create_quantum_graph(self):
        """Create QuantumGraph
        """
        _LOG.debug("submit_path = '%s'", self.submit_path)
        self.qgraph_filename = "%s/%s.pickle" % (self.submit_path, self.config["uniqProcName"])

        args = {"curvals": {"qgraphfile": self.qgraph_filename}}
        found, cmd = self.config.search("createQuantumGraph", opt=args)
        if not found:
            cmd = self._create_cmdline_building_qgraph()
            _LOG.warning("command for generating Quantum Graph not found; "
                         "generated one from scratch")
        _LOG.info(cmd)

        out = f"{self.submit_path}/quantumGraphGeneration.out"
        status = execute(cmd, out)
        if status != 0:
            raise RuntimeError(
                "QuantumGraph generation exited with non-zero exit code (%s)" % (status)
            )

    def _read_quantum_graph(self):
        """Read the QuantumGraph
        """

        with open(self.qgraph_filename, "rb") as infh:
            self.qgraph = QuantumGraph.load(infh, DimensionUniverse())
        if len(self.qgraph) == 0:
            raise RuntimeError("QuantumGraph is empty")

    def _create_science_graph(self):
        """Create expanded graph from the QuantumGraph that has
        explicit dependencies and has individual nodes for each
        input/output dataset

        Parameters
        ----------
        qgraph : `QuantumGraph`
            QuantumGraph for the pipeline (as generated by the
            QuantumGraph Generator)
        """
        _LOG.info("creating explicit science graph")

        self.sci_graph = networkx.DiGraph()
        tcnt = 0   # task node counter
        dcnt = 0   # dataset ref node counter

        dsname_to_node_id = {}

        for node in self.qgraph:
            _LOG.debug("type(node)=%s", type(node))
            _LOG.debug("nodeId=%s", node.nodeId)

            task_def = node.taskDef

            _LOG.debug("config=%s", task_def.config)
            _LOG.debug("taskClass=%s", task_def.taskClass)
            _LOG.debug("taskName=%s", task_def.taskName)
            _LOG.debug("label=%s", task_def.label)

            tcnt += 1

            tnode_name = "%06d" % (node.nodeId.number)
            self.sci_graph.add_node(
                tnode_name,
                node_type=TASKNODE,
                task_abbrev=task_def.label,
                qgnode=node,
                shape="box",
                fillcolor="gray",
                # style='"filled,bold"',
                style="filled",
                label=".".join(task_def.taskName.split(".")[-2:]),
            )
            quantum = node.quantum

            # Make dataset ref nodes for inputs
            for ds_ref in itertools.chain.from_iterable(quantum.inputs.values()):
                ds_name = f"{ds_ref.datasetType.name}+{ds_ref.dataId}"
                if ds_name not in dsname_to_node_id:
                    dcnt += 1
                    fnode_name = f"ds{dcnt:06}"
                    dsname_to_node_id[ds_name] = fnode_name
                    fnode_label = pretty_dataset_label(ds_name)
                    self.sci_graph.add_node(
                        fnode_name, node_type=FILENODE, label=fnode_label, shape="box", style="rounded"
                    )
                fnode_name = dsname_to_node_id[ds_name]
                self.sci_graph.add_edge(fnode_name, tnode_name)

            # Make dataset ref nodes for outputs
            for ds_ref in itertools.chain.from_iterable(quantum.outputs.values()):
                ds_name = f"{ds_ref.datasetType.name}+{ds_ref.dataId}"
                if ds_name not in dsname_to_node_id:
                    dcnt += 1
                    fnode_name = f"ds{dcnt:06}"
                    dsname_to_node_id[ds_name] = fnode_name
                    fnode_label = pretty_dataset_label(ds_name)
                    self.sci_graph.add_node(
                        fnode_name, node_type=FILENODE, label=fnode_label, shape="box", style="rounded"
                    )
                fnode_name = dsname_to_node_id[ds_name]
                self.sci_graph.add_edge(tnode_name, fnode_name)

        if "pipelineLabels" in self.config:
            self.pipeline_labels = self.config["pipelineLabels"].split(",")
        else:
            self.pipeline_labels = [task.label for task in self.qgraph.iterTaskGraph()]
        _LOG.info("pipeline_labels = %s", self.pipeline_labels)

        _LOG.info("Number of sci_graph nodes: tasks=%d files=%d", tcnt, dcnt)

    def _update_task(self, task_abbrev, tnode, qlfn):
        """Update task node with workflow info

        Parameters
        ----------
        task_abbrev: `str`
            Task abbreviation used for config searches
        tnode: node
            Task node
        qlfn: `str`
            Single quantum logical file name
        """
        task_opt = {"curvals": {"curr_pipetask": task_abbrev, "qlfn": qlfn}, "required": True}
        _, tnode["exec_name"] = self.config.search("runQuantumExec", opt=task_opt)
        _, tnode["exec_args"] = self.config.search("runQuantumArgs", opt=task_opt)
        _, compute_site = self.config.search("computeSite", opt=task_opt)

        task_opt["required"] = False
        job_profile = {}
        job_attribs = {}
        if "profile" in self.config["site"][compute_site]:
            if "condor" in self.config["site"][compute_site]["profile"]:
                for key, val in self.config["site"][compute_site]["profile"]["condor"].items():
                    if key.startswith("+"):
                        job_attribs[key[1:]] = val
                    else:
                        job_profile[key] = val

        found, val = self.config.search("requestMemory", opt=task_opt)
        if found:
            job_profile["request_memory"] = val

        found, val = self.config.search("requestCpus", opt=task_opt)
        if found:
            job_profile["request_cpus"] = val

        if len(job_profile) > 0:
            tnode["jobProfile"] = job_profile
        if len(job_attribs) > 0:
            tnode["jobAttribs"] = job_attribs

    def _add_workflow_init_nodes(self):
        """ Add nodes to workflow graph that perform any initialization for the workflow.

        Assumes that all of the initialization should be executed prior to any of the
        current workflow.
        """
        # Create a workflow graph that will have task and file nodes necessary for
        # initializing the pipeline execution
        init_graph = self._create_workflow_init_graph()
        _LOG.debug("init_graph nodes = %s", init_graph.nodes())

        # Find source nodes in workflow graph.
        task_nodes = [n for n, d in self.gen_wf_graph.nodes(data=True) if d["node_type"] == TASKNODE]
        task_graph = bipartite.projected_graph(self.gen_wf_graph, task_nodes)
        task_sources = [n for n in task_graph if task_graph.in_degree(n) == 0]
        _LOG.debug("workflow sources = %s", task_sources)

        # Find sink nodes of initonly graph.
        init_sinks = [n for n in init_graph if init_graph.out_degree(n) == 0]
        _LOG.debug("init sinks = %s", init_sinks)

        # Add initonly nodes to Workflow graph and make new edges.
        self.gen_wf_graph.add_nodes_from(init_graph.nodes(data=True))
        self.gen_wf_graph.add_edges_from(init_graph.edges())
        for source in task_sources:
            for sink in init_sinks:
                self.gen_wf_graph.add_edge(sink, source)

    def _create_workflow_init_graph(self):
        """Create workflow subgraph for running initialization job(s).
        """
        _LOG.info("creating init subgraph")
        initgraph = networkx.DiGraph()

        # create nodes for executing --init-only
        tnode_name = "pipetask_init"
        initgraph.add_node(
            tnode_name,
            node_type=TASKNODE,
            task_abbrev=tnode_name,
            label=tnode_name,
            job_attrib={"bps_jobabbrev": tnode_name},
            shape="box",
            fillcolor="gray",
            style="filled",
        )
        self._update_task(tnode_name, initgraph.nodes[tnode_name], self.qgraph_filename)

        _LOG.info("creating init task input(s)")
        fnode_name = basename(self.qgraph_filename)
        initgraph.add_node(
            fnode_name,
            node_type=FILENODE,
            lfn=fnode_name,
            label=fnode_name,
            pfn=self.qgraph_filename,
            ignore=False,
            data_type="quantum",
            shape="box",
            style="rounded",
        )
        initgraph.add_edge(fnode_name, tnode_name)

        _LOG.info("creating init task output(s)")
        # All outputs go to Butler.  So currently need dummy file node.
        fnode_name = "pipetask_init_outputs"
        initgraph.add_node(
            fnode_name,
            node_type=FILENODE,
            lfn=fnode_name,
            label=fnode_name,
            ignore=True,
            data_type="science",
            shape="box",
            style="rounded",
        )
        initgraph.add_edge(tnode_name, fnode_name)

        return initgraph

    def _create_workflow_graph(self, gname):
        """Create workflow graph from the Science Graph that has information
        needed for WMS (e.g., filenames, command line arguments, etc)

        Parameters
        ----------
        args :
            Command line arguments
        sci_graph : `networkx.DiGraph`
            Science Graph for the pipeline
        task_def : `dict`
            Dictionary of task_def
        """

        _LOG.info("creating workflow graph")
        self.gen_wf_graph = networkx.DiGraph(self.sci_graph, gname=gname, gtype="workflow")

        ncnt = networkx.number_of_nodes(self.gen_wf_graph)
        taskcnts = {}
        qcnt = 0
        nodelist = list(self.gen_wf_graph.nodes())
        for nodename in nodelist:
            node = self.gen_wf_graph.nodes[nodename]
            if node["node_type"] == FILENODE:  # data/file
                node["lfn"] = nodename
                node["ignore"] = True
                node["data_type"] = "science"
            elif node["node_type"] == TASKNODE:  # task
                task_abbrev = node["task_abbrev"]
                node["job_attrib"] = {"bps_jobabbrev": task_abbrev}
                if task_abbrev not in taskcnts:
                    taskcnts[task_abbrev] = 0
                taskcnts[task_abbrev] += 1

                # add quantum pickle input data node
                ncnt += 1
                qcnt += 1
                qnode_name = f"qgraph_{nodename}"
                qlfn = f"quantum_{nodename}_{task_abbrev}.pickle"
                q_filename = os.path.join(self.submit_path, "input", task_abbrev, qlfn)
                lfn = basename(q_filename)
                self.gen_wf_graph.add_node(
                    qnode_name,
                    node_type=FILENODE,
                    lfn=lfn,
                    label=lfn,
                    pfn=q_filename,
                    ignore=False,
                    data_type="quantum",
                    shape="box",
                    style="rounded",
                )
                save_qg_subgraph(node["qgnode"], self.qgraph, q_filename)

                self._update_task(task_abbrev, node, qlfn)
                self.gen_wf_graph.add_edge(qnode_name, nodename)
            else:
                raise ValueError("Invalid node_type (%s)" % node["node_type"])

        if self.config.get("runInit", "{default: False}"):
            self._add_workflow_init_nodes()

        # save pipeline summary description to graph attributes
        run_summary = []
        for task_abbrev in [x.strip() for x in self.pipeline_labels]:
            run_summary.append("%s:%d" % (task_abbrev, taskcnts[task_abbrev]))
        self.gen_wf_graph.graph["run_attrib"] = {
            "bps_run_summary": ";".join(run_summary),
            "bps_isjob": "True",
            "bps_project": self.config["project"],
            "bps_campaign": self.config["campaign"],
            "bps_run": gname,
            "bps_operator": self.config["operator"],
            "bps_payload": self.config["payloadName"],
            "bps_runsite": "TODO",
        }

    def _create_generic_workflow(self):
        """Create generic workflow graph
        """
        # first convert LSST-specific graph implementation to networkX graph
        self._create_science_graph()
        if self.config.get("saveDot", {"default": False}):
            draw_networkx_dot(self.sci_graph, os.path.join(self.submit_path, "draw", "bpsgraph_sci.dot"))

        # Create workflow graph
        self._create_workflow_graph(self.config["uniqProcName"])
        if self.config.get("saveWFGraph", {"default": False}):
            with open(os.path.join(self.submit_path, "wfgraph.pickle"), "wb") as pickle_file:
                pickle.dump(self.gen_wf_graph, pickle_file)
        if self.config.get("saveDot", {"default": False}):
            draw_networkx_dot(self.gen_wf_graph, os.path.join(self.submit_path, "draw", "bpsgraph_wf.dot"))

    def _create_generic_workflow_config(self):
        """Create generic workflow configuration
        """
        self.gen_wf_config = BpsConfig(self.config)
        self.gen_wf_config["workflowName"] = self.config["uniqProcName"]
        self.gen_wf_config["workflowPath"] = self.submit_path

    def _implement_workflow(self):
        """Convert workflow to inputs for a particular WMS
        """
        # import workflow engine class
        modparts = self.config[".global.workflowEngineClass"].split(".")
        fromname = ".".join(modparts[0:-1])
        importname = modparts[-1]
        _LOG.info("%s %s", fromname, importname)
        mod = __import__(fromname, fromlist=[importname])
        dynclass = getattr(mod, importname)
        workflow_engine = dynclass(self.gen_wf_config)
        self.workflow = workflow_engine.implement_workflow(self.gen_wf_graph)

    def create_submission(self):
        """Create submission files but don't actually submit
        """
        subtime = time.time()

        found, filename = self.config.search("qgraph_file")
        if found:
            _LOG.info("Copying quantum graph (%s)", filename)
            stime = time.time()
            self.qgraph_filename = "%s/%s" % (self.submit_path, basename(filename))
            shutil.copy2(filename, self.qgraph_filename)
            _LOG.info("Copying quantum graph took %.2f seconds", time.time() - stime)
        else:
            _LOG.info("Creating quantum graph")
            stime = time.time()
            self._create_quantum_graph()
            _LOG.info("Creating quantum graph took %.2f seconds", time.time() - stime)

        _LOG.info("Reading quantum graph (%s)", self.qgraph_filename)
        stime = time.time()
        self._read_quantum_graph()
        _LOG.info("Reading quantum graph with %d nodes took %.2f seconds", len(self.qgraph),
                  time.time() - stime)

        _LOG.info("Creating Generic Workflow")
        stime = time.time()
        self._create_generic_workflow()
        self._create_generic_workflow_config()
        _LOG.info("Creating Generic Workflow took %.2f seconds", time.time() - stime)

        stime = time.time()
        _LOG.info("Creating specific implementation of workflow")
        self._implement_workflow()
        _LOG.info("Creating specific implementation of workflow took %.2f seconds", time.time() - stime)

        _LOG.info("Total submission creation time = %.2f", time.time() - subtime)

    def submit(self):
        """Submit workflow for running
        """
        self.workflow.submit()

    def get_id(self):
        """Return workflow's run ID
        """
        return self.workflow.get_id()
