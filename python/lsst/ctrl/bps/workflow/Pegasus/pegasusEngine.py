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
import re
import sys
import subprocess
import shlex
import shutil
import logging

from Pegasus.DAX3 import ADAG, File, Job, Link, PFN, Executable, Profile, Namespace
from Pegasus.catalogs import replica_catalog, sites_catalog, transformation_catalog
from lsst.ctrl.bps.workflow.HTCondor.lssthtc import htc_write_attribs

FILENODE = 0
TASKNODE = 1
_LOG = logging.getLogger()


class PegasusEngine(object):
    def __init__(self, config):
        self.config = config

    def implementWorkflow(self, genWorkflow):
        return PegasusWorkflow(genWorkflow, self.config)

    def statusID(self, id):
        pass

    def statusAll(self):
        pass

    def remove():
        pass

    # future def history()
    # future def pause()
    # future def continue()
    # future def edit()
    # future def setRestartPoint()
    # future def restart()


class PegasusWorkflow(object):
    def __init__(self, genWorkflow, config):
        self.genWorkflow = genWorkflow.copy()
        self.config = config
        _, computeSite = self.config.search("computeSite")
        self.workdir = self.config["workflowPath"]
        self.files = set(n for n, d in self.genWorkflow.nodes(data=True) if d["nodeType"] == FILENODE)
        self.tasks = set(n for n, d in self.genWorkflow.nodes(data=True) if d["nodeType"] == TASKNODE)
        self.fileCatalog = {}
        self.pegasusFiles = {}

        # Replica Catalog keeps mappings of logical file ids/names (LFN's) to physical file ids/names (PFN's)
        if "rcFile" in self.config:
            self.pegasusFiles["rc"] = {"name": self.config["rcFile"], "obj": None}
        else:
            fname = "rc.txt"
            rc = replica_catalog.ReplicaCatalog(self.workdir, fname)
            self.pegasusFiles["rc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": rc}

        # Site Catalog describes the sites where the workflow jobs are to be executed.
        if "scFile" in self.config:
            self.pegasusFiles["sc"] = {"name": self.config["scFile"], "obj": None}
        else:
            fname = "sites.xml"
            sc = sites_catalog.SitesCatalog(self.workdir, fname)
            self.pegasusFiles["sc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": sc}

        # Transformation Catalog describes all of the executables
        # (called "transformations") used by the workflow.
        if "tcFile" in self.config:
            self.pegasusFiles["tc"] = {"name": self.config["tcFile"], "obj": None}
        else:
            fname = "tc.txt"
            tc = transformation_catalog.TransformationCatalog(self.workdir, fname)
            self.pegasusFiles["tc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": tc}

        if "propFile" in self.config:
            self.pegasusFiles["prop"] = {"name": self.config["propFile"], "obj": None}
        else:
            fname = "pegasus.properties"
            self.pegasusFiles["prop"] = {"name": "%s/%s" % (self.workdir, fname), "obj": None}

        self.pegasusFiles["dax"] = {
            "name": "%s/%s" % (self.workdir, "workflow.dax"),
            "obj": ADAG(self.config["workflowName"]),
        }
        self.pegasusRunId = None
        self._implement()

    def _implement(self):
        self._handleFileNodes()
        self._handleTaskNodes()
        self._defineSites()
        self._addFilesToRC()
        self._addActivatorToTC()
        self._writePegasusFiles()
        self._runPegasusPlan()

    def _handleFileNodes(self):
        # Process file nodes.
        _, computeSite = self.config.search("computeSite")
        for file_id in self.files:
            attrs = self.genWorkflow.nodes[file_id]
            try:
                name = attrs["lfn"]
            except KeyError:
                msg = 'Mandatory attribute "{}" is missing.'
                raise AttributeError(msg.format("lfn"))
            file_ = File(name)

            # Add physical file names, if any.
            urls = attrs.get("pfn")
            if urls is not None:
                urls = urls.split(",")
                sites = len(urls) * [computeSite]
                for url, site in zip(urls, sites):
                    file_.addPFN(PFN(url, site))

            self.fileCatalog[attrs["lfn"]] = file_

    def _handleTaskNodes(self):
        dax = self.pegasusFiles["dax"]["obj"]

        # Add jobs to the DAX.
        for taskId in self.tasks:
            attrs = self.genWorkflow.nodes[taskId]
            try:
                # name = attrs['exec_name']
                name = attrs["taskAbbrev"]
            except KeyError:
                msg = 'Mandatory attribute "%s" is missing.'
                raise AttributeError(msg.format("taskAbbrev"))
            label = "{name}_{id}".format(name=attrs["label"], id=taskId)
            job = Job(name, id=taskId, node_label=label)

            # Add job command line arguments replacing any file name with
            # respective Pegasus file object.
            args = attrs.get("exec_args", [])
            if args:
                args = args.split()
                lfns = list(set(self.fileCatalog) & set(args))
                if lfns:
                    indices = [args.index(lfn) for lfn in lfns]
                    for idx, lfn in zip(indices, lfns):
                        args[idx] = self.fileCatalog[lfn]
                job.addArguments(*args)

            # Add extra job attributes
            if "jobProfile" in attrs:
                for k, v in attrs["jobProfile"].items():
                    job.addProfile(Profile(Namespace.CONDOR, k, v))

            if "jobEnv" in attrs:
                for k, v in attrs["jobEnv"].items():
                    job.addProfile(Profile("env", k, v))

            for key in ["job_attrib", "jobAttribs"]:
                if key in attrs:
                    for k, v in attrs[key].items():
                        job.addProfile(Profile(Namespace.CONDOR, key=f"+{k}", value=f'"{v}"'))

            # Specify job's inputs.
            inputs = [file_id for file_id in self.genWorkflow.predecessors(taskId)]
            for file_id in inputs:
                attrs = self.genWorkflow.nodes[file_id]
                is_ignored = attrs.get("ignore", False)
                if not is_ignored:
                    file_ = self.fileCatalog[attrs["lfn"]]
                    job.uses(file_, link=Link.INPUT)

            # Specify job's outputs
            outputs = [file_id for file_id in self.genWorkflow.successors(taskId)]
            for file_id in outputs:
                attrs = self.genWorkflow.nodes[file_id]
                is_ignored = attrs.get("ignore", False)
                if not is_ignored:
                    file_ = self.fileCatalog[attrs["lfn"]]
                    job.uses(file_, link=Link.OUTPUT)

            #        streams = attrs.get('streams')
            #        if streams is not None:
            #            if streams & 1 != 0:
            #                job.setStdout(file_)
            #            if streams & 2 != 0:
            #                job.setStderr(file_)
            #
            # Provide default files to store stderr and stdout, if not
            # specified explicitly.
            # if job.stderr is None:
            #    file_ = File('{name}.out'.format(name=label))
            #    job.uses(file_, link=Link.OUTPUT)
            #    job.setStderr(file_)
            # if job.stdout is None:
            #    file_ = File('{name}.err'.format(name=label))
            #    job.uses(file_, link=Link.OUTPUT)
            #    job.setStdout(file_)

            dax.addJob(job)

        # Add job dependencies to the DAX.
        for taskId in self.tasks:
            parents = set()
            for file_id in self.genWorkflow.predecessors(taskId):
                parents.update(self.genWorkflow.predecessors(file_id))
            for parent_id in parents:
                dax.depends(parent=dax.getJob(parent_id), child=dax.getJob(taskId))

    def _addActivatorToTC(self):
        tc = self.pegasusFiles["tc"]["obj"]
        alreadyAdded = {}
        for taskId in self.tasks:
            attrs = self.genWorkflow.nodes[taskId]
            taskAbbrev = attrs.get("taskAbbrev")
            _, computeSite = self.config.search("computeSite", opt={"curvals": {"curr_pipetask": taskAbbrev}})
            if computeSite not in alreadyAdded or taskAbbrev not in alreadyAdded[computeSite]:
                activator = Executable(
                    taskAbbrev,
                    arch=self.config["site"][computeSite]["arch"],
                    os=self.config["site"][computeSite]["os"],
                    installed=True,
                )
                activator.addPFN(PFN("file://%s" % self.config["runQuantumExec"], computeSite))
                tc.add(activator)

    def _defineSites(self):
        sc = self.pegasusFiles["sc"]["obj"]
        for site, siteData in self.config["site"].items():
            sc.add_site(site, arch=siteData["arch"], os=siteData["os"])
            if "directory" in siteData:
                # hack because no Python API
                dirDict = {}
                for d in siteData["directory"]:
                    dirDict[d] = {"path": siteData["directory"][d]["path"]}
                sc._sites[site]["directories"] = dirDict

            # add run attributes to site def to get added to DAG and
            # Pegasus-added jobs
            if "run_attrib" in self.genWorkflow.graph:
                for key, val in self.genWorkflow.graph["run_attrib"].items():
                    sc.add_site_profile(site, namespace=Namespace.CONDOR, key=f"+{key}", value=f'"{val}"')

            # add config provided site attributes
            if "profile" in siteData:
                for p, pData in siteData["profile"].items():
                    for k, v in pData.items():
                        sc.add_site_profile(site, namespace=p, key=k, value=v)

    def _addFilesToRC(self):
        rcObj = self.pegasusFiles["rc"]["obj"]
        files = [file_ for file_ in self.fileCatalog.values() if file_.pfns]
        for file_ in files:
            lfn = file_.name
            for pfn in file_.pfns:
                rcObj.add(lfn, pfn.url, pfn.site)

    def _writePegasusFiles(self):
        os.makedirs(self.workdir, exist_ok=True)

        # Write down the workflow in DAX format.
        with open(self.pegasusFiles["dax"]["name"], "w") as f:
            self.pegasusFiles["dax"]["obj"].writeXML(f)

        # output site catalog
        if self.pegasusFiles["sc"]["obj"] is not None:
            self.pegasusFiles["sc"]["obj"].write()

        # output transformation catalog
        if self.pegasusFiles["tc"]["obj"] is not None:
            self.pegasusFiles["tc"]["obj"].write()

        # output replica catalog
        if self.pegasusFiles["rc"]["obj"] is not None:
            self.pegasusFiles["rc"]["obj"].write()

        self._writePegasusPropertiesFile()

    def _runPegasusPlan(self):
        cmd = ["pegasus-plan"]
        cmd.append("--verbose")
        cmd.append("--conf %s" % self.pegasusFiles["prop"]["name"])
        cmd.append("--dax %s" % self.pegasusFiles["dax"]["name"])
        cmd.append("--dir %s/peg" % self.workdir)
        cmd.append("--cleanup none")
        cmd.append("--sites %s" % self.config["computeSite"])
        cmd.append("--input-dir %s/input --output-dir %s/output" % (self.workdir, self.workdir))
        cmdStr = " ".join(cmd)
        bufsize = 5000
        pegout = "%s/pegasus-plan.out" % self.workdir
        _LOG.info("pegasus-plan output in %s" % pegout)
        with open(pegout, "w") as ppfh:
            process = subprocess.Popen(
                shlex.split(cmdStr), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            buf = os.read(process.stdout.fileno(), bufsize).decode()
            while process.poll is None or len(buf) != 0:
                m = re.search(r"pegasus-run\s+(\S+)", buf)
                ppfh.write(buf)
                if m:
                    self.pegasusRunId = m.group(1)
                buf = os.read(process.stdout.fileno(), bufsize).decode()
            process.stdout.close()
            process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-plan exited with non-zero exit code (%s)" % process.returncode)

        # Hack - Using profile in sites.xml doesn't add run attributes to DAG submission file (see _defineSites)
        # So adding them here:
        if "run_attrib" in self.genWorkflow.graph:
            subname = f'{self.pegasusRunId}/{self.config["workflowName"]}-0.dag.condor.sub'
            shutil.copyfile(subname, subname + ".orig")
            with open(subname + ".orig", "r") as insubfh:
                with open(subname, "w") as outsubfh:
                    for line in insubfh:
                        if line.strip() == "queue":
                            htc_write_attribs(outsubfh, self.genWorkflow.graph["run_attrib"])
                            htc_write_attribs(outsubfh, {"bps_jobabbrev": "DAG"})
                        outsubfh.write(line)

    def _writePegasusPropertiesFile(self):

        with open(self.pegasusFiles["prop"]["name"], "w") as pfh:
            pfh.write("# This tells Pegasus where to find the Site Catalog.\n")
            pfh.write("pegasus.catalog.site.file=%s\n" % self.pegasusFiles["sc"]["name"])
            pfh.write("# This tells Pegasus where to find the Replica Catalog.\n")
            pfh.write("pegasus.catalog.replica=File\n")
            pfh.write("pegasus.catalog.replica.file=%s\n" % self.pegasusFiles["rc"]["name"])
            pfh.write("# This tells Pegasus where to find the Transformation Catalog.\n")
            pfh.write("pegasus.catalog.transformation=Text\n")
            pfh.write("pegasus.catalog.transformation.file=%s\n" % self.pegasusFiles["tc"]["name"])
            pfh.write("# Run Pegasus in shared file system mode.\n")
            pfh.write("pegasus.data.configuration=sharedfs\n")
            pfh.write("# Make Pegasus use links instead of transfering files.\n")
            pfh.write("pegasus.transfer.*.impl=Transfer\n")
            pfh.write("pegasus.transfer.links=true\n")
            # pfh.write("# Use a timestamp as a name for the submit directory.\n")
            # pfh.write("pegasus.dir.useTimestamp=true\n")
            # pfh.write("pegasus.dir.storage.deep=true\n")

    def submit(self):
        cmd = ["pegasus-run"]
        cmd.append(self.pegasusRunId)
        cmdStr = " ".join(cmd)
        bufsize = 5000
        with open("%s/pegasus-run.out" % self.workdir, "w") as ppfh:
            process = subprocess.Popen(
                shlex.split(cmdStr), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            buf = os.read(process.stdout.fileno(), bufsize).decode()
            while process.poll is None or len(buf) != 0:
                ppfh.write(buf)
                buf = os.read(process.stdout.fileno(), bufsize).decode()
            process.stdout.close()
            process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-run exited with non-zero exit code (%s)" % process.returncode)

    def getId(self):
        return self.pegasusRunId
