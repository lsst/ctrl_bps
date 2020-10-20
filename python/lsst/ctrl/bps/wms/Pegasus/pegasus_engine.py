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

"""Support for using Pegasus WMS"""

import os
import re
import subprocess
import shlex
import shutil
import logging

from Pegasus.DAX3 import ADAG, File, Job, Link, PFN, Executable, Profile, Namespace
from Pegasus.catalogs import replica_catalog, sites_catalog, transformation_catalog
from lsst.ctrl.bps.workflow.HTCondor.lssthtc import htc_write_attribs
from lsst.ctrl.bps.wms_service import WmsWorkflow, WmsServer

_LOG = logging.getLogger()


class PegasusService(WmsService):
    """Pegasus version of workflow engine
    Parameters
    ----------
    config : `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """

    def implement_workflow(self, gen_workflow):
        """Convert generic workflow graph to an HTCondor DAG ready for submission

        Parameters
        ----------
        gen_workflow : `networkx.DiGraph`
            The generic workflow graph (e.g., has executable name and arguments)
        """

        return PegasusWorkflow.fromGenericWorkflow(self.config, gen_workflow)


class PegasusWorkflow(WmsWorkflow):
    """Single Pegasus Workflow

    Parameters
    ----------
    gen_workflow : `networkx.DiGraph`
        The generic workflow graph (e.g., has executable name and arguments)
    config : `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, config, gen_workflow):

    def fromPickle
    def fromGenericWorkflow(self, config, generic_workflow):
        pegasus_workflow = PegasusWorkflow()
        self.workdir = self.workflow_config["workflowPath"]
        self.files = set(n for n, d in self.workflow_graph.nodes(data=True) if d["node_type"] == FILENODE)
        self.tasks = set(n for n, d in self.workflow_graph.nodes(data=True) if d["node_type"] == TASKNODE)
        self.file_catalog = {}
        self.pegasus_files = {}

        # Replica Catalog keeps mappings of logical file ids/names (LFN's) to physical file ids/names (PFN's)
        if "rcFile" in self.workflow_config:
            self.pegasus_files["rc"] = {"name": self.workflow_config["rcFile"], "obj": None}
        else:
            fname = "rc.txt"
            rcat = replica_catalog.ReplicaCatalog(self.workdir, fname)
            self.pegasus_files["rc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": rcat}

        # Site Catalog describes the sites where the workflow jobs are to be executed.
        if "scFile" in self.workflow_config:
            self.pegasus_files["sc"] = {"name": self.workflow_config["scFile"], "obj": None}
        else:
            fname = "sites.xml"
            scat = sites_catalog.SitesCatalog(self.workdir, fname)
            self.pegasus_files["sc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": scat}

        # Transformation Catalog describes all of the executables
        # (called "transformations") used by the workflow.
        if "tcFile" in self.workflow_config:
            self.pegasus_files["tc"] = {"name": self.workflow_config["tcFile"], "obj": None}
        else:
            fname = "tc.txt"
            tcat = transformation_catalog.TransformationCatalog(self.workdir, fname)
            self.pegasus_files["tc"] = {"name": "%s/%s" % (self.workdir, fname), "obj": tcat}

        if "propFile" in self.workflow_config:
            self.pegasus_files["prop"] = {"name": self.workflow_config["propFile"], "obj": None}
        else:
            fname = "pegasus.properties"
            self.pegasus_files["prop"] = {"name": "%s/%s" % (self.workdir, fname), "obj": None}

        self.pegasus_files["dax"] = {
            "name": "%s/%s" % (self.workdir, "workflow.dax"),
            "obj": ADAG(self.workflow_config["workflowName"]),
        }
        self._implement()

    def _implement(self):
        """Create files needed for a run submission
        """
        self._handle_file_nodes()
        self._handle_task_nodes()
        self._define_sites()
        self._add_files_to_rc()
        self._add_activator_to_tc()
        self._write_pegasus_files()
        self._run_pegasus_plan()

    def _handle_file_nodes(self):
        """Process file nodes in workflow
        """
        _, compute_site = self.workflow_config.search("computeSite")
        for file_id in self.files:
            attrs = self.workflow_graph.nodes[file_id]
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
                sites = len(urls) * [compute_site]
                for url, site in zip(urls, sites):
                    file_.addPFN(PFN(url, site))

            self.file_catalog[attrs["lfn"]] = file_

    def _handle_task_nodes(self):
        """Process task nodes in workflow
        """
        dax = self.pegasus_files["dax"]["obj"]

        # Add jobs to the DAX.
        for task_id in self.tasks:
            attrs = self.workflow_graph.nodes[task_id]
            try:
                # name = attrs['exec_name']
                name = attrs["task_abbrev"]
            except KeyError:
                msg = 'Mandatory attribute "%s" is missing.'
                raise AttributeError(msg.format("task_abbrev"))
            label = "{name}_{id}".format(name=attrs["label"], id=task_id)
            job = Job(name, id=task_id, node_label=label)

            # Add job command line arguments replacing any file name with
            # respective Pegasus file object.
            args = attrs.get("exec_args", [])
            if args:
                args = args.split()
                lfns = list(set(self.file_catalog) & set(args))
                if lfns:
                    indices = [args.index(lfn) for lfn in lfns]
                    for idx, lfn in zip(indices, lfns):
                        args[idx] = self.file_catalog[lfn]
                job.addArguments(*args)

            # Add extra job attributes
            if "jobProfile" in attrs:
                for key, val in attrs["jobProfile"].items():
                    job.addProfile(Profile(Namespace.CONDOR, key, val))

            if "jobEnv" in attrs:
                for key, val in attrs["jobEnv"].items():
                    job.addProfile(Profile("env", key, val))

            for akey in ["job_attrib", "jobAttribs"]:
                if akey in attrs:
                    for key, val in attrs[akey].items():
                        job.addProfile(Profile(Namespace.CONDOR, key=f"+{key}", value=f'"{val}"'))

            # Specify job's inputs.
            inputs = [file_id for file_id in self.workflow_graph.predecessors(task_id)]
            for file_id in inputs:
                attrs = self.workflow_graph.nodes[file_id]
                is_ignored = attrs.get("ignore", False)
                if not is_ignored:
                    file_ = self.file_catalog[attrs["lfn"]]
                    job.uses(file_, link=Link.INPUT)

            # Specify job's outputs
            outputs = [file_id for file_id in self.workflow_graph.successors(task_id)]
            for file_id in outputs:
                attrs = self.workflow_graph.nodes[file_id]
                is_ignored = attrs.get("ignore", False)
                if not is_ignored:
                    file_ = self.file_catalog[attrs["lfn"]]
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
        for task_id in self.tasks:
            parents = set()
            for file_id in self.workflow_graph.predecessors(task_id):
                parents.update(self.workflow_graph.predecessors(file_id))
            for parent_id in parents:
                dax.depends(parent=dax.getJob(parent_id), child=dax.getJob(task_id))

    def _add_activator_to_tc(self):
        """Add job activator to Pegasus Transformation Catalog
        """
        tcat = self.pegasus_files["tc"]["obj"]
        already_added = {}
        for task_id in self.tasks:
            attrs = self.workflow_graph.nodes[task_id]
            task_abbrev = attrs.get("task_abbrev")
            _, compute_site = self.workflow_config.search("computeSite",
                                                          opt={"curvals": {"curr_pipetask": task_abbrev}})
            if compute_site not in already_added or task_abbrev not in already_added[compute_site]:
                activator = Executable(
                    task_abbrev,
                    arch=self.workflow_config["site"][compute_site]["arch"],
                    os=self.workflow_config["site"][compute_site]["os"],
                    installed=True,
                )
                activator.addPFN(PFN("file://%s" % self.workflow_config["runQuantumExec"], compute_site))
                tcat.add(activator)

    def _define_sites(self):
        """Create Pegasus Site Catalog
        """
        scat = self.pegasus_files["sc"]["obj"]
        for site, site_data in self.workflow_config["site"].items():
            scat.add_site(site, arch=site_data["arch"], os=site_data["os"])
            if "directory" in site_data:
                # hack because no Python API
                dir_dict = {}
                for site_dir in site_data["directory"]:
                    dir_dict[site_dir] = {"path": site_data["directory"][site_dir]["path"]}
                scat._sites[site]["directories"] = dir_dict

            # add run attributes to site def to get added to DAG and
            # Pegasus-added jobs
            if "run_attrib" in self.workflow_graph.graph:
                for key, val in self.workflow_graph.graph["run_attrib"].items():
                    scat.add_site_profile(site, namespace=Namespace.CONDOR, key=f"+{key}",
                                          value=f'"{val}"')

            # add config provided site attributes
            if "profile" in site_data:
                for pname, pdata in site_data["profile"].items():
                    for key, val in pdata.items():
                        scat.add_site_profile(site, namespace=pname, key=key, value=val)

    def _add_files_to_rc(self):
        """Add files to Pegasus Replica Catalog
        """
        rc_obj = self.pegasus_files["rc"]["obj"]
        files = [file_ for file_ in self.file_catalog.values() if file_.pfns]
        for file_ in files:
            lfn = file_.name
            for pfn in file_.pfns:
                rc_obj.add(lfn, pfn.url, pfn.site)

    def _write_pegasus_files(self):
        """Write Pegasus Catalogs and DAX to files
        """
        os.makedirs(self.workdir, exist_ok=True)

        # Write down the workflow in DAX format.
        with open(self.pegasus_files["dax"]["name"], "w") as outfh:
            self.pegasus_files["dax"]["obj"].writeXML(outfh)

        # output site catalog
        if self.pegasus_files["sc"]["obj"] is not None:
            self.pegasus_files["sc"]["obj"].write()

        # output transformation catalog
        if self.pegasus_files["tc"]["obj"] is not None:
            self.pegasus_files["tc"]["obj"].write()

        # output replica catalog
        if self.pegasus_files["rc"]["obj"] is not None:
            self.pegasus_files["rc"]["obj"].write()

        self._write_pegasus_properties_file()

    def _run_pegasus_plan(self):
        """Execute pegasus-plan to convert DAX to HTCondor DAG for submission
        """
        cmd = ["pegasus-plan"]
        cmd.append("--verbose")
        cmd.append("--conf %s" % self.pegasus_files["prop"]["name"])
        cmd.append("--dax %s" % self.pegasus_files["dax"]["name"])
        cmd.append("--dir %s/peg" % self.workdir)
        cmd.append("--cleanup none")
        cmd.append("--sites %s" % self.workflow_config["computeSite"])
        cmd.append("--input-dir %s/input --output-dir %s/output" % (self.workdir, self.workdir))
        cmd_str = " ".join(cmd)
        bufsize = 5000
        pegout = "%s/pegasus-plan.out" % self.workdir
        _LOG.info("pegasus-plan output in %s", pegout)
        with open(pegout, "w") as ppfh:
            process = subprocess.Popen(
                shlex.split(cmd_str), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            buf = os.read(process.stdout.fileno(), bufsize).decode()
            while process.poll is None or len(buf) != 0:
                match = re.search(r"pegasus-run\s+(\S+)", buf)
                ppfh.write(buf)
                if match:
                    self.run_id = match.group(1)
                buf = os.read(process.stdout.fileno(), bufsize).decode()
            process.stdout.close()
            process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-plan exited with non-zero exit code (%s)" % process.returncode)

        # Hack - Using profile in sites.xml doesn't add run attributes to DAG submission
        # file (see _define_sites). So adding them here:
        if "run_attrib" in self.workflow_graph.graph:
            subname = f'{self.run_id}/{self.workflow_config["workflowName"]}-0.dag.condor.sub'
            shutil.copyfile(subname, subname + ".orig")
            with open(subname + ".orig", "r") as insubfh:
                with open(subname, "w") as outsubfh:
                    for line in insubfh:
                        if line.strip() == "queue":
                            htc_write_attribs(outsubfh, self.workflow_graph.graph["run_attrib"])
                            htc_write_attribs(outsubfh, {"bps_jobabbrev": "DAG"})
                        outsubfh.write(line)

    def _write_pegasus_properties_file(self):
        """Write Pegasus Properties File
        """

        with open(self.pegasus_files["prop"]["name"], "w") as pfh:
            pfh.write("# This tells Pegasus where to find the Site Catalog.\n")
            pfh.write("pegasus.catalog.site.file=%s\n" % self.pegasus_files["sc"]["name"])
            pfh.write("# This tells Pegasus where to find the Replica Catalog.\n")
            pfh.write("pegasus.catalog.replica=File\n")
            pfh.write("pegasus.catalog.replica.file=%s\n" % self.pegasus_files["rc"]["name"])
            pfh.write("# This tells Pegasus where to find the Transformation Catalog.\n")
            pfh.write("pegasus.catalog.transformation=Text\n")
            pfh.write("pegasus.catalog.transformation.file=%s\n" % self.pegasus_files["tc"]["name"])
            pfh.write("# Run Pegasus in shared file system mode.\n")
            pfh.write("pegasus.data.configuration=sharedfs\n")
            pfh.write("# Make Pegasus use links instead of transfering files.\n")
            pfh.write("pegasus.transfer.*.impl=Transfer\n")
            pfh.write("pegasus.transfer.links=true\n")
            # pfh.write("# Use a timestamp as a name for the submit directory.\n")
            # pfh.write("pegasus.dir.useTimestamp=true\n")
            # pfh.write("pegasus.dir.storage.deep=true\n")

    def submit(self):
        """Submit run to WMS by calling pegasus-run
        """
        cmd = ["pegasus-run"]
        cmd.append(self.run_id)
        cmd_str = " ".join(cmd)
        bufsize = 5000
        with open("%s/pegasus-run.out" % self.workdir, "w") as ppfh:
            process = subprocess.Popen(
                shlex.split(cmd_str), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            buf = os.read(process.stdout.fileno(), bufsize).decode()
            while process.poll is None or len(buf) != 0:
                ppfh.write(buf)
                buf = os.read(process.stdout.fileno(), bufsize).decode()
            process.stdout.close()
            process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-run exited with non-zero exit code (%s)" % process.returncode)
