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
from ...bps_utils import chdir
from ..HTCondor.lssthtc import htc_write_attribs
from ...wms_service import BaseWmsService, BaseWmsWorkflow

_LOG = logging.getLogger()


class PegasusService(BaseWmsService):
    """Pegasus version of workflow engine
    Parameters
    ----------
    config : `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def prepare(self, config, generic_workflow, out_prefix=None):
        """Create submission for a generic workflow
        in a specific WMS
        """
        peg_workflow = PegasusWorkflow.from_generic_workflow(config, generic_workflow)
        peg_workflow.write(out_prefix)
        peg_workflow._run_pegasus_plan(out_prefix, generic_workflow.run_attrs)
        return peg_workflow


    def submit(self, peg_workflow):
        """Submit a single WMS workflow
        """
        with chdir(peg_workflow.submit_path):
            _LOG.info("Submitting from directory: %s", os.getcwd())
            command = f"pegasus-run {peg_workflow.run_id}"
            with open(f"{peg_workflow.name}_pegasus-run.out", "w") as outfh:
                process = subprocess.Popen(shlex.split(command), shell=False, stdout=outfh,
                                           stderr=subprocess.STDOUT)
                process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-run exited with non-zero exit code (%s)" % process.returncode)

        # Note: No need to save run id as the same as the run id generated when running pegasus-plan earlier

    def status(self, wms_workflow_id=None):
        """Query WMS for status of submitted WMS
        """
        raise NotImplementedError

    def history(self, wms_workflow_id=None):
        """Query WMS for status of completed WMS
        """
        raise NotImplementedError


class PegasusWorkflow(BaseWmsWorkflow):
    """Single Pegasus Workflow

    Parameters
    ----------
    gen_workflow : `networkx.DiGraph`
        The generic workflow graph (e.g., has executable name and arguments)
    config : `lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, name, config):
        # config, run_id, submit_path
        super().__init__(name, config)
        self.dax = ADAG(name)

        self.replica_catalog = None
        self.sites_catalog = None
        self.transformation_catalog = None
        self._init_catalogs()
        self.properties_filename = None
        self.dax_filename = None

    def _init_catalogs(self):
        # Set workdir in catalogs at write time.  So pass None as value here.

        # Replica Catalog keeps mappings of logical file ids/names (LFN's) to physical file ids/names (PFN's)
        if "rcFile" not in self.config:
            fname = "rc.txt"
            self.replica_catalog = replica_catalog.ReplicaCatalog(None, fname)

        # Transformation Catalog describes all of the executables
        # (called "transformations") used by the workflow.
        if "tcFile" not in self.config:
            fname = "tc.txt"
            self.transformation_catalog = transformation_catalog.TransformationCatalog(None, fname)

        # Note: SitesCatalog needs workdir at initialization to create local site for
        # submit side directory where the output data from the workflow will be stored.
        # So delaying creation of SitesCatalog until all the write function is called
        # with a given output directory.

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow):
        peg_workflow = cls(generic_workflow.name, config)

        peg_files = peg_workflow._create_peg_files(generic_workflow.get_files(data=True, transfer_only=True))

        # Add jobs to the DAX.
        for job_name in generic_workflow:
            gwf_job = generic_workflow.get_job(job_name)
            job = peg_workflow._create_job(generic_workflow, gwf_job, peg_files)
            peg_workflow.dax.addJob(job)

        # Add job dependencies to the DAX.
        for job_name in generic_workflow:
            for child_name in generic_workflow.successors(job_name):
                peg_workflow.dax.depends(parent=peg_workflow.dax.getJob(job_name),
                                         child=peg_workflow.dax.getJob(child_name))

        return peg_workflow

    def _create_peg_files(self, gwf_files):
        """Create Pegasus File objects for all files
        """
        peg_files = {}
        for gwf_file in gwf_files:
            if gwf_file.wms_transfer:
                peg_file = File(gwf_file.name)
                peg_file.addPFN(PFN(f"file://{gwf_file.src_uri}", "local"))
                peg_files[gwf_file.name] = peg_file
        return peg_files

    def _create_peg_executable(self, exec_name, compute_site):
        # search_opts = {'curvals': {'site': compute_site}, 'default': None}
        # arch = self.config.search("arch", opt=search_opts)
        # os = self.config.search("os", opt=search_opts)
        # executable = Executable(exec_name, arch=arch, os=os, installed=True)

        executable = Executable(os.path.basename(exec_name), installed=True)
        executable.addPFN(PFN(f"file://{exec_name}", compute_site))
        return executable

    def _create_job(self, generic_workflow, gwf_job, peg_files):
        """Create Pegasus job corresponding to given GenericWorkflow job.
        """
        _LOG.info("GenericWorkflowJob=%s", gwf_job)

        # Add job command line arguments replacing any file name with
        # respective Pegasus file object.

        args = gwf_job.cmdline.split()

        # Save transformation.
        self.transformation_catalog.add(self._create_peg_executable(args[0], gwf_job.compute_site))

        # Create Pegasus Job.
        job = Job(os.path.basename(args[0]), id=gwf_job.name, node_label=gwf_job.label)

        # Pegasus Job Arguments must include executable

        # Must replace filenames on command line with corresponding Pegasus File object
        logical_file_names = list(set(peg_files) & set(args))
        if logical_file_names:
            indices = [args.index(lfn) for lfn in logical_file_names]
            for idx, lfn in zip(indices, logical_file_names):
                args[idx] = peg_files[lfn]

        job.addArguments(*args)

        # Add extra job attributes
        for key, val in gwf_job.profile.items():
            job.addProfile(Profile(Namespace.CONDOR, key, val))

        for key, val in gwf_job.environment.items():
            job.addProfile(Profile("env", key, val))

        for key, val in gwf_job.attrs.items():
            job.addProfile(Profile(Namespace.CONDOR, key=f"+{key}", value=f'"{val}"'))

        # Specify job's inputs.
        inputs = generic_workflow.get_job_inputs(gwf_job.name, data=True, transfer_only=True)
        for gwf_file in inputs:
            peg_file = peg_files[gwf_file.name]
            job.uses(peg_file, link=Link.INPUT)
            for pfn in peg_file.pfns:
                self.replica_catalog.add(peg_file.name, pfn.url, pfn.site)

        # Specify job's outputs
        outputs = generic_workflow.get_job_outputs(gwf_job.name, data=True, transfer_only=True)
        for gwf_file in outputs:
            peg_file = peg_files[gwf_file.name]
            job.uses(peg_file, link=Link.OUTPUT)
            for pfn in peg_file.pfns:
                self.replica_catalog.add(peg_file.name, pfn.url, pfn.site)

        return job

    def _define_sites(self, out_prefix):
        """Create Pegasus Site Catalog

        Note: SitesCatalog needs workdir at initialization to create local site for
        submit side directory where the output data from the workflow will be stored.
        """
        self.sites_catalog = sites_catalog.SitesCatalog(out_prefix, f"{self.name}_sites.xml")

        # Adding information for all sites defined in config instead of
        # limiting to those actually used by the workflow
        for site, site_data in self.config["site"].items():
            self.sites_catalog.add_site(site, arch=site_data["arch"], os=site_data["os"])
            if "directory" in site_data:
                # hack because no Python API
                dir_dict = {}
                for site_dir in site_data["directory"]:
                    dir_dict[site_dir] = {"path": site_data["directory"][site_dir]["path"]}
                self.sites_catalog._sites[site]["directories"] = dir_dict

            # add config provided site attributes
            if "profile" in site_data:
                for pname, pdata in site_data["profile"].items():
                    for key, val in pdata.items():
                        self.sites_catalog.add_site_profile(site, namespace=pname, key=key, value=val)

    def write(self, out_prefix):
        """Write Pegasus Catalogs and DAX to files
        """
        self.submit_path = out_prefix

        # filenames needed for properties file
        filenames = {}

        # Write down the workflow in DAX format.
        self.dax_filename = f"{self.dax.name}.dax"
        if out_prefix is not None:
            os.makedirs(out_prefix, exist_ok=True)
            self.dax_filename = os.path.join(out_prefix, self.dax_filename)
        with open(self.dax_filename, "w") as outfh:
            self.dax.writeXML(outfh)

        # output site catalog
        filename = f"{self.name}_sites.xml"
        if "scFile" not in self.config:
            self._define_sites(out_prefix)
            self.sites_catalog.workflow_dir = out_prefix
            self.sites_catalog.filename = filename
            self.sites_catalog.write()
        else:
            shutil.copy(self.config["sitesFile"], os.path.join(self.submit_path, filename))
        filenames['sites'] = filename

        # output transformation catalog
        filename = f"{self.name}_tc.txt"
        if self.transformation_catalog is not None:
            self.transformation_catalog.workflow_dir = out_prefix
            self.transformation_catalog.filename = filename
            self.transformation_catalog.write()
        else:
            shutil.copy(self.config["tcFile"], os.path.join(self.submit_path, filename))
        filenames['transformation'] = filename

        # output replica catalog
        filename = f"{self.name}_rc.txt"
        if self.replica_catalog is not None:
            self.replica_catalog.workflow_dir = out_prefix
            self.replica_catalog.filename = filename
            self.replica_catalog.write()
        else:
            shutil.copy(self.config["tcFile"], os.path.join(self.submit_path, filename))
        filenames['replica'] = filename

        self.properties_filename = self._write_properties_file(out_prefix, filenames)

    def _run_pegasus_plan(self, out_prefix, run_attr):
        """Execute pegasus-plan to convert DAX to HTCondor DAG for submission
        """
        cmd = ["pegasus-plan"]
        cmd.append("--verbose")
        cmd.append(f"--conf {self.properties_filename}")
        cmd.append(f"--dax {self.dax_filename}")
        cmd.append(f"--dir {out_prefix}/peg")
        cmd.append("--cleanup none")
        cmd.append(f"--sites {self.config['computeSite']}")
        cmd.append(f"--input-dir {out_prefix}/input --output-dir {out_prefix}/output")
        cmd_str = " ".join(cmd)
        bufsize = 5000
        _LOG.info("Plan command: %s", cmd_str)
        pegout = f"{self.submit_path}/{self.name}_pegasus-plan.out"
        with chdir(self.submit_path):
            _LOG.info("pegasus-plan in directory: %s", os.getcwd())
            _LOG.info("pegasus-plan output in %s", pegout)
            with open(pegout, "w") as pegfh:
                pegfh.write(f"Command: {cmd_str}\n\n")
                process = subprocess.run(shlex.split(cmd_str), shell=False, stdout=pegfh,
                                         stderr=subprocess.STDOUT)
                if process.returncode != 0:
                    print(f"Error trying to generate Pegasus files.  See {pegout}.")
                    raise RuntimeError("pegasus-plan exited with non-zero exit code (%s)" % process.returncode)

            # Grab run id from pegasus-plan output and save
            with open(pegout, "r") as pegfh:
                for line in pegfh:
                    match = re.search(r"pegasus-run\s+(\S+)", line)
                    if match:
                        self.run_id = match.group(1)
                        break

        # Hack - Using profile in sites.xml doesn't add run attributes to DAG submission
        # file (see _define_sites). So adding them here:
        if run_attr is not None:
            subname = f'{self.run_id}/{self.name}-0.dag.condor.sub'
            shutil.copyfile(subname, subname + ".orig")
            with open(subname + ".orig", "r") as insubfh:
                with open(subname, "w") as outsubfh:
                    for line in insubfh:
                        if line.strip() == "queue":
                            htc_write_attribs(outsubfh, run_attr)
                            htc_write_attribs(outsubfh, {"bps_joblabel": "DAG"})
                        outsubfh.write(line)

    def _write_properties_file(self, out_prefix, filenames):
        """Write Pegasus Properties File
        """
        properties = f"{self.name}_pegasus.properties"
        if out_prefix is not None:
            properties = os.path.join(out_prefix, properties)
        with open(properties, "w") as outfh:
            outfh.write("# This tells Pegasus where to find the Site Catalog.\n")
            outfh.write(f"pegasus.catalog.site.file={filenames['sites']}\n")

            outfh.write("# This tells Pegasus where to find the Replica Catalog.\n")
            outfh.write(f"pegasus.catalog.replica.file={filenames['replica']}\n")

            outfh.write("# This tells Pegasus where to find the Transformation Catalog.\n")
            outfh.write("pegasus.catalog.transformation=Text\n")
            outfh.write(f"pegasus.catalog.transformation.file={filenames['transformation']}\n")

            outfh.write("# Run Pegasus in shared file system mode.\n")
            outfh.write("pegasus.data.configuration=sharedfs\n")

            outfh.write("# Make Pegasus use links instead of transferring files.\n")
            outfh.write("pegasus.transfer.*.impl=Transfer\n")
            outfh.write("pegasus.transfer.links=true\n")

            # outfh.write("# Use a timestamp as a name for the submit directory.\n")
            # outfh.write("pegasus.dir.useTimestamp=true\n")
            # outfh.write("pegasus.dir.storage.deep=true\n")

        return properties
