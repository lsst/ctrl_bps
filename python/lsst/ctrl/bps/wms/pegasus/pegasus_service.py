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
import copy
import re
import subprocess
import shlex
import shutil
import logging

from Pegasus.DAX3 import ADAG, File, Job, Link, PFN, Executable, Profile, Namespace
from Pegasus.catalogs import replica_catalog, sites_catalog, transformation_catalog

from lsst.ctrl.bps.bps_utils import chdir
from lsst.ctrl.bps.wms_service import BaseWmsService, BaseWmsWorkflow
from lsst.ctrl.bps.wms.htcondor.lssthtc import htc_write_attribs
from lsst.ctrl.bps.wms.htcondor.htcondor_service import HTCondorService

_LOG = logging.getLogger(__name__)


class PegasusService(BaseWmsService):
    """Pegasus version of workflow engine
    """
    def prepare(self, config, generic_workflow, out_prefix=None):
        """Create submission for a generic workflow
        in a specific WMS

        Parameters
        ----------
        config : `~lsst.ctrl.bps.BPSConfig`
            BPS configuration that includes necessary submit/runtime information
        generic_workflow :  `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments)
        out_prefix : `str`
            The root directory into which all WMS-specific files are written.

        Returns
        ----------
        workflow : `~lsst.ctrl.bps.wms.pegasus.pegasus_service.PegasusWorkflow`
            A workflow ready for Pegasus to run.
        """
        service_class = f"{self.__class__.__module__}.{self.__class__.__name__}"
        peg_workflow = PegasusWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                             service_class)
        peg_workflow.write(out_prefix)
        peg_workflow.run_pegasus_plan(out_prefix, generic_workflow.run_attrs)
        return peg_workflow

    def submit(self, workflow):
        """Submit a single WMS workflow

        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single HTCondor workflow to submit
        """
        with chdir(workflow.submit_path):
            _LOG.info("Submitting from directory: %s", os.getcwd())
            command = f"pegasus-run {workflow.run_id}"
            with open(f"{workflow.name}_pegasus-run.out", "w") as outfh:
                process = subprocess.Popen(shlex.split(command), shell=False, stdout=outfh,
                                           stderr=subprocess.STDOUT)
                process.wait()

        if process.returncode != 0:
            raise RuntimeError("pegasus-run exited with non-zero exit code (%s)" % process.returncode)

        # Note: No need to save run id as the same as the run id generated when running pegasus-plan earlier

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True, pass_thru=None):
        """Query WMS for list of submitted WMS workflows/jobs.

        This should be a quick lookup function to create list of jobs for
        other functions.

        Parameters
        ----------
        wms_id : `int` or `str`, optional
            Id or path that can be used by WMS service to look up job.
        user : `str`, optional
            User whose submitted jobs should be listed.
        require_bps : `bool`, optional
            Whether to require jobs returned in list to be bps-submitted jobs.
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        -------
        job_ids : `list` of `Any`
            Only job ids to be used by cancel and other functions.  Typically
            this means top-level jobs (i.e., not children jobs).
        """
        htc_service = HTCondorService(self.config)
        return htc_service.list_submitted_jobs(wms_id, user, require_bps, pass_thru)

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """Query WMS for status of submitted WMS workflows
         Parameters
         ----------
         wms_workflow_id : `int` or `str`, optional
             Id that can be used by WMS service to look up status.
         user : `str`, optional
             Limit report to submissions by this particular user
         hist : `int`, optional
             Number of days to expand report to include finished WMS workflows.
         pass_thru : `str`, optional
             Additional arguments to pass through to the specific WMS service.

         Returns
         -------
         run_reports : `dict` of `BaseWmsReport`
             Status information for submitted WMS workflows
         message : `str`
             Message to user on how to find more status information specific to WMS
         """
        htc_service = HTCondorService(self.config)
        return htc_service.report(wms_workflow_id, user, hist, pass_thru)

    def cancel(self, wms_id, pass_thru=None):
        """Cancel submitted workflows/jobs.

        Parameters
        ----------
        wms_id : `str`
            ID or path of job that should be canceled.
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        --------
        deleted : `bool`
            Whether successful deletion or not.  Currently, if any doubt or any
            individual jobs not deleted, return False.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        _LOG.debug("Canceling wms_id = %s", wms_id)

        # if wms_id is a numeric HTCondor id, use HTCondor plugin to delete
        try:
            float(wms_id)
            htc_service = HTCondorService(self.config)
            deleted, message = htc_service.cancel(wms_id, pass_thru)
        except ValueError:
            command = f"pegasus-remove {wms_id}"
            _LOG.debug(command)
            completed_process = subprocess.run(shlex.split(command), shell=False, check=False,
                                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            _LOG.debug(completed_process.stdout)
            _LOG.debug("Return code = %s", completed_process.returncode)

            if completed_process.returncode != 0:
                deleted = False
                m = re.match(b"443", completed_process.stdout)
                if m:
                    message = "no such bps job in batch queue"
                else:
                    message = f"pegasus-remove exited with non-zero exit code {completed_process.returncode}"
                print("XXX", completed_process.stdout.decode(), "XXX")
                print(message)
            else:
                deleted = True

        return deleted, message


class PegasusWorkflow(BaseWmsWorkflow):
    """Single Pegasus Workflow

    Parameters
    ----------
    name : `str`
        Name of Workflow
    config : `~lsst.ctrl.bps.bps_config.BpsConfig`
        BPS configuration that includes necessary submit/runtime information
    """
    def __init__(self, name, config):
        # config, run_id, submit_path
        super().__init__(name, config)
        self.dax = ADAG(name)
        self.run_attrs = None

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
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited.
        peg_workflow = cls(generic_workflow.name, config)
        peg_workflow.run_attrs = copy.deepcopy(generic_workflow.run_attrs)
        peg_workflow.run_attrs['bps_wms_service'] = service_class
        peg_workflow.run_attrs['bps_wms_workflow'] = f"{cls.__module__}.{cls.__name__}"

        # Create initial Pegasus File objects for all files that WMS must handle
        peg_files = {}
        for gwf_file in generic_workflow.get_files(data=True, transfer_only=True):
            if gwf_file.wms_transfer:
                peg_file = File(gwf_file.name)
                peg_file.addPFN(PFN(f"file://{gwf_file.src_uri}", "local"))
                peg_files[gwf_file.name] = peg_file

        # Add jobs to the DAX.
        for job_name in generic_workflow:
            gwf_job = generic_workflow.get_job(job_name)
            job = peg_workflow.create_job(generic_workflow, gwf_job, peg_files)
            peg_workflow.dax.addJob(job)

        # Add job dependencies to the DAX.
        for job_name in generic_workflow:
            for child_name in generic_workflow.successors(job_name):
                peg_workflow.dax.depends(parent=peg_workflow.dax.getJob(job_name),
                                         child=peg_workflow.dax.getJob(child_name))

        return peg_workflow

    def create_job(self, generic_workflow, gwf_job, peg_files):
        """Create a Pegasus job corresponding to the given GenericWorkflow job.

        Parameters
        ----------
        generic_workflow : `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic workflow that is being converted.
        gwf_job : `~lsst.ctrl.bps.generic_workflow.GenericWorkflowJob`
            The generic job to convert to a Pegasus job.
        peg_files : `dict` of `Pegasus.DAX3.File`
            Pegasus Files needed when creating Pegasus Job.

        Returns
        -------
        job : `Pegasus.DAX3.Job`
            Pegasus job created from the generic workflow job.
        """
        _LOG.debug("GenericWorkflowJob=%s", gwf_job)
        _LOG.debug("%s gwf_job.cmdline = %s", gwf_job.name, gwf_job.cmdline)
        cmd_parts = gwf_job.cmdline.split(' ', 1)

        # Save transformation.
        executable = Executable(os.path.basename(cmd_parts[0]), installed=True)
        executable.addPFN(PFN(f"file://{cmd_parts[0]}", gwf_job.compute_site))
        self.transformation_catalog.add(executable)

        # Create Pegasus Job.
        job = Job(os.path.basename(cmd_parts[0]), id=gwf_job.name, node_label=gwf_job.label)

        # Replace filenames on command line with corresponding Pegasus File object
        if len(cmd_parts) > 1:
            args = cmd_parts[1].split()
            logical_file_names = list(set(peg_files) & set(args))
            if logical_file_names:
                indices = [args.index(lfn) for lfn in logical_file_names]
                for idx, lfn in zip(indices, logical_file_names):
                    args[idx] = peg_files[lfn]

            job.addArguments(*args)
        else:
            _LOG.warning("Job %s does not have any arguments", gwf_job.name)

        if gwf_job.request_memory:  # MB
            job.addProfile(Profile(Namespace.CONDOR, "request_memory", gwf_job.request_memory))
        if gwf_job.request_cpus:  # cores
            job.addProfile(Profile(Namespace.CONDOR, "request_cpus", gwf_job.request_cpus))
        if gwf_job.request_disk:  # MB
            job.addProfile(Profile(Namespace.CONDOR, "request_disk", gwf_job.request_disk))
        if gwf_job.priority:  # MB
            job.addProfile(Profile(Namespace.CONDOR, "priority", gwf_job.priority))

        # Add extra job attributes
        for key, value in gwf_job.profile.items():
            job.addProfile(Profile(Namespace.CONDOR, key, value))

        for key, value in gwf_job.environment.items():
            job.addProfile(Profile(Namespace.ENV, key, value))

        # Add run attributes
        for key, value in self.run_attrs.items():
            job.addProfile(Profile(Namespace.CONDOR, key=f"+{key}", value=f'"{value}"'))

        for key, value in gwf_job.attrs.items():
            _LOG.debug("create_job: attrs = %s", gwf_job.attrs)
            job.addProfile(Profile(Namespace.CONDOR, key=f"+{key}", value=f'"{value}"'))

        job.addProfile(Profile(Namespace.CONDOR, key="+bps_job_name", value=f'"{gwf_job.name}"'))
        job.addProfile(Profile(Namespace.CONDOR, key="+bps_job_label", value=f'"{gwf_job.label}"'))
        job.addProfile(Profile(Namespace.CONDOR, key="+bps_job_quanta", value=f'"{gwf_job.quanta_summary}"'))

        # Specify job's inputs.
        for gwf_file in generic_workflow.get_job_inputs(gwf_job.name, data=True, transfer_only=True):
            peg_file = peg_files[gwf_file.name]
            job.uses(peg_file, link=Link.INPUT)
            for pfn in peg_file.pfns:
                self.replica_catalog.add(peg_file.name, pfn.url, pfn.site)

        # Specify job's outputs
        for gwf_file in generic_workflow.get_job_outputs(gwf_job.name, data=True, transfer_only=True):
            peg_file = peg_files[gwf_file.name]
            job.uses(peg_file, link=Link.OUTPUT)
            for pfn in peg_file.pfns:
                self.replica_catalog.add(peg_file.name, pfn.url, pfn.site)

        return job

    def _define_sites(self, out_prefix):
        """Create Pegasus Site Catalog

        Note: SitesCatalog needs workdir at initialization to create local site for
        submit side directory where the output data from the workflow will be stored.

        Parameters
        ----------
        out_prefix : `str`
            Directory prefix for the site catalog file.
        """
        self.sites_catalog = sites_catalog.SitesCatalog(out_prefix, f"{self.name}_sites.xml")

        # Adding information for all sites defined in config instead of
        # limiting to those actually used by the workflow
        for site, site_data in self.config["site"].items():
            self.sites_catalog.add_site(site, arch=site_data["arch"], os=site_data["os"])
            if "directory" in site_data:
                # Workaround because no Python API
                dir_dict = {}
                for site_dir in site_data["directory"]:
                    dir_dict[site_dir] = {"path": site_data["directory"][site_dir]["path"]}
                self.sites_catalog._sites[site]["directories"] = dir_dict

            # add config provided site attributes
            if "profile" in site_data:
                for pname, pdata in site_data["profile"].items():
                    for key, val in pdata.items():
                        self.sites_catalog.add_site_profile(site, namespace=pname, key=key, value=val)
            self.sites_catalog.add_site_profile(site, namespace=Namespace.DAGMAN, key="NODE_STATUS_FILE",
                                                value=f"{self.name}.node_status")

    def write(self, out_prefix):
        """Write Pegasus Catalogs and DAX to files

        Parameters
        ----------
        out_prefix : `str`
            Directory prefix for all the Pegasus workflow files.
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

    def run_pegasus_plan(self, out_prefix, run_attr):
        """Execute pegasus-plan to convert DAX to HTCondor DAG for submission

        Parameters
        ----------
        out_prefix : `str`
            Root directory in which to output all files
        run_attr : `dict`
            Attributes to add to main DAG
        """
        cmd = f"pegasus-plan --verbose --conf {self.properties_filename} --dax {self.dax_filename} --dir " \
              f"{out_prefix}/peg --cleanup none --sites {self.config['computeSite']} " \
              f"--input-dir {out_prefix}/input --output-dir {out_prefix}/output"
        _LOG.debug("Plan command: %s", cmd)
        pegout = f"{self.submit_path}/{self.name}_pegasus-plan.out"
        with chdir(self.submit_path):
            _LOG.debug("pegasus-plan in directory: %s", os.getcwd())
            _LOG.debug("pegasus-plan output in %s", pegout)
            with open(pegout, "w") as pegfh:
                print(f"Command: {cmd}\n", file=pegfh)  # Note: want blank line
                process = subprocess.run(shlex.split(cmd), shell=False, stdout=pegfh,
                                         stderr=subprocess.STDOUT, check=False)
                if process.returncode != 0:
                    print(f"Error trying to generate Pegasus files.  See {pegout}.")
                    raise RuntimeError(f"pegasus-plan exited with non-zero exit code ({process.returncode})")

            # Grab run id from pegasus-plan output and save
            with open(pegout, "r") as pegfh:
                for line in pegfh:
                    match = re.search(r"pegasus-run\s+(\S+)", line)
                    if match:
                        self.run_id = match.group(1)
                        break

        # Hack - Using profile in sites.xml doesn't add run attributes to DAG submission
        # file. So adding them here:
        if run_attr is not None:
            subname = f'{self.run_id}/{self.name}-0.dag.condor.sub'
            shutil.copyfile(subname, subname + ".orig")
            with open(subname + ".orig", "r") as infh:
                with open(subname, "w") as outfh:
                    for line in infh:
                        line = line.strip()
                        if line == "queue":
                            htc_write_attribs(outfh, run_attr)
                            htc_write_attribs(outfh, {"bps_job_label": "DAG"})
                        print(line, file=outfh)

    def _write_properties_file(self, out_prefix, filenames):
        """Write Pegasus Properties File

        Parameters
        ----------
        out_prefix : `str`
            Directory prefix for properties file.
        filenames : `dict` of `str`
            Mapping of Pegasus file keys to filenames.

        Returns
        -------
        properties : `str`
            Filename of the pegasus properties file.
        """
        properties = f"{self.name}_pegasus.properties"
        if out_prefix is not None:
            properties = os.path.join(out_prefix, properties)
        with open(properties, "w") as outfh:
            print("# This tells Pegasus where to find the Site Catalog.", file=outfh)
            print(f"pegasus.catalog.site.file={filenames['sites']}", file=outfh)

            print("# This tells Pegasus where to find the Replica Catalog.", file=outfh)
            print(f"pegasus.catalog.replica.file={filenames['replica']}", file=outfh)

            print("# This tells Pegasus where to find the Transformation Catalog.", file=outfh)
            print("pegasus.catalog.transformation=Text", file=outfh)
            print(f"pegasus.catalog.transformation.file={filenames['transformation']}", file=outfh)

            print("# Run Pegasus in shared file system mode.", file=outfh)
            print("pegasus.data.configuration=sharedfs", file=outfh)

            print("# Make Pegasus use links instead of transferring files.", file=outfh)
            print("pegasus.transfer.*.impl=Transfer", file=outfh)
            print("pegasus.transfer.links=true", file=outfh)

        return properties
