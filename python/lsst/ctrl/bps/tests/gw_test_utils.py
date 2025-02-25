# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
"""GenericWorkflow-related utilities to support ctrl_bps testing."""

__all__ = [
    "make_3_label_workflow",
    "make_3_label_workflow_groups_sort",
    "make_3_label_workflow_noop_sort",
]

from collections import Counter

from lsst.ctrl.bps import (
    GenericWorkflow,
    GenericWorkflowExec,
    GenericWorkflowGroup,
    GenericWorkflowJob,
    GenericWorkflowNoopJob,
)


def make_3_label_workflow(workflow_name: str, final: bool) -> GenericWorkflow:
    """Create a simple 3 label test workflow.

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwexec = GenericWorkflowExec("exec1", "/usr/bin/uptime", False)
    gwf = GenericWorkflow(workflow_name)
    job = GenericWorkflowJob("pipetaskInit", label="pipetaskInit", executable=gwexec)
    gwf.add_job(job)
    for visit, vgroup in [
        (10001, "2024-06-26T07:28:26.289"),
        (10002, "2024-06-26T07:29:06.969"),
        (301, "2024-06-26T07:27:45.775"),
    ]:  # 301 is to ensure numeric sorting
        for detector in [10, 11]:
            prev_name = "pipetaskInit"
            for label in ["label1", "label2", "label3"]:
                name = f"{label}_{visit}_{detector}"
                job = GenericWorkflowJob(
                    name,
                    label=label,
                    executable=gwexec,
                    quanta_counts=Counter({label: 1}),
                    tags={"visit": visit, "detector": detector, "group": vgroup},
                )
                gwf.add_job(job, [prev_name], None)
                prev_name = name

    if final:
        gwexec = GenericWorkflowExec("finalJob.bash", "finalJob.bash", True)
        job = GenericWorkflowJob("finalJob", label="finalJob", executable=gwexec)
        gwf.add_final(job)

    return gwf


def make_3_label_workflow_noop_sort(workflow_name: str, final: bool) -> GenericWorkflow:
    """Create a test workflow that has noop jobs.

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwexec = GenericWorkflowExec("exec1", "/usr/bin/uptime", False)
    gwf = GenericWorkflow(workflow_name)
    job = GenericWorkflowJob("pipetaskInit", label="pipetaskInit", executable=gwexec)
    gwf.add_job(job)
    prev_noop: GenericWorkflowNoopJob | None = None
    for visit in sorted([10001, 10002, 301]):  # 301 is to ensure numeric sorting
        if visit != 10002:
            noop_job = GenericWorkflowNoopJob(f"noop_order1_{visit}", "order1")
            gwf.add_job(noop_job)
        for detector in [10, 11]:
            prev_name = "pipetaskInit"
            for label in ["label1", "label2", "label3"]:
                name = f"{label}_{visit}_{detector}"
                job = GenericWorkflowJob(
                    name, label=label, executable=gwexec, tags={"visit": visit, "detector": detector}
                )
                gwf.add_job(job, [prev_name], None)
                if label == "label1" and prev_noop:
                    gwf.add_job_relationships([prev_noop.name], [name])
                if label == "label2" and visit != 10002:
                    gwf.add_job_relationships([name], [noop_job.name])
                prev_name = name
        prev_noop = noop_job

    if final:
        gwexec = GenericWorkflowExec("finalJob.bash", "finalJob.bash", True)
        job = GenericWorkflowJob("finalJob", label="finalJob", executable=gwexec)
        gwf.add_final(job)
    return gwf


def make_3_label_workflow_groups_sort(workflow_name: str, final: bool) -> GenericWorkflow:
    """Create a test workflow that has job groups.

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwexec = GenericWorkflowExec("exec1", "/usr/bin/uptime", False)
    gwf = GenericWorkflow(workflow_name)
    job = GenericWorkflowJob("pipetaskInit", label="pipetaskInit", executable=gwexec)
    gwf.add_job(job)
    prev_group: GenericWorkflowGroup | None = None
    for visit in sorted([10001, 10002, 301]):  # 301 is to ensure numeric sorting
        job_group = GenericWorkflowGroup(f"group_order1_{visit}", "order1")
        for detector in [10, 11]:
            prev_name: str | None = None
            for label in ["label1", "label2"]:
                name = f"{label}_{visit}_{detector}"
                job = GenericWorkflowJob(
                    name, label=label, executable=gwexec, tags={"visit": visit, "detector": detector}
                )
                job_group.add_job(job)
                if prev_name:
                    job_group.add_job_relationships(prev_name, name)
                prev_name = name
        gwf.add_job(job_group, ["pipetaskInit"], None)
        if prev_group:
            gwf.add_job_relationships([prev_group.name], [job_group.name])

        prev_group = job_group
    for visit in sorted([10001, 10002, 301]):  # 301 is to ensure numeric sorting
        for detector in [10, 11]:
            for label in ["label3"]:
                name = f"{label}_{visit}_{detector}"
                job = GenericWorkflowJob(
                    name, label=label, executable=gwexec, tags={"visit": visit, "detector": detector}
                )
                gwf.add_job(job, [f"group_order1_{visit}"], None)

    if final:
        gwexec = GenericWorkflowExec("finalJob.bash", "finalJob.bash", True)
        job = GenericWorkflowJob("finalJob", label="finalJob", executable=gwexec)
        gwf.add_final(job)

    return gwf
