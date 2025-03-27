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
    "make_5_label_workflow",
    "make_5_label_workflow_2_groups",
    "make_5_label_workflow_middle_groups",
]

import logging
from collections import Counter
from typing import cast

from lsst.ctrl.bps import (
    GenericWorkflow,
    GenericWorkflowExec,
    GenericWorkflowGroup,
    GenericWorkflowJob,
    GenericWorkflowNodeType,
    GenericWorkflowNoopJob,
)

_LOG = logging.getLogger(__name__)


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


# 301 is to ensure numeric sorting
DEFAULT_DIMS = [
    (10001, 10),
    (10001, 11),
    (10001, 20),
    (10002, 10),
    (10002, 11),
    (10002, 20),
    (301, 10),
    (301, 11),
    (301, 20),
]
DIM_MAPPING = {301: "gval1", 10001: "gval2", 10002: "gval3"}

UNEVEN_LABEL_DIMS = {
    "T1": [(10002, 11), (10002, 20)],
    "T2": [(10001, 11), (10001, 20), (10002, 10), (10002, 11), (10002, 20)],
    "T2b": [(301, 11), (301, 20), (10001, 11), (10001, 20), (10002, 10), (10002, 11), (10002, 20)],
    "T3": DEFAULT_DIMS,
    "T4": DEFAULT_DIMS,
}

EVEN_LABEL_DIMS = {
    "T1": DEFAULT_DIMS,
    "T2": DEFAULT_DIMS,
    "T2b": DEFAULT_DIMS,
    "T3": DEFAULT_DIMS,
    "T4": DEFAULT_DIMS,
}


def make_5_label_workflow(
    workflow_name: str, final: bool, uneven: bool = False, equiv_dims: bool = False
) -> GenericWorkflow:
    """Create a simple 3 label test workflow.

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.
    uneven : `bool`, optional
        Whether some of the jobs for initial tasks are
        not included as if finished in previous run.
    equiv_dims : `bool`, optional
        Whether first label jobs have a different but equivalent
        dim (like group and visit in AP pipeline).

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwexec = GenericWorkflowExec("exec1", "/usr/bin/uptime", False)
    gwf = GenericWorkflow(workflow_name)
    job = GenericWorkflowJob("pipetaskInit", label="pipetaskInit", executable=gwexec)
    gwf.add_job(job)
    if uneven:
        label_dims = UNEVEN_LABEL_DIMS
    else:
        label_dims = EVEN_LABEL_DIMS

    prev_label = "pipetaskInit"
    for label in sorted(label_dims):
        for dim1, dim2 in label_dims[label]:
            tags: dict[str, str | int] = {"detector": dim2}
            # if want to test with equivalent dims (e.g., group and visit)
            if equiv_dims and label == "T1":
                tags["group"] = DIM_MAPPING[dim1]
                name = f"{label}_{DIM_MAPPING[dim1]}_{dim2}"
            else:
                tags["visit"] = dim1
                name = f"{label}_{dim1}_{dim2}"

            job = GenericWorkflowJob(
                name, label=label, executable=gwexec, quanta_counts=Counter({label: 1}), tags=tags
            )
            parents = []
            if label == "T1":
                parents = ["pipetaskInit"]
            elif (dim1, dim2) in label_dims[prev_label]:
                if equiv_dims and label == "T2":
                    prev_name = f"{prev_label}_{DIM_MAPPING[dim1]}_{dim2}"
                else:
                    prev_name = f"{prev_label}_{dim1}_{dim2}"
                parents = [prev_name]
            else:
                parents = ["pipetaskInit"]

            gwf.add_job(job, parents, None)

        if label != "T2b":  # nothing is a descenant of T2b
            prev_label = label

    if final:
        gwexec = GenericWorkflowExec("finalJob.bash", "finalJob.bash", True)
        job = GenericWorkflowJob("finalJob", label="finalJob", executable=gwexec)
        gwf.add_final(job)

    return gwf


def make_5_label_workflow_2_groups(
    workflow_name: str, final: bool, uneven: bool = False, equiv_dims: bool = False, blocking: bool = False
) -> GenericWorkflow:
    """Create a simple 3 label test workflow.

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.
    uneven : `bool`, optional
        Whether some of the jobs for initial tasks are
        not included as if finished in previous run.
    equiv_dims : `bool`, optional
        Whether first label jobs have a different but equivalent
        dim (like group and visit in AP pipeline).
    blocking : `bool`, optional
        Value to use in group nodes.

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwf_orig = make_5_label_workflow("sink_uneven", final, uneven, equiv_dims)

    if uneven:
        label_dims = UNEVEN_LABEL_DIMS
    else:
        label_dims = EVEN_LABEL_DIMS

    # make job lists
    job_lists: dict[str, list[str]] = {}
    group_labels: dict[str, str] = {}

    group_label = "order1"
    for dim1, dim2 in label_dims["T1"]:
        if equiv_dims:
            job_name = f"T1_{DIM_MAPPING[dim1]}_{dim2}"
        else:
            job_name = f"T1_{dim1}_{dim2}"
        group_name = f"group_{group_label}_{dim1}"
        group_labels[group_name] = group_label
        job_lists.setdefault(group_name, []).append(job_name)

    for dim1, dim2 in label_dims["T2"]:
        job_name = f"T2_{dim1}_{dim2}"
        group_name = f"group_{group_label}_{dim1}"
        group_labels[group_name] = group_label
        job_lists.setdefault(group_name, []).append(job_name)

    group_label = "order2"
    for label in ["T3", "T4"]:
        for dim1, dim2 in label_dims[label]:
            job_name = f"{label}_{dim1}_{dim2}"
            group_name = f"group_{group_label}_{dim1}"
            group_labels[group_name] = group_label
            job_lists.setdefault(group_name, []).append(job_name)

    # make groups of jobs
    groups = {}
    for group_name, job_names in job_lists.items():
        if job_names:
            group = GenericWorkflowGroup(group_name, group_labels[group_name], blocking=blocking)
            # Add all jobs first then add edges
            for job_name in job_names:
                group.add_job(gwf_orig.get_job(job_name))

            for name in job_names:
                edges = [(name, p) for p in gwf_orig.predecessors(name) if p in job_names]
                group.add_edges_from(edges)
            groups[group_name] = group

    gwf = GenericWorkflow(workflow_name)

    # add main workflow nodes
    gwf.add_job(gwf_orig.get_job("pipetaskInit"))
    for dim1, dim2 in label_dims["T2b"]:
        job_name = f"T2b_{dim1}_{dim2}"
        gwf.add_job(gwf_orig.get_job(job_name))

    for group in groups.values():
        gwf.add_job(group)

    # add main workflow edges
    edges = [
        ("pipetaskInit", "group_order1_10001"),
        ("pipetaskInit", "group_order1_10002"),
        ("group_order1_10001", "group_order2_10001"),
        ("group_order1_10002", "group_order2_10002"),
        ("group_order1_10001", "T2b_10001_11"),
        ("group_order1_10001", "T2b_10001_20"),
        ("group_order1_10002", "T2b_10002_10"),
        ("group_order1_10002", "T2b_10002_11"),
        ("group_order1_10002", "T2b_10002_20"),
        # group order dependencies
        ("group_order1_10001", "group_order1_10002"),
        ("group_order2_301", "group_order2_10001"),
        ("group_order2_10001", "group_order2_10002"),
    ]

    if uneven:
        edges.extend(
            [
                ("pipetaskInit", "T2b_301_11"),
                ("pipetaskInit", "T2b_301_20"),
                ("pipetaskInit", "group_order2_301"),
                ("pipetaskInit", "group_order2_10001"),
            ]
        )
    else:
        edges.extend(
            [
                ("pipetaskInit", "group_order1_301"),
                ("group_order1_301", "group_order1_10001"),
                ("group_order1_301", "group_order2_301"),
                ("group_order1_301", "T2b_301_10"),
                ("group_order1_301", "T2b_301_11"),
                ("group_order1_301", "T2b_301_20"),
                ("group_order1_10001", "T2b_10001_10"),
            ]
        )
    gwf.add_edges_from(edges)

    if final:
        job = cast(GenericWorkflowJob, gwf_orig.get_final())
        gwf.add_final(job)

    return gwf


def make_5_label_workflow_middle_groups(
    workflow_name: str, final: bool, uneven: bool = False, equiv_dims: bool = False, blocking: bool = False
) -> GenericWorkflow:
    """Create a test workflow with a group in middle of workflow
       (T2, T2b, and T3).

    Parameters
    ----------
    workflow_name : `str`
        Name of the test workflow.
    final : `bool`
        Whether to add a final job.
    uneven : `bool`, optional
        Whether some of the jobs for initial tasks are
        not included as if finished in previous run.
    equiv_dims : `bool`, optional
        Whether first label jobs have a different but equivalent
        dim (like group and visit in AP pipeline).
    blocking : `bool`, optional
        Value to use in group nodes.

    Returns
    -------
    gwf : `lsst.ctrl.bps.GenericWorkflow`
        The test workflow.
    """
    gwf_orig = make_5_label_workflow(workflow_name, final, uneven, equiv_dims)

    if uneven:
        label_dims = UNEVEN_LABEL_DIMS
    else:
        label_dims = EVEN_LABEL_DIMS

    # make job lists
    job_lists: dict[str, list[str]] = {}
    group_labels: dict[str, str] = {}

    group_label = "mid"
    for label in ["T2", "T2b", "T3"]:
        for dim1, dim2 in label_dims[label]:
            job_name = f"{label}_{dim1}_{dim2}"
            group_name = f"group_{group_label}_{dim1}"
            group_labels[group_name] = group_label
            job_lists.setdefault(group_name, []).append(job_name)

    # make groups of jobs
    groups = {}
    for group_name, job_names in job_lists.items():
        if job_names:
            group = GenericWorkflowGroup(group_name, group_labels[group_name], blocking=blocking)
            # Add all jobs first then add edges
            for job_name in job_names:
                group.add_job(gwf_orig.get_job(job_name))

            for name in job_names:
                edges = [(name, p) for p in gwf_orig.predecessors(name) if p in job_names]
                group.add_edges_from(edges)

            groups[group_name] = group

    gwf = GenericWorkflow(workflow_name)

    # add main workflow nodes
    gwf.add_job(gwf_orig.get_job("pipetaskInit"))
    for label in ["T1", "T4"]:
        for dim1, dim2 in label_dims[label]:
            if equiv_dims and label == "T1":
                job_name = f"T1_{DIM_MAPPING[dim1]}_{dim2}"
            else:
                job_name = f"{label}_{dim1}_{dim2}"
            gwf.add_job(gwf_orig.get_job(job_name))

    for group in groups.values():
        gwf.add_job(group)

    # add main workflow edges
    edges = [
        ("group_mid_301", "T4_301_10"),
        ("group_mid_301", "T4_301_11"),
        ("group_mid_301", "T4_301_20"),
        ("group_mid_10001", "T4_10001_10"),
        ("group_mid_10001", "T4_10001_11"),
        ("group_mid_10001", "T4_10001_20"),
        ("group_mid_10002", "T4_10002_10"),
        ("group_mid_10002", "T4_10002_11"),
        ("group_mid_10002", "T4_10002_20"),
        # group order dependencies
        ("group_mid_301", "group_mid_10001"),
        ("group_mid_10001", "group_mid_10002"),
    ]

    if uneven:
        if equiv_dims:
            edges.extend(
                [
                    ("pipetaskInit", "T1_gval3_11"),
                    ("pipetaskInit", "T1_gval3_20"),
                    ("T1_gval3_11", "group_mid_10002"),
                    ("T1_gval3_20", "group_mid_10002"),
                ]
            )
        else:
            edges.extend(
                [
                    ("pipetaskInit", "T1_10002_11"),
                    ("pipetaskInit", "T1_10002_20"),
                    ("T1_10002_11", "group_mid_10002"),
                    ("T1_10002_20", "group_mid_10002"),
                ]
            )

        # Because in orig workflow, pipetaskInit has edge to T2(10002, 10),
        # there will be an "extra" edge from pipetaskInit to group_mid_10002.
        edges.extend(
            [
                ("pipetaskInit", "group_mid_301"),
                ("pipetaskInit", "group_mid_10001"),
                ("pipetaskInit", "group_mid_10002"),
            ]
        )
    else:
        dim1s = [301, 10001, 10002]
        for dim1 in dim1s:
            T1_dim1: str | int = dim1
            if equiv_dims:
                T1_dim1 = DIM_MAPPING[dim1]

            edges.extend(
                [
                    ("pipetaskInit", f"T1_{T1_dim1}_10"),
                    ("pipetaskInit", f"T1_{T1_dim1}_11"),
                    ("pipetaskInit", f"T1_{T1_dim1}_20"),
                    (f"T1_{T1_dim1}_10", f"group_mid_{dim1}"),
                    (f"T1_{T1_dim1}_11", f"group_mid_{dim1}"),
                    (f"T1_{T1_dim1}_20", f"group_mid_{dim1}"),
                ]
            )
    gwf.add_edges_from(edges)

    if final:
        job = cast(GenericWorkflowJob, gwf_orig.get_final())
        gwf.add_final(job)

    return gwf


def compare_generic_workflows(gwf1: GenericWorkflow, gwf2: GenericWorkflow) -> bool:
    """Compare two workflows printing log messages where not equal.

    Parameters
    ----------
    gwf1 : `lsst.ctrl.bps.GenericWorkflow`
        First workflow to compare.
    gwf2 : `lsst.ctrl.bps.GenericWorkflow`
        Second workflow to compare.

    Returns
    -------
    equal : bool
        Whether the two workflows are the same.
    """
    equal = True

    # check edges
    edges1 = set(gwf1.edges)
    edges2 = set(gwf2.edges)
    only_in_first = edges1 - edges2
    only_in_second = edges2 - edges1
    if only_in_first:
        _LOG.debug("Edges only in %s, but not in %s: %s", gwf1.name, gwf2.name, only_in_first)
        equal = False
    if only_in_second:
        _LOG.debug("Edges only in %s, but not in %s: %s", gwf2.name, gwf1.name, only_in_second)
        equal = False

    # check nodes
    names1 = set(gwf1.nodes)
    names2 = set(gwf2.nodes)
    only_in_first = names1 - names2
    only_in_second = names2 - names1
    if only_in_first:
        _LOG.debug("Jobs only in %s, but not in %s: %s", gwf1.name, gwf2.name, only_in_first)
        equal = False
    if only_in_second:
        _LOG.debug("Jobs only in %s, but not in %s: %s", gwf2.name, gwf1.name, only_in_second)
        equal = False

    # check node values
    for name in names1 & names2:
        job1 = gwf1.get_job(name)
        job2 = gwf2.get_job(name)

        if job1.node_type != job2.node_type:
            _LOG.debug(
                "Group jobs` node_type not equal %s=%s, %s=%s",
                job1.name,
                job1.blocking,
                job2.name,
                job2.blocking,
            )
            equal = False
        elif job1.node_type == GenericWorkflowNodeType.GROUP:
            if job1.blocking != job2.blocking:
                _LOG.debug(
                    "Group jobs` blocking not equal %s=%s, %s=%s",
                    job1.name,
                    job1.blocking,
                    job2.name,
                    job2.blocking,
                )
                equal = False

            # compare workflows
            equal = equal or compare_generic_workflows(job1, job2)

    # check final
    final1 = gwf1.get_final()
    final2 = gwf2.get_final()
    if final1 != final2:
        _LOG.debug("Final jobs are not equal: %s vs %s", final1, final2)
        equal = False

    return equal
