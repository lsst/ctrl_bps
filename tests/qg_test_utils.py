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
"""QuantumGraph-related utilities to support ctrl_bps testing."""

# Not actually running Quantum so do not need to override 'run' Method
# pylint: disable=abstract-method

# Many dummy classes for testing.
# pylint: disable=missing-class-docstring

import lsst.pipe.base.connectionTypes as cT
from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionConfig, Quantum
from lsst.pex.config import Field
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo

METADATA = {"D1": [1, 2, 3]}


# For each dummy task, create a Connections, Config, and PipelineTask


class Dummy1Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """Connections class used for tests."""

    initOutput = cT.InitOutput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Input", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy1Config(PipelineTaskConfig, pipelineConnections=Dummy1Connections):
    """Config class used for testing."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy1PipelineTask(PipelineTask):
    """PipelineTask used for testing."""

    ConfigClass = Dummy1Config


class Dummy2Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """Second connections class used for testing."""

    initInput = cT.InitInput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy2Config(PipelineTaskConfig, pipelineConnections=Dummy2Connections):
    """Config class used for second pipeline task."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy2PipelineTask(PipelineTask):
    """Second test PipelineTask."""

    ConfigClass = Dummy2Config


class Dummy2bConnections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """A connections class used for testing mid-pipeline leaf node."""

    initInput = cT.InitInput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy2bInitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy2bOutput", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy2bConfig(PipelineTaskConfig, pipelineConnections=Dummy2bConnections):
    """Config used for testing dummy2b."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy2bPipelineTask(PipelineTask):
    """PipelineTask for dummy2b."""

    ConfigClass = Dummy2bConfig


class Dummy3Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """Third connections class used for testing."""

    initInput = cT.InitInput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy3InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy3Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy3Config(PipelineTaskConfig, pipelineConnections=Dummy3Connections):
    """Third config used for testing."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy3PipelineTask(PipelineTask):
    """Third test PipelineTask."""

    ConfigClass = Dummy3Config


class Dummy4Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """Fourth connections class used for testing."""

    initInput = cT.InitInput(name="Dummy3InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy4InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy3Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy4Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy4Config(PipelineTaskConfig, pipelineConnections=Dummy4Connections):
    """Fourth config used for testing."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy4PipelineTask(PipelineTask):
    """Fourth test PipelineTask."""

    ConfigClass = Dummy4Config


# Test if a Task that does not interact with the other Tasks works fine in
# the graph.
class Dummy5Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    """Fifth connections class used for testing."""

    input = cT.Input(name="Dummy5Input", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy5Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy5Config(PipelineTaskConfig, pipelineConnections=Dummy5Connections):
    """Fifth config used for testing."""

    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy5PipelineTask(PipelineTask):
    """Fifth test PipelineTask."""

    ConfigClass = Dummy5Config


def _make_quantum(run, universe, task, task_def, dim1, dim2, intermediate_refs):
    if task_def.connections.initInputs:
        init_init_ds_type = DatasetType(
            task_def.connections.initInput.name,
            (),
            storageClass=task_def.connections.initInput.storageClass,
            universe=universe,
        )
        init_refs = [DatasetRef(init_init_ds_type, DataCoordinate.make_empty(universe), run=run)]
    else:
        init_refs = None
    input_ds_type = DatasetType(
        task_def.connections.input.name,
        task_def.connections.input.dimensions,
        storageClass=task_def.connections.input.storageClass,
        universe=universe,
    )
    data_id = DataCoordinate.standardize({"D1": dim1, "D2": dim2}, universe=universe)
    if ref := intermediate_refs.get((input_ds_type, data_id)):
        input_refs = [ref]
    else:
        input_refs = [DatasetRef(input_ds_type, data_id, run=run)]
    output_ds_type = DatasetType(
        task_def.connections.output.name,
        task_def.connections.output.dimensions,
        storageClass=task_def.connections.output.storageClass,
        universe=universe,
    )
    ref = DatasetRef(output_ds_type, data_id, run=run)
    intermediate_refs[(output_ds_type, data_id)] = ref
    output_refs = [ref]
    quantum = Quantum(
        taskName=task.__qualname__,
        dataId=data_id,
        taskClass=task,
        initInputs=init_refs,
        inputs={input_ds_type: input_refs},
        outputs={output_ds_type: output_refs},
    )
    return quantum


def make_test_helper() -> InMemoryRepo:
    """Make a test helper that can produce a quantum graph useful for
    clustering tests.

    See `make_quantum_graph` for a more complete description of this graph.
    """
    dimension_config = DimensionConfig(
        {
            "version": 1,
            "skypix": {
                "common": "htm7",
                "htm": {
                    "class": "lsst.sphgeom.HtmPixelization",
                    "max_level": 24,
                },
            },
            "elements": {
                "D1": {
                    "keys": [
                        {
                            "name": "id",
                            "type": "int",
                        }
                    ],
                    "storage": {
                        "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                    },
                },
                "D2": {
                    "keys": [
                        {
                            "name": "id",
                            "type": "int",
                        }
                    ],
                    "storage": {
                        "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                    },
                },
            },
            "packers": {},
        }
    )
    helper = InMemoryRepo(dimension_config=dimension_config)
    helper.butler.registry.insertDimensionData("D1", *[{"id": n} for n in (1, 3)])
    helper.butler.registry.insertDimensionData("D2", *[{"id": n} for n in (2, 4)])
    # Note that automatic inputs and outputs work for most of the tasks below
    # to create a chain from each task to the next.
    helper.add_task(
        "T1",
        dimensions=("D1", "D2"),
        init_outputs={"initOutput": DynamicConnectionConfig(dataset_type_name="Dummy1InitOutput")},
    )
    helper.add_task(
        "T2",
        dimensions=("D1", "D2"),
        init_inputs={"initInput": DynamicConnectionConfig(dataset_type_name="Dummy1InitOutput")},
        init_outputs={"initOutput": DynamicConnectionConfig(dataset_type_name="Dummy2InitOutput")},
    )
    helper.add_task(
        "T3",
        dimensions=("D1", "D2"),
        init_inputs={"initInput": DynamicConnectionConfig(dataset_type_name="Dummy2InitOutput")},
        init_outputs={"initOutput": DynamicConnectionConfig(dataset_type_name="Dummy3InitOutput")},
    )
    helper.add_task(
        "T4",
        dimensions=("D1", "D2"),
        init_inputs={"initInput": DynamicConnectionConfig(dataset_type_name="Dummy3InitOutput")},
        init_outputs={"initOutput": DynamicConnectionConfig(dataset_type_name="Dummy4InitOutput")},
    )
    helper.add_task(
        "T5",
        dimensions=("D1", "D2"),
        inputs={
            "initInput": DynamicConnectionConfig(dataset_type_name="Dummy5Input", dimensions=("D1", "D2"))
        },
        outputs={
            "initOutput": DynamicConnectionConfig(dataset_type_name="Dummy5Output", dimensions=("D1", "D2"))
        },
    )
    helper.add_task(
        "T2b",
        dimensions=("D1", "D2"),
        init_inputs={"initInput": DynamicConnectionConfig(dataset_type_name="Dummy2InitOutput")},
        init_outputs={"initOutput": DynamicConnectionConfig(dataset_type_name="Dummy2bInitOutput")},
        inputs={"input": DynamicConnectionConfig(dataset_type_name="dataset_auto2", dimensions=("D1", "D2"))},
    )
    helper.insert_datasets("dataset_auto0", where="D1 < D2")
    helper.insert_datasets("Dummy5Input", where="D1 < D2")
    return helper


def make_test_quantum_graph(run: str = "run", uneven=False):
    """Create a quantum graph for unit tests.

    Parameters
    ----------
    run : `str`, optional
        Name of the RUN collection for output datasets.
    uneven : `bool`, optional
        Whether some of the quanta for initial tasks are
        not included as if finished in previous run.

    Returns
    -------
    qgraph : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
        A test QuantumGraph looking like the following:
        (Task T5 is completely independent).

        Numbers in parens are the values for the two dimensions (D1, D2).

        .. code-block::
           T1(1,2)    T1(1,4)     T1(3,4)  T5(1,2)  T5(1,4)  T5(3,4)
            |          |           |
           T2(1,2)    T2(1,4)     T2(3,4)
            |   |      |   |       |   |
            | T2b(1,2) | T2b(1,4)  | T2b(3,4)
            |          |           |
           T3(1,2)    T3(1,4)     T3(3,4)
            |          |           |
           T4(1,2)    T4(1,4)     T4(3,4)
    """
    with make_test_helper() as helper:
        qgc = helper.make_quantum_graph_builder(output_run=run).finish(attach_datastore_records=False)
        if uneven:
            keys_to_drop = {("T1", 1, 2), ("T1", 1, 4), ("T2", 1, 2)}
            qgc.quantum_datasets = {
                qd.quantum_id: qd
                for qd in qgc.quantum_datasets.values()
                if (qd.task_label, *qd.data_coordinate) not in keys_to_drop
            }
            qgc.set_thin_graph()
            qgc.set_header_counts()
        return qgc.assemble()
