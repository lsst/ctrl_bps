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
"""QuantumGraph-related utilities to support ctrl_bps testing.
"""

# Not actually running Quantum so do not need to override 'run' Method
# pylint: disable=abstract-method

# Many dummy classes for testing.
# pylint: disable=missing-class-docstring

import lsst.pipe.base.connectionTypes as cT
from lsst.daf.butler import Config, DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, Quantum
from lsst.pex.config import Field
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections, QuantumGraph, TaskDef
from lsst.utils.introspection import get_full_type_name

METADATA = {"D1": [1, 2, 3]}


# For each dummy task, create a Connections, Config, and PipelineTask


class Dummy1Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    initOutput = cT.InitOutput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Input", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy1Config(PipelineTaskConfig, pipelineConnections=Dummy1Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy1PipelineTask(PipelineTask):
    ConfigClass = Dummy1Config


class Dummy2Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    initInput = cT.InitInput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy2Config(PipelineTaskConfig, pipelineConnections=Dummy2Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy2PipelineTask(PipelineTask):
    ConfigClass = Dummy2Config


class Dummy3Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    initInput = cT.InitInput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy3InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy3Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy3Config(PipelineTaskConfig, pipelineConnections=Dummy3Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy3PipelineTask(PipelineTask):
    ConfigClass = Dummy3Config


# Test if a Task that does not interact with the other Tasks works fine in
# the graph.
class Dummy4Connections(PipelineTaskConnections, dimensions=("D1", "D2")):
    input = cT.Input(name="Dummy4Input", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))
    output = cT.Output(name="Dummy4Output", storageClass="ExposureF", doc="n/a", dimensions=("D1", "D2"))


class Dummy4Config(PipelineTaskConfig, pipelineConnections=Dummy4Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy4PipelineTask(PipelineTask):
    ConfigClass = Dummy4Config


def make_test_quantum_graph():
    """Create a QuantumGraph for unit tests.

    Returns
    -------
    qgraph : `lsst.pipe.base.QuantumGraph`
        A test QuantumGraph looking like the following:
        (DummyTask4 is completely independent.)

        Numbers in parens are the values for the two dimensions (D1, D2).

        T1(1,2)   T1(3,4)  T4(1,2)  T4(3,4)
           |         |
        T2(1,2)   T2(3,4)
           |         |
        T3(1,2)   T3(3,4)
    """
    config = Config(
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

    universe = DimensionUniverse(config=config)
    # need to make a mapping of TaskDef to set of quantum
    quantum_map = {}
    tasks = []
    for task, label in (
        (Dummy1PipelineTask, "T1"),
        (Dummy2PipelineTask, "T2"),
        (Dummy3PipelineTask, "T3"),
        (Dummy4PipelineTask, "T4"),
    ):
        task_def = TaskDef(get_full_type_name(task), task.ConfigClass(), task, label)
        tasks.append(task_def)
        quantum_set = set()
        for dim1, dim2 in ((1, 2), (3, 4)):
            if task_def.connections.initInputs:
                init_init_ds_type = DatasetType(
                    task_def.connections.initInput.name,
                    tuple(),
                    storageClass=task_def.connections.initInput.storageClass,
                    universe=universe,
                )
                init_refs = [DatasetRef(init_init_ds_type, DataCoordinate.makeEmpty(universe))]
            else:
                init_refs = None
            input_ds_type = DatasetType(
                task_def.connections.input.name,
                task_def.connections.input.dimensions,
                storageClass=task_def.connections.input.storageClass,
                universe=universe,
            )
            input_refs = [
                DatasetRef(
                    input_ds_type, DataCoordinate.standardize({"D1": dim1, "D2": dim2}, universe=universe)
                )
            ]
            output_ds_type = DatasetType(
                task_def.connections.output.name,
                task_def.connections.output.dimensions,
                storageClass=task_def.connections.output.storageClass,
                universe=universe,
            )
            output_refs = [
                DatasetRef(
                    output_ds_type, DataCoordinate.standardize({"D1": dim1, "D2": dim2}, universe=universe)
                )
            ]
            quantum_set.add(
                Quantum(
                    taskName=task.__qualname__,
                    dataId=DataCoordinate.standardize({"D1": dim1, "D2": dim2}, universe=universe),
                    taskClass=task,
                    initInputs=init_refs,
                    inputs={input_ds_type: input_refs},
                    outputs={output_ds_type: output_refs},
                )
            )
        quantum_map[task_def] = quantum_set
    qgraph = QuantumGraph(quantum_map, metadata=METADATA)

    return qgraph
