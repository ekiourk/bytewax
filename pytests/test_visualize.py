import json
import textwrap
from unittest.mock import patch

import bytewax.operators as op
import pytest
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource
from bytewax.visualize import (
    _parse_args,
    _visualize_main,
    to_json,
    to_mermaid,
    to_plantuml,
)


def test_to_json_linear():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "inp",
                "step_id": "test_df.inp",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.inp.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_one",
                "step_id": "test_df.add_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_one.up",
                        "from_port_ids": ["test_df.inp.down"],
                        "from_stream_ids": ["test_df.inp.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_one.down",
                        "from_port_ids": ["test_df.add_one.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_one.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.inp.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out",
                "step_id": "test_df.out",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out.up",
                        "from_port_ids": ["test_df.add_one.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_json_nonlinear():
    flow = Dataflow("test_df")
    nums = op.input("nums", flow, TestingSource([1, 2, 3]))
    ones = op.map("add_one", nums, lambda x: x + 1)
    twos = op.map("add_two", nums, lambda x: x + 2)
    op.output("out_one", ones, TestingSink([]))
    op.output("out_two", twos, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "nums",
                "step_id": "test_df.nums",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.nums.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_one",
                "step_id": "test_df.add_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_one.up",
                        "from_port_ids": ["test_df.nums.down"],
                        "from_stream_ids": ["test_df.nums.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_one.down",
                        "from_port_ids": ["test_df.add_one.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_one.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_two",
                "step_id": "test_df.add_two",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_two.up",
                        "from_port_ids": ["test_df.nums.down"],
                        "from_stream_ids": ["test_df.nums.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_two.down",
                        "from_port_ids": ["test_df.add_two.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_two.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_two.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_two.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_two.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_two.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out_one",
                "step_id": "test_df.out_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out_one.up",
                        "from_port_ids": ["test_df.add_one.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out_two",
                "step_id": "test_df.out_two",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out_two.up",
                        "from_port_ids": ["test_df.add_two.down"],
                        "from_stream_ids": ["test_df.add_two.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_json_multistream_inp():
    flow = Dataflow("test_df")
    ones = op.input("ones", flow, TestingSource([2, 3, 4]))
    twos = op.input("twos", flow, TestingSource([3, 4, 5]))
    s = op.merge("merge", ones, twos)
    op.output("out", s, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "ones",
                "step_id": "test_df.ones",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.ones.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "twos",
                "step_id": "test_df.twos",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.twos.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "merge",
                "step_name": "merge",
                "step_id": "test_df.merge",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "ups",
                        "port_id": "test_df.merge.ups",
                        "from_port_ids": ["test_df.ones.down", "test_df.twos.down"],
                        "from_stream_ids": ["test_df.ones.down", "test_df.twos.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.merge.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out",
                "step_id": "test_df.out",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out.up",
                        "from_port_ids": ["test_df.merge.down"],
                        "from_stream_ids": ["test_df.merge.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_mermaid_linear():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.inp["inp (input)"]
        test_df.add_one["add_one (map)"]
        test_df.inp -- "down → up" --> test_df.add_one
        test_df.out["out (output)"]
        test_df.add_one -- "down → up" --> test_df.out
        end"""
    )


def test_to_mermaid_nonlinear():
    flow = Dataflow("test_df")
    nums = op.input("nums", flow, TestingSource([1, 2, 3]))
    ones = op.map("add_one", nums, lambda x: x + 1)
    twos = op.map("add_two", nums, lambda x: x + 2)
    op.output("out_one", ones, TestingSink([]))
    op.output("out_two", twos, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.nums["nums (input)"]
        test_df.add_one["add_one (map)"]
        test_df.nums -- "down → up" --> test_df.add_one
        test_df.add_two["add_two (map)"]
        test_df.nums -- "down → up" --> test_df.add_two
        test_df.out_one["out_one (output)"]
        test_df.add_one -- "down → up" --> test_df.out_one
        test_df.out_two["out_two (output)"]
        test_df.add_two -- "down → up" --> test_df.out_two
        end"""
    )


def test_to_mermaid_multistream_inp():
    flow = Dataflow("test_df")
    ones = op.input("ones", flow, TestingSource([2, 3, 4]))
    twos = op.input("twos", flow, TestingSource([3, 4, 5]))
    s = op.merge("merge", ones, twos)
    op.output("out", s, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.ones["ones (input)"]
        test_df.twos["twos (input)"]
        test_df.merge["merge (merge)"]
        test_df.ones -- "down → ups" --> test_df.merge
        test_df.twos -- "down → ups" --> test_df.merge
        test_df.out["out (output)"]
        test_df.merge -- "down → up" --> test_df.out
        end"""
    )


def test_to_plantuml_linear():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    assert to_plantuml(flow) == textwrap.dedent(
        """\
        @startuml
        component test_df.inp [
            test_df.inp (input)
        ]
        component test_df.inp {
            portout test_df.inp.down
        }
        component test_df.add_one [
            test_df.add_one (map)
        ]
        component test_df.add_one {
            portin test_df.add_one.up
            portout test_df.add_one.down
            test_df.inp.down --> test_df.add_one.up : test_df.inp.down
        }
        component test_df.out [
            test_df.out (output)
        ]
        component test_df.out {
            portin test_df.out.up
            test_df.add_one.down --> test_df.out.up : test_df.add_one.flat_map_batch.down
        }
        @enduml"""
    )


def test_to_plantuml_recursive_shows_substeps():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    out = to_plantuml(flow, recursive=True)
    # The non-recursive variant doesn't include flat_map_batch substeps.
    assert "test_df.add_one.flat_map_batch (flat_map_batch)" in out
    # The closing connection inside the wrapping component should fire
    # (this is the recursive-only branch in `_to_plantuml_step`).
    assert "test_df.add_one.flat_map_batch.down --> test_df.add_one.down" in out


def test_to_plantuml_nonlinear():
    """A branching dataflow exercises multi-output ports."""
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    b = op.branch("br", s, lambda x: x % 2 == 0)
    op.output("out_evens", b.trues, TestingSink([]))
    op.output("out_odds", b.falses, TestingSink([]))

    out = to_plantuml(flow)
    # Both branches should appear as separate output ports of the
    # branch step, and connect to the respective sink.
    assert "portout test_df.br.trues" in out
    assert "portout test_df.br.falses" in out
    assert "test_df.br.trues --> test_df.out_evens.up" in out
    assert "test_df.br.falses --> test_df.out_odds.up" in out


@pytest.fixture
def _tiny_flow():
    flow = Dataflow("cli_test")
    s = op.input("inp", flow, TestingSource([1]))
    op.output("out", s, TestingSink([]))
    return flow


@pytest.mark.parametrize("output_format", ["json", "mermaid", "plantuml"])
def test_visualize_main_dispatches_to_each_format(_tiny_flow, output_format, capsys):
    """`_visualize_main` should call the right renderer for each format
    and print its output to stdout."""
    with patch("bytewax.visualize._locate_dataflow", return_value=_tiny_flow):
        _visualize_main("module:flow", output_format, recursive=False)

    stdout = capsys.readouterr().out

    # Each format has a distinct, unmistakable preamble we can match
    # without snapshotting the whole output (which is already covered
    # by the per-format tests above).
    if output_format == "json":
        assert '"typ": "RenderedDataflow"' in stdout
    elif output_format == "mermaid":
        assert "flowchart TD" in stdout
    elif output_format == "plantuml":
        assert "@startuml" in stdout


def test_visualize_main_unknown_format_raises(_tiny_flow):
    with patch("bytewax.visualize._locate_dataflow", return_value=_tiny_flow):
        with pytest.raises(ValueError, match="unknown visualization type"):
            _visualize_main("module:flow", "graphviz", recursive=False)  # type: ignore[arg-type]


def test_parse_args_defaults():
    with patch("sys.argv", ["bytewax-visualize", "myflow:flow"]):
        ns = _parse_args()
    assert ns.import_str == "myflow:flow"
    assert ns.output_format == "mermaid"
    assert ns.recursive is False


@pytest.mark.parametrize("fmt", ["json", "mermaid", "plantuml"])
def test_parse_args_format_selection(fmt):
    with patch("sys.argv", ["bytewax-visualize", "myflow:flow", "-o", fmt]):
        ns = _parse_args()
    assert ns.output_format == fmt


def test_parse_args_recursive_flag():
    with patch("sys.argv", ["bytewax-visualize", "myflow:flow", "--recursive"]):
        ns = _parse_args()
    assert ns.recursive is True
