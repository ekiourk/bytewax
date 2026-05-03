"""Tests for `bytewax.run` — particularly `_locate_dataflow`.

The CLI-arg-parsing side of `run.py` is covered in `test_parse.py`. This
module focuses on dynamically locating a `Dataflow` given a module name
and an attribute or factory expression — the heart of
`python -m bytewax.run <module:flow>`.
"""

import sys
from types import ModuleType
from typing import Iterator
from unittest.mock import patch

import pytest
from bytewax.dataflow import Dataflow
from bytewax.run import _locate_dataflow, _parse_args


@pytest.fixture
def stub_module() -> Iterator[str]:
    """Register a synthetic module in `sys.modules` for the duration of
    a test, exposing a variety of attributes that exercise every branch
    in `_locate_dataflow`.
    """
    name = "_bytewax_test_locate_module"
    mod = ModuleType(name)

    # Variable case: a real Dataflow at module scope.
    mod.flow = Dataflow("variable_case")  # type: ignore[attr-defined]

    # Factory cases: functions that return a Dataflow.
    def make_flow():
        return Dataflow("no_args_factory")

    def make_flow_with_args(name, value=42):
        return Dataflow(f"{name}_{value}")

    def make_flow_returns_int():
        return 7  # not a Dataflow → triggers the RuntimeError branch

    def make_flow_raises_typeerror():
        # A genuine TypeError raised *inside* the factory body. Should
        # propagate, not be swallowed by `_called_with_wrong_args`.
        msg = "genuine error from inside factory"
        raise TypeError(msg)

    mod.make_flow = make_flow  # type: ignore[attr-defined]
    mod.make_flow_with_args = make_flow_with_args  # type: ignore[attr-defined]
    mod.make_flow_returns_int = make_flow_returns_int  # type: ignore[attr-defined]
    mod.make_flow_raises_typeerror = make_flow_raises_typeerror  # type: ignore[attr-defined]

    # Non-Dataflow attribute for the "wrong type" branch.
    mod.not_a_flow = "this is a string"  # type: ignore[attr-defined]

    sys.modules[name] = mod
    try:
        yield name
    finally:
        del sys.modules[name]


# -----
# `_locate_dataflow`: variable form (`module:flow_var`)
# -----


def test_locate_dataflow_returns_module_attribute(stub_module: str) -> None:
    flow = _locate_dataflow(stub_module, "flow")
    assert isinstance(flow, Dataflow)
    assert flow.flow_id == "variable_case"


def test_locate_dataflow_attribute_not_dataflow_raises(stub_module: str) -> None:
    with pytest.raises(RuntimeError, match="valid Bytewax dataflow"):
        _locate_dataflow(stub_module, "not_a_flow")


def test_locate_dataflow_missing_attribute_raises(stub_module: str) -> None:
    with pytest.raises(AttributeError, match="does_not_exist"):
        _locate_dataflow(stub_module, "does_not_exist")


# -----
# `_locate_dataflow`: factory form (`module:make_flow(args)`)
# -----


def test_locate_dataflow_calls_factory_no_args(stub_module: str) -> None:
    flow = _locate_dataflow(stub_module, "make_flow()")
    assert isinstance(flow, Dataflow)
    assert flow.flow_id == "no_args_factory"


def test_locate_dataflow_calls_factory_with_positional_args(stub_module: str) -> None:
    flow = _locate_dataflow(stub_module, "make_flow_with_args('hello')")
    assert isinstance(flow, Dataflow)
    assert flow.flow_id == "hello_42"


def test_locate_dataflow_calls_factory_with_kwargs(stub_module: str) -> None:
    flow = _locate_dataflow(stub_module, "make_flow_with_args('hi', value=99)")
    assert flow.flow_id == "hi_99"


def test_locate_dataflow_factory_returns_non_dataflow_raises(
    stub_module: str,
) -> None:
    with pytest.raises(RuntimeError, match="valid Bytewax dataflow"):
        _locate_dataflow(stub_module, "make_flow_returns_int()")


def test_locate_dataflow_factory_called_with_wrong_args_raises(
    stub_module: str,
) -> None:
    # `make_flow` takes no args; calling with one is wrong. The error
    # surface should explain this came from arg-mismatch, not propagate
    # a confusing internal TypeError.
    with pytest.raises(TypeError, match="could not be called"):
        _locate_dataflow(stub_module, "make_flow('unexpected')")


def test_locate_dataflow_factory_internal_typeerror_propagates(
    stub_module: str,
) -> None:
    # Distinct from "wrong args": the factory itself raises TypeError
    # in its body. We must NOT mask this as an arg-mismatch error.
    with pytest.raises(TypeError, match="genuine error from inside factory"):
        _locate_dataflow(stub_module, "make_flow_raises_typeerror()")


# -----
# `_locate_dataflow`: malformed dataflow_name expressions
# -----


def test_locate_dataflow_syntax_error_in_expr(stub_module: str) -> None:
    with pytest.raises(SyntaxError, match="attribute name or function call"):
        _locate_dataflow(stub_module, "this is not valid python")


def test_locate_dataflow_complex_callable_rejected(stub_module: str) -> None:
    # `module:obj.method()` — the function ref is an Attribute, not a
    # simple Name. Reject explicitly rather than try to resolve it.
    with pytest.raises(TypeError, match="must be a simple name"):
        _locate_dataflow(stub_module, "make_flow.weird()")


def test_locate_dataflow_non_literal_args_rejected(stub_module: str) -> None:
    # Args must be literal; references aren't allowed.
    with pytest.raises(ValueError, match="literal values"):
        _locate_dataflow(stub_module, "make_flow_with_args(some_variable)")


def test_locate_dataflow_unsupported_expression_rejected(stub_module: str) -> None:
    # Neither a Name nor a Call — this is an arithmetic expression.
    with pytest.raises(ValueError, match="attribute name or function call"):
        _locate_dataflow(stub_module, "1 + 1")


# -----
# `_locate_dataflow`: import-side errors
# -----


def test_locate_dataflow_module_does_not_exist_raises() -> None:
    # The original ImportError propagates (its `.__traceback__` is
    # non-None, which is the conditional re-raise in `_locate_dataflow`).
    # The "Could not import" friendly-wrap branch is currently
    # unreachable; treat as a separate cleanup task.
    with pytest.raises(ImportError, match="definitely_not_a_real_module_xyz"):
        _locate_dataflow("definitely_not_a_real_module_xyz", "flow")


# -----
# `_parse_args`: error paths not covered by `test_parse.py`
# -----


def test_parse_args_process_id_without_addresses_errors(monkeypatch) -> None:
    """Passing -i without -a (and no BYTEWAX_HOSTFILE_PATH env var) is
    a usage error: argparse should exit with code 2."""
    # Ensure the helm-chart env vars don't accidentally satisfy the
    # check.
    for key in (
        "BYTEWAX_HOSTFILE_PATH",
        "BYTEWAX_POD_NAME",
        "BYTEWAX_STATEFULSET_NAME",
        "BYTEWAX_PROCESS_ID",
        "BYTEWAX_ADDRESSES",
    ):
        monkeypatch.delenv(key, raising=False)

    with (
        patch.object(sys, "argv", ["bytewax", "examples.basic:flow", "-i", "0"]),
        pytest.raises(SystemExit) as exc_info,
    ):
        _parse_args()
    # argparse `parser.error()` exits with code 2.
    assert exc_info.value.code == 2


def test_parse_args_recovery_dir_without_intervals_errors(monkeypatch) -> None:
    """Setting --recovery-directory without --snapshot-interval and
    --backup-interval should be flagged as a misconfig."""
    for key in (
        "BYTEWAX_RECOVERY_DIRECTORY",
        "BYTEWAX_SNAPSHOT_INTERVAL",
        "BYTEWAX_RECOVERY_BACKUP_INTERVAL",
    ):
        monkeypatch.delenv(key, raising=False)

    with (
        patch.object(
            sys,
            "argv",
            ["bytewax", "examples.basic:flow", "--recovery-directory", "/tmp/r"],
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        _parse_args()
    assert exc_info.value.code == 2
