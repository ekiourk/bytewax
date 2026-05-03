# ADR 0001 — Distribute wheels using the CPython Stable ABI (`abi3-py310`)

- **Status:** Accepted
- **Date:** 2026-05-03
- **Deciders:** @ekiourk (fork maintainer)

## Context

bytewax is a Python streaming library implemented in Rust via PyO3. Until
the Python 3.13 modernization PR (#1), wheels were built per
(Python version × OS × architecture):

- 4 supported Python versions (3.10, 3.11, 3.12, 3.13)
- 3 OSes (Linux, macOS, Windows)
- Up to 3 architectures per OS (Linux: x86_64, aarch64, armv7;
  macOS: x86_64, aarch64; Windows: x86_64)

= **24 wheels per release.**

This created friction:

- Long CI build matrix (24 cells) for every push to `main` and every PR.
- 24 wheels to upload, store, and publish per release; the CD pipeline
  hard-coded a wheel-count check (`WHEELS=24`) that broke whenever the
  matrix changed.
- Adding a new Python version required updating the matrix in two
  workflows, the `requirements/build-pyX.Y.txt` files, the docs, and
  the CD wheel-count check.
- The release workflow involved staging wheels in S3 between CI and
  the publish step (see PR #3 for the cleanup).

CPython's [Stable ABI / Limited API](https://peps.python.org/pep-0384/)
lets a single compiled extension target Python 3.X+: the resulting
wheel works on all current and future minor Python releases (with
caveats — see *Consequences*). PyO3 supports this via the
`abi3-pyXY` Cargo feature.

The cost: certain Python C-API operations that are inlined macros in
the full API (e.g. `Py_INCREF`/`Py_DECREF`,
`PyList_GET_ITEM`) become real function calls under `abi3`. For a
high-frequency Rust ↔ Python boundary like a streaming dataflow
operator, this could plausibly add measurable overhead.

## Decision

**Use `abi3-py310` to build a single wheel per (OS, architecture)
targeting Python 3.10+.** Wheel matrix shrinks from 24 to 6:
3 Linux + 2 macOS + 1 Windows.

## Methodology

The decision was validated empirically with two CodSpeed experiments
(instrumentation-mode, instruction-count-based measurement):

### Experiment 1 — abi3 perf comparison

Branch: `experiment/disable-abi3-for-perf-comparison` (PR #4, closed
without merging).

Removed `"abi3-py310"` from the `pyo3` features list in `Cargo.toml`,
leaving everything else identical. Built a non-abi3 wheel
(`cp310-cp310-...whl`) and ran the CodSpeed bench suite against `main`
(which had `abi3-py310` enabled).

**Result:** *no measurable change.* All 6 benchmarks reported as
"untouched" — within CodSpeed's threshold of ~1%.

### Experiment 2 — calibration

Branch: `experiment/codspeed-calibration` (PR #5, closed without
merging).

To rule out a false negative on Experiment 1, introduced a deterministic
slowdown in exactly one benchmark: a `_busy_identity` `op.map` that
runs ~100 Python loop iterations per item, returning the value
unchanged. With 100,000 items per benchmark run that's roughly
~5×10^8 extra instructions.

**Result:** CodSpeed correctly flagged a 78.6% regression on
`test_branch_benchmark[run_main]` (464.2 ms → 2,172.5 ms) and a 78.1%
regression on `test_branch_benchmark[cluster_main-1thread]` (487.1 ms
→ 2,221.3 ms). The other 4 benchmarks (`test_flat_map_batch_benchmark`,
`test_fold_window_benchmark`, both parametrizations) reported as
untouched, confirming per-benchmark isolation.

### Combined interpretation

CodSpeed reliably catches regressions of order ~5×10^8 instructions
per run and isolates them per-benchmark. Therefore Experiment 1's "no
change" result means abi3's overhead is below CodSpeed's threshold —
fewer than ~5×10^6 extra instructions across 100,000 items, i.e.
**under ~50 instructions per item flowing through the dataflow**. This
is consistent with abi3 adding a handful of function-call entries
(`Py_IncRef` / `Py_DecRef` instead of inlined macros) per item.

## Consequences

**Positive:**

- 24 → 6 wheels per release; CI build time and CD pipeline both
  significantly simpler.
- New Python releases (3.14, 3.15, …) work with existing wheels until
  we choose to bump the abi3 floor — no rebuild required.
- One wheel name pattern (`cp310-abi3-*`) instead of per-version
  variants; downstream tooling that pattern-matches on wheel names is
  simpler.
- The `test-abi3` smoke-test job in CI is enough cross-version
  coverage; we don't need a 4× build matrix to validate compatibility.

**Negative:**

- Slightly slower per-Python-call overhead. Below CodSpeed's
  measurement threshold for our representative dataflow benchmarks,
  but plausibly real and detectable on a synthetic
  pure-boundary-crossing microbenchmark (which we don't currently
  have).
- Limited to the abi3-stable subset of the Python C-API. Some PyO3
  features and optimizations (e.g. vectorcall fast paths, direct list
  /tuple access) aren't available under abi3.
- Build process must use Python 3.10 (the abi3 floor) to avoid
  PyO3 0.22.x cfg leaks. The PyO3 0.22.6 implementation gates some
  symbols (notably `PyType_GetModuleName`) on `#[cfg(Py_3_13)]`
  rather than on `#[cfg(not(Py_LIMITED_API))]`. Building with a newer
  Python in the abi3 build chain leaks post-3.10 symbols into the
  binary; building with Python 3.10 sidesteps this. PyO3 0.23+
  reportedly fixes the gating — when we bump, this constraint can be
  relaxed.

## When to revisit

This ADR should be re-evaluated if any of the following occurs:

- We add microbenchmarks of pure Rust ↔ Python boundary crossing
  (which would be more sensitive to abi3 cost than current dataflow
  benchmarks).
- A user reports measurable production slowdown traced to abi3.
- We want to use a Python C-API feature only available outside the
  limited API (e.g. vectorcall fast paths in a hot loop).
- We bump PyO3 to a version that materially changes abi3 ergonomics
  or cost characteristics.
- A future Python release adds a feature important to bytewax that
  requires the full API.

## References

- [PEP 384 — Defining a Stable ABI](https://peps.python.org/pep-0384/)
- [CPython Stable ABI documentation](https://docs.python.org/3/c-api/stable.html)
- [PyO3 user guide — Building and distribution](https://pyo3.rs/v0.22.6/building-and-distribution.html)
- PR #1 — Python 3.13 modernization (initial introduction of `abi3-py310`)
- PR #4 — Experiment: disable abi3 to measure perf overhead (closed
  without merging; provides Experiment 1 evidence)
- PR #5 — Experiment: calibrate CodSpeed by introducing a known
  regression (closed without merging; provides Experiment 2 evidence)
