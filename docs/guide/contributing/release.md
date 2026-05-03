# Library Release Checklist

Release process for this fork. The fork is **not currently published
to PyPI**; releases produce GitHub Releases with pre-built abi3 wheels
attached as downloadable assets. PyPI publishing is on the roadmap (see
`docs/decisions/0001-abi3-wheels.md` for the wheel-format ADR; PyPI
Trusted Publishing will be added as its own focused workflow when we're
ready).

## 1. One Final PR

Make a PR which commits the following:

1. Bumps version number in `Cargo.toml`

   ```{code-block} diff
   :caption: Cargo.toml
    [package]
   -version = "0.1.0"
   +version = "1.2.3"
   ```

2. Commits updated stubs

   ```console
   (dev) $ just stubgen
   ```

   If there was no change to any of our PyO3 Rust API, there might be
   no changes to commit here. People also should be committing these
   changes in the actual feature PRs.

3. Labels the latest changelog entries with the version number

   Look in `CHANGELOG.md` for the latest batch of hand-written
   changelog notes and add a new heading with the version number.

   ```{code-block} diff
   :caption: CHANGELOG.md
    ## Latest

    __Add any extra change notes here and we'll put them in the release
    notes on GitHub when we make a new release.__

   +## 1.2.3
   +
    * Example note here. Describe any super important changes that you
      wouldn't glean from PR names which will be added by GitHub
      automatically.
   ```

4. (Optional) Write migration guide entry

   For breaking changes only. Add a section to
   `docs/guide/reference/migration.md` with before/after example code.
   See existing entries for the format.

Then check before merging:

1. Confirm CI tests pass.
2. Confirm `repo-checks` (the lint/doctest job) passed.

Approve and merge the PR.

## 2. Push the release tag

The CI workflow watches for tag pushes. When a tag is pushed, the
`publish-release` job (in `.github/workflows/CI.yml`) downloads the
built wheels and attaches them to a new GitHub Release.

**Tag conventions:**

- `vX.Y.Z` (e.g. `v1.2.3`) → published as a **full release**
- Anything else (e.g. `dev-2026-05-03`, `rc-1`, `alpha-2`) → published
  as a **prerelease**

Push the tag from your local clone after the release PR has merged:

```console
$ git checkout main
$ git pull origin main
$ git tag v1.2.3
$ git push origin v1.2.3
```

CI will then:

1. Build all wheels (one abi3 wheel per platform/arch — 6 total: 3
   Linux + 2 macOS + 1 Windows).
2. Run the test suite on each.
3. Run the abi3 cross-version smoke tests.
4. Create a GitHub Release named after the tag.
5. Attach all 6 wheels to the release as downloadable assets.

Total runtime is ~15 minutes for the full matrix.

## 3. Verify the release

After CI completes:

1. Visit <https://github.com/ekiourk/bytewax/releases>.
2. Confirm the new release exists with all 6 wheels attached.
3. GitHub will have auto-generated release notes from the merged PRs.
   Edit the release description and prepend the hand-written
   `CHANGELOG.md` notes for this version.

   ```{code-block} diff
   :caption: GitHub Release Notes Form
   +## Overview
   +* Paste in the stuff from `CHANGELOG.md` here.
   +
    ## What's Changed
    * List of PRs that were merged, but sometimes the names aren't helpful.
   ```

4. Hit "Update release."

Users can then install a specific version directly from the release
URL:

```console
$ pip install https://github.com/ekiourk/bytewax/releases/download/v1.2.3/bytewax-1.2.3-cp310-abi3-manylinux_2_28_x86_64.whl
```

(Adjust the wheel filename to match the user's platform.)

## 4. Future: PyPI publishing

When PyPI publishing is set up, this section will document the
GitHub-native flow via [Trusted Publishing](https://docs.pypi.org/trusted-publishers/):

- A separate workflow (`.github/workflows/publish-pypi.yml`) will
  trigger on `release.published` events.
- It uses `pypa/gh-action-pypi-publish` with OIDC — no PyPI tokens
  stored in GitHub.
- The PyPI project name will likely **not** be `bytewax` (that's
  owned by the original Bytewax company). A fork-specific name like
  `bytewax-community` or similar will be chosen at the time of
  publication.

Until then, GitHub Releases are the canonical distribution channel for
this fork.
