# Agent playbook

`@johnhenry/dataflow` -- an async dataflow scheduler that decides when nodes run and tracks their run states, without ever storing node values. Single package, Node >= 26, `node:test` (`npm test`), ships source; no build step. It has no DOM or Node-specific code: keep it that way, because it runs in browsers and Workers too.

`CLAUDE.md` in this directory is a symlink to this file.

## The verification loop (before every push)

1. `npm test` -- runs `node --test test/` and then every `examples/NN-*.mjs` (each asserts what it demonstrates). 0 failed and 0 skipped.
2. `npm pack --dry-run` -- read the file list: `src/`, `README.md`, `CHANGELOG.md`, `LICENSE`, `package.json`, nothing else.
3. A genuinely fresh clone: `git clone . /tmp/dataflow-verifyN && cd $_ && npm ci && npm test`.
4. Commit, push, close the issue with a comment naming the commit SHA.

CI (`.github/workflows/ci.yml`) calls `johnhenry/workflows`' reusable `ci.yml@v1` with `npm test`, plus `workflow-lint`; match it locally.

## Repo-specific gotchas

- **"Never run" and "asked to run" are different states.** A queued node that has never run must become `waiting` (not stay `idle`) when its input fails, or it never runs after the input recovers. This shipped broken in the first draft and only `examples/03` caught it; the regression test is "a queued, never-run dependent of a failing node waits...".
- **Every superseded or reset run must bump `runId`.** `settle()` drops results by comparing run ids; a code path that aborts a run without bumping `runId` lets a late result through.
- **Don't add value storage.** Equality cut-off, caching and persistence belong to the caller; the README's Honest limitations says so on purpose.

## Definition of done

- A regression test exists for any bug fixed.
- Anything the scheduler does **not** do is stated in the README, not only in an issue comment.
- `CHANGELOG.md` has an entry citing the commit/PR; `examples/README.md` lists every example.
- `src/index.d.ts` matches the runtime API.

## Non-goals

- Storing, comparing or persisting node values (see README, Honest limitations).
- Hard cancellation: the scheduler can't kill code; the caller restarts its own runtime and calls `reset()`.

## Releases

`main` is the release branch. Bump `version` in `package.json` and add the dated `CHANGELOG.md` entry in a PR; merging it publishes through `.github/workflows/publish.yml` (npm trusted publishing, idempotent `npm view` guard), which then creates the `v<version>` tag and GitHub Release. Don't run `gh release create`. Don't rename `publish.yml`: npm trusted publishing is bound to that filename.
