# Documentation Site

This directory is a standalone Hugo source tree using the Relearn theme. The
site content lives in `content/`; the legacy implementation notes beside this
file are retained as historical source material.

## Preview locally

Install Hugo extended, then run:

```bash
make docs-serve
```

The documentation is a Hugo Module. The Make targets pin and fetch a Relearn
revision compatible with Hugo 0.93.0 or newer on first use; Hugo records the
resolved version in `docs/go.mod` and `docs/go.sum`. A later offline build uses
the Go module cache.

## Authoring rules

- Add a front matter block with `title` and `weight` to every page.
- Use `mermaid` fenced blocks for interactions spanning components.
- Keep payload semantics explicit: minipulsar transports opaque bytes and does
  not interpret message schemas.
- Update `content/development/codebase-reference.md` whenever a source file is
  added, removed, or materially repurposed.
