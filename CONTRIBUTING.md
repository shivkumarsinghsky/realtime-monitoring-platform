# Contributing

Thanks for your interest in `realtime-monitoring-platform`. This repository is a reference implementation, so contributions that
improve clarity, correctness or test coverage are the most valuable.

## Workflow

1. Open an issue describing the change, especially for architectural changes.
2. Create a branch from `main` and keep pull requests focused.
3. Run the linters and tests locally (see the README's *Testing* section) — CI runs the same checks.
4. For architectural changes, add or update an ADR in `docs/decisions/`.

## Conventions

- Conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`).
- Diagrams are Mermaid blocks in Markdown so they can be reviewed as text.
- Never commit secrets; use `.env` (git-ignored) for local configuration.
- Keep claims accurate: describe what the code demonstrates, not what it might do in production.
