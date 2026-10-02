# Security Policy

`realtime-monitoring-platform` is a portfolio / reference implementation maintained by
[Shiv Kumar](https://github.com/shivkumarsinghsky). It is not operated as a hosted service.

## Reporting a vulnerability

Please report security issues privately through GitHub's
[private vulnerability reporting](https://github.com/shivkumarsinghsky/realtime-monitoring-platform/security/advisories/new)
rather than opening a public issue. Include steps to reproduce and the affected files or components.

## Scope

- Code, configuration and documentation in this repository.
- Insecure guidance in the architecture documents is also in scope — if a recommendation here would be unsafe
  in a real deployment, please report it.

## Secrets

No real credentials are stored in this repository. `.env.example` contains placeholders only; copy it to `.env`
(which is git-ignored) for local use.
