# Changelog

All notable changes to this package are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/).

## [1.0.0]

First release under Nimble Tech.

### Added
- Transactional outbox for PostgreSQL with pgx and gorm persisters.
- Polling relay with partitioned fan-out and bounded publish retries.
- Per-message `Headers` persisted in a nullable `headers` column.
- `make` targets, golangci-lint config, GitHub Actions CI and dependabot.

### Changed
- Relay publishes via a context-aware fan-out helper with no external
  concurrency dependency.

### Fixed
- `PGXAdapter.Exec` now forwards query arguments correctly.
