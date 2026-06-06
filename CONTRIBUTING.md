# Contributing

Thanks for taking the time to contribute.

## Workflow

1. Create a branch from `main`.
2. Make your change with tests.
3. Run the checks locally:

   ```sh
   make tidy
   make lint
   make test
   ```

4. Open a pull request describing the change and the motivation behind it.

## Code style

- Keep the public API stable; breaking changes require a major version bump.
- Run `gofmt`/`goimports` (covered by `make fmt`).
- Add a `CHANGELOG.md` entry for user-visible changes.
