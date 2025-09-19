# Contributing Guidelines for correlator

Thank you for considering contributing to correlator! This document provides guidelines to make the process as smooth as possible for everyone involved.

## Code of Conduct

We expect all contributors to adhere to a high standard of conduct, treating all participants with respect and fostering an inclusive environment.

## Reporting Bugs and Issues

If you find a bug or issue with correlator, please open an issue in the project's [issue tracker](https://github.com/correlator-io/correlator/issues). Please provide as much detail as possible, including:

- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Environment details
- Stack traces or error messages

## Contributing Code

If you would like to contribute code to correlator, please follow these guidelines:

1. Fork the Project
2. Create your Feature [Branch](#branch-naming-convention-and-commit-message-format) (`git checkout -b major/AmazingFeature`)
3. [Commit](#commit-message-guidelines) your Changes (`git commit -sm 'major: Add some AmazingFeature'`)
4. Push to the Branch (`git push origin major/AmazingFeature`)
5. Open a Pull Request

Before submitting a pull request, please ensure that your code adheres to the following guidelines:

- Write clear and concise [commit messages](#commit-message-guidelines)
- Include tests for any new functionality or bug fixes (use **TDD**)
- Ensure your changes pass all pre-commit hooks
- Run `make fmt`, `make lint`, `make test` to verify all checks pass (formatting, linting, tests)

By contributing to correlator, you agree to license your contributions under the terms of the Apache License 2.0.

### Code Review

All code contributions will be reviewed by a maintainer of the project. The maintainer may provide feedback or request changes to the code. Please be patient during the review process.

## Development Environment Setup

To set up the development environment:

```bash
# Clone the repository
git https://github.com/correlator-io/correlator.git
cd correlator

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

## Quality Assurance

The project uses several code quality tools that can be run via Make commands:

```bash
# Format code with golangci
make fmt

# Run linting with golangci
make lint

# Run type checking with vet
make vet

# Run unit tests
make test-unit

# Run integration tests
make test-integration

# Run all tests
make test
```

## Branch Naming Convention and Commit Message Format

The branch naming convention and commit message format are as follows:

- Branch naming convention: `type/branch-name`
- Commit message format: `type: commit message`

The `type` can be one of the following:

- `minor`: Minor changes or a new feature
- `major`: Major changes or breaking change
- `patch`: A bug fix
- `test`: Adding tests
- `chore`: Maintenance tasks such as updating dependencies or configuration files or bootstrap code

### Commit Message Guidelines

To maintain consistency and clarity in our project history, all commit messages **must** follow the format: `type: commit message` and the **must** be signed, for example; `git commit -sm 'major: Add some AmazingFeature'`

#### Accepted Types
- **minor**: For minor changes or new features.
- **major**: For major changes or breaking changes.
- **patch**: For bug fixes.
- **chore**: For maintenance tasks, such as updating dependencies or configuration files or bootstrap code.

#### Examples
- `minor: Refactor cache implementation`
- `major: Add api endpoint`
- `patch: Fix null pointer in api endpoint handler`
- `chore: Update CI configuration`

#### Why This Matters
Using a consistent format for commit messages helps:
- Easily identify the purpose and impact of each commit
- Streamline the release process by automatically generating changelogs
- Improve collaboration and understanding among team members

Make sure to follow these guidelines for every commit to keep our project history clean and meaningful!

## Testing

All new features and bug fixes should be accompanied by appropriate tests. We maintain strict separation between unit and integration tests using Go's `testing.Short()` flag.

### Test Types and Conventions

**Unit Tests**:
- Test individual functions/methods in isolation
- Use mocks/stubs for external dependencies
- Keep tests fast (< 100ms each)
- **Required**: Add this skip condition at the start of every unit test:
  ```go
  if !testing.Short() {
      t.Skip("skipping unit test in non-short mode")
  }
  ```

**Integration Tests**:
- Test end-to-end workflows with real external services
- Use testcontainers for database testing
- Allow longer timeouts (up to 10 minutes)
- **Required**: Add this skip condition at the start of every integration test:
  ```go
  if testing.Short() {
      t.Skip("skipping integration test in short mode")
  }
  ```

### Running Tests

```bash
# Run unit tests only (fast feedback loop)
make test-unit

# Run integration tests only (slower, with real services)
make test-integration

# Run all tests with coverage
make test
```

### Test File Organization

- Place test files alongside the code they test (e.g., `config.go` → `config_test.go`)
- Use descriptive test function names that clearly indicate scope
- Follow table-driven test patterns for multiple test cases

## License

By contributing to correlator, you agree to license your contributions under the terms of the Apache License 2.0.

If you have any questions or issues, please open an issue in this repository.