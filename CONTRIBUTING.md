# Contributing to HAOlib

Thank you for considering contributing to HAOlib! This document provides guidelines and information for contributors.

## Table of Contents

- [Contributing to HAOlib](#contributing-to-haolib)
  - [Table of Contents](#table-of-contents)
  - [Code of Conduct](#code-of-conduct)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Development Setup](#development-setup)
  - [Making Changes](#making-changes)
    - [Branch Naming](#branch-naming)
    - [Types of Contributions](#types-of-contributions)
  - [Testing Guidelines](#testing-guidelines)
    - [Running Tests](#running-tests)
    - [Writing Tests](#writing-tests)
    - [Coverage Requirements](#coverage-requirements)
  - [Code Style](#code-style)
    - [Linting and Formatting](#linting-and-formatting)
    - [Code Standards](#code-standards)
  - [Submitting Changes](#submitting-changes)
    - [Pull Request Process](#pull-request-process)
    - [Commit Messages](#commit-messages)
  - [Release Process](#release-process)
    - [Version Numbering](#version-numbering)
    - [Release Checklist](#release-checklist)
  - [Getting Help](#getting-help)
  - [Recognition](#recognition)

## Code of Conduct

This project adheres to No Code of Conduct.  We are all adults.  We accept anyone's contributions.  Nothing else matters.

For more information please visit the [No Code of Conduct](https://github.com/domgetter/NCoC) homepage.

## Getting Started

### Prerequisites

- Python 3.13 or higher
- Git for version control

### Development Setup

1. **Fork and Clone**

   ```bash
   git submodule add https://github.com/hao-vc/haolib.git
   cd haolib
   ```

2. **Install Dependencies**

   ```bash
   # Using uv (recommended)
   uv sync
   ```

3. **Set Up Pre-commit Hooks** (optional but recommended)

   ```bash
   pre-commit install
   ```

4. **Verify Installation**

   ```bash
   uv run pytest --version
   ```

## Making Changes

### Branch Naming

Create feature branches with descriptive names:

- `feature/add-new-middleware`
- `fix/jwt-token-validation`
- `docs/update-configuration-guide`
- `refactor/improve-exception-handling`

### Types of Contributions

1. **Bug Fixes**
   - Include reproduction steps in the issue
   - Add regression tests
   - Update relevant documentation

2. **New Features**
   - Discuss the feature in an issue first
   - Include comprehensive tests
   - Update documentation and examples
   - Follow existing code patterns

3. **Documentation**
   - Improve clarity and completeness
   - Add practical examples
   - Fix typos and formatting

4. **Performance Improvements**
   - Include benchmarks showing improvement
   - Ensure no functionality is broken
   - Document any API changes

## Testing Guidelines

### Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=haolib --cov-report=html

# Run specific test file
uv run pytest tests/test_jwt.py

# Run tests in parallel
uv run pytest -n auto
```

### Writing Tests

You have to write tests for all new features and bug fixes.

### Coverage Requirements

- Maintain at least 90% code coverage
- New features must include comprehensive tests
- Critical paths require 100% coverage

## Code Style

### Linting and Formatting

We use `ruff` for both linting and formatting:

```bash
# Check code style
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check . --fix

# Format code
uv run ruff format .

# Check types
uv run mypy haolib
```

### Code Standards

1. **Python Style**
   - Follow PEP 8 guidelines
   - Use type hints for all public APIs
   - Maximum line length: 120 characters
   - Use descriptive variable names

2. **Imports**

   Handled by `ruff`.

3. **Docstrings**
   Use Google-style docstrings for all public functions and classes:

   ```python
   def encode_token(self, data: dict, expires_in: int = None) -> str:
       """Encode data into a JWT token.

       Args:
           data: The data to encode into the token
           expires_in: Token expiration time in minutes

       Returns:
           The encoded JWT token string

       Raises:
           JWTEncodeError: If token encoding fails

       Examples:
           >>> service = JWTService("secret", "HS256")
           >>> token = service.encode_token({"user_id": 1}, expires_in=60)
       """
   ```

4. **Error Handling**
   - Use specific exception types
   - Include helpful error messages
   - Log appropriately for debugging

## Submitting Changes

### Pull Request Process

1. **Before Submitting**
   - Run all tests and ensure they pass
   - Update documentation if needed
   - Add appropriate changelog entries
   - Ensure code coverage is maintained

2. **Pull Request Template**

   ```markdown
   ## Description
   Brief description of the changes

   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update

   ## Testing
   - [ ] Tests added/updated
   - [ ] All tests pass
   - [ ] Coverage maintained

   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Self-review completed
   - [ ] Documentation updated
   - [ ] Changelog updated
   ```

3. **Review Process**
   - At least one maintainer must approve
   - All CI checks must pass
   - Address review feedback promptly
   - Keep PR focused and reasonably sized

### Commit Messages

Follow conventional commit format:

```bash
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance tasks

**Examples:**

```text
feat(jwt): add token refresh functionality

Add support for refreshing JWT tokens with extended expiration
times while maintaining security best practices.

Fixes #123
```

```bash
fix(middleware): handle missing idempotency key header


Previously, the idempotency middleware would raise an exception
when the header was missing. Now it gracefully continues without
idempotency checking.
```

## Release Process

### Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

1. Update version in `pyproject.toml`
2. Create and push version tag

## Getting Help

If you need help or have questions:

1. Check existing [issues](https://github.com/your-org/haolib/issues) and [discussions](https://github.com/your-org/haolib/discussions)
2. Create a new issue with detailed information
3. Join our community chat (link TBD)
4. Contact maintainers directly for sensitive issues

## Recognition

Contributors are recognized in:

- CHANGELOG.md for each release
- GitHub contributors page
- Special recognition for significant contributions

Thank you for contributing to HAOlib! 🚀
