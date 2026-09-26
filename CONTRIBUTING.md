# Contributing to s3-pylogger

Thank you for your interest in contributing! Here's everything you need to get started.

## Getting started

### 1. Fork and clone

```bash
# Fork on GitHub, then:
git clone https://github.com/<your-username>/s3-pylogger.git
cd s3-pylogger
```

### 2. Install in editable mode with dev dependencies

```bash
pip install -e ".[dev]"
```

This installs `pytest` and `moto` (the S3 mock library) — no real AWS
credentials are needed to run the tests.

### 3. Run the tests

```bash
pytest tests/ -v
```

All 15 tests should pass before you start making changes.

---

## Making changes

### Branch naming

Create a descriptive branch from `main`:

```bash
git checkout -b fix/json-formatter-datefmt
git checkout -b feat/cloudwatch-support
git checkout -b docs/athena-guide
```

### Code style

- Follow existing code patterns in [`handler.py`](src/s3_pylogger/handler.py)
- Keep all public APIs documented with NumPy-style docstrings
- All new options should have a corresponding entry in the options table in [`README.md`](README.md)

### Tests

- Every new feature **must** come with at least one test in [`tests/test_basic.py`](tests/test_basic.py)
- Every bug fix **should** include a regression test
- Tests use `moto` to mock S3 — no AWS credentials needed
- Run the full suite before opening your PR:

```bash
pytest tests/ -v
```

---

## Opening a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feat/my-feature
   ```

2. Open a Pull Request against the `main` branch of this repo

3. Fill in the PR description:
   - **What** the change does
   - **Why** it's needed
   - Any relevant issue numbers (`Fixes #123`)

4. CI will automatically run your tests against **Python 3.8 – 3.12**. All checks must pass before merging.

---

## Types of contributions welcome

| Type | Examples |
|---|---|
| 🐛 Bug fixes | Incorrect S3 key format, threading issues, encoding edge cases |
| ✨ Features | New log fields, additional formatters, multipart upload support |
| 📖 Documentation | Clearer README examples, docstring improvements, Athena query recipes |
| 🧪 Tests | More edge-case coverage, additional Python version compatibility |
| 🔧 Dev tooling | Linting, type checking, pre-commit hooks |

---

## Reporting issues

Please open a [GitHub Issue](../../issues) with:
- Python version and OS
- Minimal code to reproduce the problem
- Expected vs. actual behaviour
- Any relevant error tracebacks

---

## Credits

This project ports the original Node.js
[s3-streamlogger](https://github.com/Coggle/s3-streamlogger) by
[Coggle Ltd.](https://coggle.it). Please see [LICENSE](LICENSE) for
full attribution.
