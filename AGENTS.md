# Developer Instructions

This repository contains processing scripts for DeSC data. Please keep sensitive or large data outside the repository as described in the README.

## Coding Guidelines
- Use Python 3.9 or above.
- Follow PEP 8 style conventions.
- Format code with `black` (default settings) before committing.
- Run `flake8` for linting.
- Document new functions and modules with docstrings.
- Use English for variable and function names.
- follow `.clinerules` to act as an agent AI.

## Contribution Workflow
1. Ensure `black --check .` and `flake8` run without errors.
2. Add or update tests where appropriate and run `pytest`.
3. Commit changes with clear messages in English.

Large data files such as `.feather` or `.csv` should not be tracked in Git.
