# MLB Airflow Data Pipeline

This project extracts MLB statistics from the public MLB Stats API, stores them in SQLite, derives player statistics, and produces charts and HTML reports. Apache Airflow schedules daily extraction and weekly reporting workflows for the American and National Leagues.

## Technologies

- Python (version pinned in `pyproject.toml`)
- Apache Airflow for scheduling and orchestration
- MLB-StatsAPI for access to MLB data
- pandas and NumPy for tabular data and feature calculations
- SQLite for local persistence
- Matplotlib for charts

## Set up a development environment

Use Linux or macOS, install [Micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html), and create the environment with the Python version specified by `requires-python` in `pyproject.toml`:

```sh
micromamba create -n mlb-airflow-env python=3.14.7
micromamba install -n mlb-airflow-env -c conda-forge apache-airflow google-re2
micromamba run -n mlb-airflow-env python -m pip install -e .
```

The Python version above matches the project metadata. Install `pre-commit` and enable the repository hooks:

```sh
micromamba run -n mlb-airflow-env python -m pip install pre-commit
micromamba run -n mlb-airflow-env pre-commit install
```

## Run checks

```sh
micromamba run -n mlb-airflow-env pytest tests
micromamba run -n mlb-airflow-env pre-commit run --all-files
```

Pytest excludes tests marked `manual` by default; see `pytest.ini`.

## Project structure

- `dags/`: Airflow DAGs for daily extraction and weekly reporting.
- `bash/`: shell commands called by the DAGs.
- `mlb_airflow_data_pipeline/`: Python code for API extraction, SQLite persistence, data treatment, feature creation, analysis, and reporting.
- `tests/unit/` and `tests/integration/`: unit and integration tests.
- `.github/workflows/`: GitHub Actions test workflow.
- `.pre-commit-config.yaml`: whitespace, YAML, mypy, and Ruff hooks.

