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

## Scheduled database updates

The `Scheduled Database Update` workflow runs daily at 10:00 UTC and can also
be dispatched manually from `main`. It runs the extraction bash script for the
National League, then the American League, using a temporary copy of the database.
Only after both extractions and database validation succeed does it publish a
database-only commit to `maintenance/scheduled-database-update`.

An open PR titled `Scheduled Database Update` is reused each day. Its branch's
database is the starting point, so daily snapshots accumulate until the PR is merged. The repository deletes merged branches automatically; the next run then
creates the branch again from current `main`. Local extraction defaults are
unchanged. Season selection remains in `statsapi_parameters_script.py`.

If extraction fails, the published database is unchanged. Rerunning on the same
UTC date replaces that day's snapshots. If a branch push succeeded but PR creation
failed, the next run resumes the unpublished branch. A PR closed without merging
blocks collection while its branch exists. If that branch was deleted, the next
run starts a new collection from `main`. If a merged PR's branch still exists,
inspect it for unmerged data before deleting it
and rerunning. The workflow never force-pushes or automatically resolves database
conflicts. Code changes on `main` reach an open collection branch only when you
update it or begin the next collection cycle.

## Project structure

- `dags/`: Airflow DAGs for daily extraction and weekly reporting.
- `bash/`: shell commands called by the DAGs.
- `mlb_airflow_data_pipeline/`: Python code for API extraction, SQLite persistence, data treatment, feature creation, analysis, and reporting.
- `tests/unit/` and `tests/integration/`: unit and integration tests.
- `.github/workflows/`: GitHub Actions test workflow.
- `.pre-commit-config.yaml`: whitespace, YAML, mypy, and Ruff hooks.
