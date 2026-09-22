# Working in this repository

Write clear, concise Python that fits the existing project. Prefer straightforward code and small, cohesive functions over cleverness or unnecessary abstraction. Avoid writing defensive code if possible.

Use Clean Code, DRY, KISS, YAGNI, and SOLID as guidance; apply them with judgment rather than adding layers or generality without a current need.

## Examples of good practice

- Treat the [LAPACK reference BLAS implementation](https://github.com/Reference-LAPACK/lapack/tree/master/BLAS) as an example of focused routines with defined responsibilities that can be composed into larger algorithms. Apply the idea through clear inputs, outputs, and behavior; do not imitate Fortran-specific conventions in Python.
- Prefer small units that compose into the pipeline's larger work. Keep orchestration, data transformation, and persistence responsibilities distinct where that makes behavior easier to understand and verify.
- Test observable behavior and important boundary cases. Avoid tests that merely repeat implementation details.

## Project layout

- `mlb_airflow_data_pipeline/`: extraction, database, transformation, and reporting code.
- `dags/`: Airflow DAG definitions and orchestration.
- `bash/`: shell entry points used by the DAGs.
- `tests/unit/` and `tests/integration/`: unit and integration coverage.

Keep code in the layer that owns its behavior. Avoid duplicating pipeline logic in DAGs or shell scripts when the Python package can own it.

## Code changes

- Follow the surrounding code's naming, formatting, and dependency patterns. Use type hints for public functions and meaningful return values.
- Keep functions focused; use names that explain domain intent. Add comments only when they explain a non-obvious reason or constraint.
- Handle expected failure cases at the boundary that can respond usefully. Let unexpected errors retain their original traceback; do not catch broad exceptions just to log and continue.
- Preserve useful context when adding error messages or logs. Do not log credentials, tokens, or other secrets.
- Keep database writes explicit and review transaction, commit, and failure behavior when changing persistence code.
- Do not add dependencies, configuration layers, or compatibility paths unless the task needs them.
- Never commit credentials, local machine paths, generated database contents, or other machine-specific artifacts.

## Tests and verification

- Put focused unit coverage in `tests/unit/` and tests requiring the database, Airflow, or external boundaries in `tests/integration/`.
- When asked to verify a change, run the narrowest relevant checks first. The default pytest configuration excludes tests marked `manual`.
- Do not change unrelated tests or generated data as part of an implementation.

## Contributions

### Branches

Create a branch from an up-to-date `main`. Use a prefix found in the repository's branch history, then add a short, lowercase, hyphen-separated description:

- `enhancement/` for new 
- `maintenance/` for upkeep, dependency, and tooling changes.
- `observability/` for logging and diagnostics.
- `testing/` for tests and test infrastructure.
- `infrastructure/` for environment, pipeline, and runtime setup.
- `fix/` for bug fixes.


### Pull requests

Push the branch and open a pull request against `main` with GitHub CLI:

```sh
gh pr create --base main --head <branch> --title "<short description>" --body-file <description-file>
```

Base the description on the changes in the branch and the checks actually run. Keep it succinct and specific, do not use markdown sections, bold or italics. Use this format:

```markdown
<One sentence summary on what changed and why. Reference to any relevant issues or previous PRs>
```

