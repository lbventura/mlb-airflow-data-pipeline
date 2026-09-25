# Implementation plan: measure and improve `statsapi_extraction_script`

Related issue: [#61 — `statsapi_extraction_script` performance](https://github.com/lbventura/mlb-airflow-data-pipeline/issues/61)

This plan incorporates the feedback in [the original proposal](statsapi-extraction-runtime-proposal.md): measure before changing the extractor, profile the extraction directly instead of running the full DAG, use threads if concurrency is warranted, and use a small roster helper to carry player IDs.

## What is known now

The log records a completed American League extraction on 2026-09-23, from `14:52:13.154905Z` to `15:08:22.151178Z`, with 796 output rows across 15 teams and zero reported failed teams or inactive players. Rows are not necessarily unique players: a player can appear for more than one team. The log does not record the source revision, effective request parameters, or whether the API responses were live or substituted, so treat it as preliminary evidence until the new harness establishes those facts.

| Scope | Elapsed time | Evidence |
| --- | ---: | --- |
| Whole extraction task | 16m 09.0s | `extraction_started` to `extraction_completed` |
| Standings and roster preparation | 8.64s | `extraction_started` to `league_standings_loaded` |
| Standings database write and surrounding logging | 0.004s | `league_standings_loaded` to `league_standings_saved` |
| Team mapping and surrounding logging | 0.001s | `league_standings_saved` to `team_mapping_created` |
| Player extraction plus player database write | 16m 00.4s | `team_mapping_created` to `extraction_completed` |
| Final concatenation, schema check, player database write, and logging | 0.021s | last team success to `extraction_completed` |

The intervals between the 15 sequential team completions were about 49 to 79 seconds. These event intervals locate the likely bottleneck; they are not isolated function timings. The log does not identify how much time belongs to `lookup_player`, `player_stats`, response parsing, or pandas work, so it cannot yet justify a particular worker count.

Other attempts in the log report connection resets after long waits. Include failed and timed-out attempts in reliability comparisons, with their duration and completed work. Compare successful-run latency separately; collecting only eventual successes would hide a slower or less reliable candidate.

## Phase 1: establish a reproducible baseline

### 1. Add a manual direct-extraction profiling test

Add `tests/integration/test_statsapi_extraction_performance.py` with two manual tests sharing a small runner: `test_extraction_profile` for diagnosis and `test_extraction_benchmark` for timings with `cProfile` disabled. Parameterize both by `american_league` and `national_league`. Start with the American League; validate the selected solution on both leagues. `pytest.mark.manual` matches the repository's existing default exclusion in `pytest.ini`.

The first implementation changes only the measurement harness. Reproduce the current script's operations and their order without Airflow:

1. Open a fresh temporary SQLite database and create `DataExtractor(league_name=league_name)`.
2. Run the existing `set_league_team_rosters_player_names()` once. It already loads standings; do not call the standings method separately and add requests to the baseline.
3. Call `ensure_dataframe_columns()` for standings, then write them with `insert_dataframe()`.
4. Run `set_team_ids_and_names()`.
5. Run `get_player_stats_per_league()`.
6. Reproduce the current entry point's persistence preparation: add `league_name=league_name` to a copy of the player DataFrame, call `ensure_dataframe_columns()` for that copy, insert it, and close the database. Measure preparation and insertion together; retain the original extraction DataFrame for schema/identity comparisons.

Measure this block after test setup and module imports, ending before assertions and artifact serialization. Use the database helpers directly with the temporary path. Check extraction columns against `expected_output_columns()` and check the stored copy's added league column separately. Do not edit the league-choice file to select the test league. Update the runner explicitly if Phase 2 renames a method; no compatibility branch is needed.

The column helper returns sorted names, so compare sorted column names for the schema assertion and record actual column order separately for baseline/candidate equivalence. Verify persisted values and row multiplicity after reopening the temporary database, accounting for SQLite's type representation. Bind test logging to the explicit league: the module logger otherwise inherits the league-choice file's value. Resolve and record the imported module paths so an editable installation cannot accidentally measure a different worktree.

Record the effective inputs before the first run. Currently `SEASON_YEAR` is 2023 for rosters and lookups, but `player_stats()` receives no season argument. Holding that constant alone does not freeze the statistics. Record the effective `person` hydration parameters and execution date; characterize any season correction separately before using it in a speed comparison. Imported constants and default arguments also mean that changing the parameters module after import may not change the called functions.

### 2. Capture wall time, request data, and a diagnostic profile

In that test, add small test-local helpers with one purpose each:

- A stage timer based on `time.perf_counter()` for setup, player extraction, and persistence. Time team extraction calls as well, including failed calls. Add `time.process_time()` for total process CPU time, which helps distinguish computation from waiting.
- A wrapper around `statsapi.get` installed with `monkeypatch`. Record endpoint, selected parameters (team/player ID, season, stats hydration), elapsed time, and success or exception in `finally`; call the saved original exactly once with unchanged arguments and re-raise its exception. Measure wrapper overhead outside the request interval. Read an HTTP status from an exception's response when available.
- A `cProfile.Profile` enabled only around the diagnostic extraction block. Save it as a `.prof` file and render the top 30 entries by `pstats.SortKey.CUMULATIVE` and `pstats.SortKey.TIME` to inspect inclusive and self time. The benchmark test leaves the profiler disabled and keeps the same lightweight timers and recorder for every candidate.

`statsapi.get` timing includes URL construction, HTTP waiting, and JSON decoding. Its count is a count of wrapper invocations, not necessarily wire requests if redirects occur. Use the profile to distinguish those costs and inspect callers of `lookup_player` and `player_stats` when necessary. Default profiler timings include blocking time; they are not CPU utilization measurements. Profiling adds overhead, so use the unprofiled test for speed comparisons. See the [Python profiler documentation](https://docs.python.org/3.14/library/profile.html) and [Python clocks](https://docs.python.org/3.14/library/time.html#time.process_time).

Treat `STATSAPI_PROFILE_OUTPUT_DIR` as an optional artifact root. Create a unique subdirectory for every attempt, including league, mode, worker count, and a unique run ID, so repeated runs cannot overwrite one another. Default to pytest's `tmp_path` and print the actual path at startup and completion. Temporary output may be cleaned later; use an explicit retained directory for comparisons. Write `summary.json` for every attempt; write `.prof` and `pstats.txt` only in diagnostic mode. Save output snapshots with the player-ID index and schema metadata for correctness comparisons. Keep generated artifacts out of commits and put the resulting timings and conclusions in a concise Markdown report.

Each JSON summary should contain:

- run ID and UTC start time, source SHA plus a dirty-state/source digest, Python/MLB-StatsAPI/requests/pandas versions, league, effective parameters, live or fixture-backed source, profiling mode, and worker count;
- total wall and process CPU seconds, stage and team durations, and overall outcome (`success`, `failed`, or `timed_out`);
- extraction completion and validation status separately, so a completed extraction with incorrect coverage remains a failed attempt without losing its useful timing evidence;
- per-endpoint call count, total elapsed seconds, median, p95, maximum, and exceptions grouped by type/status; include sample counts because small samples make percentiles weak evidence;
- requested and completed teams, roster entries, output rows, unique player IDs, per-team `(team_id, player_id)` membership, inactive/failed-team details, column order, dtypes, and output snapshot paths;
- diagnostic artifact paths when profiling was enabled, and the error/stage when extraction or validation failed.

Use a lock only for updates to shared recorder state, never around a network call. Keep record creation and artifact output outside the measured API duration. After adding threads, endpoint durations overlap and their sum may exceed extraction wall time; do not treat that sum as a percentage of wall time.

Allowlist recorded parameters and associate calls with their stage and team without adding request arguments. Capture roster IDs from responses already returned by the wrapper; do not issue extra validation requests. Count started, completed, and interrupted calls separately. Document the percentile calculation and use `null`, not zero, when no duration was observed. Timers are nested: do not add team time, request time, and player-stage time together. Checkpoint writes contribute to overall wall time even when excluded from API intervals; report recorder overhead and keep its policy identical across compared runs.

Write summaries in `finally` so setup errors, an empty-concatenation error, schema assertions, and test failures also leave evidence. Fail the test on failed teams, empty output, unexpected columns, or unexplained coverage differences. A successful process exit alone is insufficient: the extractor can return partial data. Use small offline tests to verify recorder counts and error artifacts before spending time on live runs.

Declare a deadline for each live attempt (initially 30 minutes) and supervise the test in a separate process. Preserve completed-team checkpoints and have the supervisor record a timeout if it terminates the attempt; a killed process cannot reliably execute `finally`. The current helper calls have no explicit HTTP timeout. Keep transport changes out of the baseline. A future timeout does not stop a running HTTP call, so thread cancellation cannot substitute for process supervision. See [executor shutdown behavior](https://docs.python.org/3.14/library/concurrent.futures.html#concurrent.futures.Executor.shutdown).

Implement supervision inside the shared test runner: the pytest parent creates the attempt directory and initial summary, then launches one extraction worker with the same Python interpreter. Install patches and profiling inside that worker. Flush a small request/stage event journal and atomically replace summary checkpoints. On timeout, allow a bounded termination grace period, then kill and reap that attempt's process group if necessary. Record supervisor elapsed time separately from extraction wall time, preserve the last checkpoint on startup failure or forced termination, and mark unavailable profiles or partial snapshots explicitly. Artifact-writing failures must not replace the original extraction exception. Exercise startup failure, timeout, and cleanup with offline child processes before live runs.

After the tests exist, select the diagnostic or benchmark case explicitly from the repository root (use the declared process deadline for each invocation):

```sh
profile_dir="$(mktemp -d)"
STATSAPI_PROFILE_OUTPUT_DIR="$profile_dir" micromamba run -n mlb-airflow-env pytest -o addopts="" -m manual -s 'tests/integration/test_statsapi_extraction_performance.py::test_extraction_profile[american_league]'
STATSAPI_PROFILE_OUTPUT_DIR="$profile_dir" micromamba run -n mlb-airflow-env pytest -o addopts="" -m manual -s 'tests/integration/test_statsapi_extraction_performance.py::test_extraction_benchmark[american_league]'
```

Run one diagnostic profile, then a fixed batch of three benchmark attempts with profiling disabled. Retain every attempt. Use a fixed revision or isolated source snapshot so concurrent workspace edits cannot change the experiment. Keep inputs, dependencies, hardware, recorder, logging, and database setup fixed, and avoid concurrent extraction jobs. If fewer than three attempts succeed or timings vary widely, report the result as inconclusive and investigate before another fixed batch. Do not rerun indefinitely until only successes remain.

The commands above each execute one attempt; repeat the benchmark invocation three times sequentially, recording failures without discarding them. Save the measured source snapshot or a reconstructable patch alongside its digest, including the harness and any pre-existing uncommitted changes. Record the installed client version and source digest as well. Check source digests before and after each attempt; label changed-source runs non-comparable. Keep environment-specific interpreter paths in local commands and artifacts rather than committed documentation.

### 3. Use the baseline to make decisions

The expected current request pattern must be verified by the recorder, not assumed:

- `standings` is likely called three times because `set_league_division_standings()` calls `statsapi.standings_data()` inside its division loop.
- `team_roster` is likely called once per team.
- `sports_players` is called for each input name processed by `_set_player_name_ids()`. The formatted roster has a trailing newline; the current split can include an empty name. Count actual invocations rather than assuming they equal roster size.
- `person` is called for each resolved name processed by `player_stats()`, including calls that later fail or are classified as inactive. Duplicate IDs and early team failures can also change the relationship to output rows.

At the end of this phase, write `statsapi-extraction-performance-results.md` with run IDs, all outcomes, successful-run median/range, the dominant measured cost, and the first candidate to implement. Estimate saved time using the measured time in the calls that candidate removes; fewer calls alone does not establish an equal percentage reduction in runtime. The same report should record later comparisons.

If the unchanged baseline violates coverage checks, retain the failing assertions and report completed-run durations separately from valid successful-run latency. Characterize the mismatch with fixed responses before choosing a comparable reference; do not repair production behavior inside the measurement-only phase. If no valid baseline exists, explicitly report that a speedup or numerical acceptance target cannot yet be established. Phase 1 ends with the harness, artifacts, and results report; optimization implementation starts in Phase 2.

Phase 2 contains candidates, not predetermined winners. If repeated player lookups dominate, implement the roster helper first. If they do not, choose the measured bottleneck. The two redundant standings calls are a small independent change whose benefit should be measured separately. If local parsing or pandas work dominates, investigate that path before committing to network concurrency.

`cProfile` only observes the thread that enables it. For a later threaded run, compare whole-stage wall time and the locked request recorder; do not interpret a main-thread-only profile as a complete worker profile.

## Phase 2: remove duplicate API work

Implement the chosen change independently. Compare unprofiled benchmark attempts against both the original baseline and the preceding revision. Alternate baseline/candidate runs in matched pairs to reduce drift from API conditions or caching. Run another diagnostic profile only when an unexplained result warrants it.

### 4. Carry player IDs from the roster response

Add a small private helper in `mlb_airflow_data_pipeline/statsapi_extraction_script.py`:

```python
def _get_team_roster_players(team_id: int) -> dict[int, str]:
```

It should call `statsapi.get("team_roster", {"teamId": team_id, "season": SEASON_YEAR, "rosterType": "active"})` and return player ID to full name in roster order. Confirm the fields against a captured response before implementing the change. IDs provide stable keys when different players share a name; the helper needs no new client class or dependency.

Refactor the data flow as follows:

1. Replace `league_team_rosters_player_names` with `league_team_roster_players: dict[int, dict[int, str]]` on `DataExtractor`.
2. Rename its setup method to `set_league_team_roster_players()` and call the helper once per team.
3. Change `TeamStats` to accept that team's ID-to-name mapping, key stats and intermediate results by player ID, and remove `_set_player_name_ids()`.
4. Adapt DataFrame/name assignment and all callers, including `__main__`, the measurement runner, and existing extraction integration tests. Preserve stat-field flattening, column order/dtypes, team assignments, and league return structure.

First characterize behavior with fixed responses for ordinary names, suffixes/multipart names, duplicate names with distinct IDs, the blank trailing roster line, and inactive players. `_extract_player_name()` currently keeps only two words and lookup uses the first match. Using `fullName` and authoritative IDs can change output, not just runtime. Document and resolve these differences as correctness changes, including any collision in the existing inactive-player name mapping; do not silently waive mismatches or count dropped players as a speed improvement.

Keep the existing failure boundaries explicit. Roster loading occurs before `get_player_stats_per_league()` and outside its per-team handler, so malformed roster data currently aborts setup. Preserve that setup failure with its traceback and team context; the profiler must save the failed stage. Keep `TypeError` handling only at the current per-player stats boundary, and report other team-extraction exceptions as failed teams. Do not add lookup fallbacks or broad retries to make a measurement pass.

Use a small fixed API-response fixture to compare serial baseline and candidate DataFrames, including `(team_id, player_id)`, names, values, dtypes, duplicate rows, and inactive/failed-team results. Preserve the player index in snapshots: the current SQLite write uses `index=False`, so database row counts cannot establish ID equivalence. For live runs, report membership and value differences; matching row counts alone does not prove correctness. Once the roster change is in place, verify zero `sports_players` calls and one `team_roster` call per requested team on successful runs. Apply those expectations to the candidate, not the unmodified baseline.

Compare membership as counts of `(team_id, player_id)` pairs, not only sets, so duplicate rows remain visible. Reconcile roster IDs against active output and inactive records per team, with missing, unexpected, and duplicate IDs reported explicitly. Characterize repeated IDs in the raw roster before choosing a dictionary, since that representation collapses duplicates. Use frozen response fixtures for exact value equivalence; live statistics and the generated `date` can change between runs. Identify those differences field by field and exclude incomparable runs from speedup claims instead of weakening correctness checks.

### 5. Reuse the standings response

Change `set_league_division_standings()` to call `statsapi.standings_data()` once, then build one DataFrame for each configured division from that response. The current implementation requests the same league standings once for each division even though the default response contains all divisions.

Add a mocked unit test that confirms a 15-team league standings DataFrame is produced and `standings_data()` is called once. The profiler summary should then confirm one `standings` request per league run.

## Phase 3: introduce threads only if measurements support them

Use the latest measurements to decide whether overlapping team HTTP calls is worthwhile. Threads are the selected concurrency mechanism. If request removal already meets the measured objective, record that result; concurrency is not required solely to satisfy this phase.

1. Add a keyword-only `max_workers: int = 1` argument to `get_player_stats_per_league()`. Validate positive values and keep one worker as the serial reference. Parameterize the benchmark by 1, 2, and 4 only once the API exists. Set the script entry point to the measured choice after comparing results.
2. Use `concurrent.futures.ThreadPoolExecutor`; the pinned client uses synchronous HTTP calls, so threads overlap network waits without replacing the client.
3. Submit one `get_player_stats_dataframe_per_team()` call per team. Each worker owns its `TeamStats` and DataFrame.
4. Use `as_completed()` with a future-to-team map and call every future's `result()` in the main thread. Keep successful results keyed by team ID, retain inactive-player data, and handle failures with the existing team-level reporting.
5. Assemble rows, columns, inactive-player metadata, and failed-team names in original team order after collection. Keep setup mappings read-only while workers run and all database writes in the main thread. Test the case where every team fails as well as one failed team.
6. Do not create a second pool for player requests. A team pool already bounds simultaneous `person` calls.

Use fixed-response tests for serial/threaded output equivalence and synchronized fake calls to prove both overlap and the worker cap. A lock-protected in-flight counter and bounded events/barriers can test these properties without fragile sleep-based speed thresholds. Join workers before removing test patches. HTTP exceptions must retain their team context and must not become inactive-player records.

Benchmark 1, then 2, then 4 workers with profiling disabled, using fixed batches of three attempts and interleaved serial comparisons. Report wall time saved and speedup (`serial median / candidate median`) for both the player stage and whole extraction, the spread of successful timings, and failed/timed-out attempts out of all attempts. Select the smallest count with a repeatable gain beyond observed run-to-run variation and no observed degradation in coverage or failure rate. If results overlap or failures prevent a fair comparison, label the result inconclusive and investigate before increasing load. A small batch cannot prove that a failure rate is unchanged.

For each candidate configuration, declare three baseline/candidate pairs before running, alternating their order across pairs. Report paired absolute differences as well as medians, and retain failed pairs in the reliability counts. Do not advance to a higher worker count after observed throttling or increased failures without first investigating the current configuration. Worker count bounds simultaneous calls, not requests per second; record both peak in-flight calls and request throughput when assessing API load.

Confirm the chosen configuration on both leagues with the same inputs and measurement modes as their serial references. Account for overlapping AL/NL extraction processes: the worker limit is per process, not a global MLB API limit. Record whether other extraction jobs were running when measuring.

## Completion criteria

The issue is ready to close when the measured solution meets these criteria. The results report must say which candidate changes were selected or deferred and why:

- Phase 1 produced a diagnostic profile and unprofiled baseline before optimization, with reproducible inputs and an outcome for every attempt.
- Each implemented request reduction has its predicted call-count change verified. Omitted optimizations have a reason in the report.
- Fixed-response tests prove output equivalence or explicitly documented correctness changes, covering identities, values, schema, and failure behavior. Both leagues have live validation results.
- The selected implementation improves whole-extraction wall time beyond the observed variability on comparable work. Report absolute time saved, speedup, spread, and failure/timeout rate without claiming guarantees from a small sample.
- The new offline tests and affected extraction integration tests pass, and the direct manual benchmark passes for the chosen configuration. Raw profile artifacts remain available at the paths recorded in the results report.

Set the numerical acceptance target in the Phase 1 results report using the baseline spread and measured removable work, before evaluating the candidate. Report limitations if the data are insufficient to select a worker count. This keeps the target tied to measured work rather than a guessed five-minute threshold.

## Files expected to change

- `tests/integration/test_statsapi_extraction_performance.py` — manual diagnostic/benchmark cases, recording helpers, and bounded live-run supervision.
- `tests/unit/test_statsapi_extraction_script.py` and small fixtures — behavior characterization and coverage for selected changes; focused offline coverage for the recorder and failure artifacts where appropriate.
- `tests/integration/test_statsapi_extraction_script.py` — update `TeamStats` construction and roster setup calls if the roster refactor is selected.
- `mlb_airflow_data_pipeline/statsapi_extraction_script.py` — only the optimizations supported by measurements, including any required caller updates.
- `statsapi-extraction-performance-results.md` — measured baseline, target, per-change comparisons, outcomes, and selected worker count or reason to stay serial.
