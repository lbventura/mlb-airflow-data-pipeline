# StatsAPI extraction: Phase 1 performance results

Related: [issue #61](https://github.com/lbventura/mlb-airflow-data-pipeline/issues/61) and [implementation plan](statsapi-extraction-runtime-implementation-plan.md).

**Phase 1 is complete; the correctness-qualified baseline is inconclusive.** One diagnostic and all three declared unprofiled attempts completed extraction, but every attempt failed identity validation. Unprofiled elapsed time had a descriptive median of **974.738 seconds (16m 14.7s)**. Repeated name lookups consumed **551.796–555.861 seconds** per attempt and are the first candidate to remove. No optimization has been implemented.

## Scope and provenance

Phase 1 measures the existing serial extraction directly, including its database writes. It does not run Airflow or implement the roster, standings, or concurrency optimizations.

The measurements were made on branch `observability/extraction-phase1`, originally based on `testing/fix-manual-dag-test` at `29124f1d93ee3f127ed549d66fdbb55195f47a08`. The measured extraction and database modules included the original workspace's existing, uncommitted league-column persistence changes. Those changes were copied as baseline inputs, not introduced as performance improvements. Each attempt retains the measured files under `source/`, their SHA-256 digests, the Git revision/status, and dependency versions in `summary.json`.

The source branch subsequently advanced and was merged through PR #126. The profiling branch was then rebased onto merged `main` at `4736955`, selecting the complete `main` versions of the two conflicted production files. The saved source snapshots preserve the exact earlier experiment. The measured source and dependency fingerprints were rechecked before resuming on September 25 and still matched at that time.

## Measurement method

- Two manual tests share a direct runner: a diagnostic with `cProfile`, and a benchmark with profiling disabled. Both are parameterized for American and National leagues; this initial batch measures the American League.
- Each attempt uses one worker and a fresh SQLite database. The runner loads standings/rosters once, writes standings, creates the team mapping, extracts players, prepares the league column, writes players, and closes the database.
- Extraction wall and process CPU timers exclude imports, validation, and final artifact serialization. Request timing includes the unchanged client's HTTP call and JSON decoding. Lightweight instrumentation remains enabled in benchmarks; its overhead and checkpoint costs must be considered when interpreting timings.
- The parent supervises one child process with a 1,800-second deadline, followed by at most five seconds for termination before a forced kill. Each attempt has its own output directory, request journal, checkpoints, console log, database, output snapshots, and metadata. Diagnostic attempts additionally retain `extraction.prof` and `pstats.txt`.
- All attempted runs are retained, including failures. A completed extraction is not accepted as a successful baseline if validation finds incomplete or incorrect coverage.

The initial experiment is one diagnostic followed by three sequential, unprofiled benchmark attempts. Raw artifacts are retained locally under the Git-ignored `logs/extraction-profiles/` directory; they are not committed.

## Inputs and limitations

Rosters and name lookups explicitly request season 2023. `player_stats()` omits the season and requests season statistics using the client's default behavior. Consequently, the roster season does not freeze returned statistics. The baseline preserves this behavior; any correction needs a separate correctness decision and comparable measurements.

The installed client performs a `sports_players` request for every name lookup and filters the returned list locally. The extractor selects the first match. Formatted roster strings end with a newline, and the extractor processes the resulting blank name as well. The final data can also contain the same player in multiple teams. Coverage comparisons therefore need player IDs, team memberships, and multiplicity, not just row counts.

SQLite output omits the player-ID index. Pickle snapshots retain the original index and dtypes, with CSV copies for inspection. Database comparisons must account for SQLite's representation of types.

## Run results

| Attempt | Outcome | Extraction wall | Process CPU | Output |
| --- | --- | ---: | ---: | --- |
| `american_league-profile-workers1-20260924T190425Z-48fbc657` | Extraction completed; validation failed | 975.066 s (16m 15.1s) | 43.517 s | 796 rows, 758 unique IDs, 15 teams |
| `american_league-benchmark-workers1-20260924T192543Z-f09438d8` (B1) | Extraction completed; validation failed | 973.308 s | 39.161 s | 796 rows, 758 unique IDs, 15 teams |
| `american_league-benchmark-workers1-20260924T194157Z-9d1fce02` (B2) | Worker completed; validation failed; supervisor metadata unavailable | 975.070 s | 43.504 s | 796 rows, 758 unique IDs, 15 teams |
| `american_league-benchmark-workers1-20260925T105612Z-3f0408da` (B3) | Extraction completed; validation failed | 974.738 s | 42.228 s | 796 rows, 758 unique IDs, 15 teams |

The diagnostic test exited nonzero after 976.15 seconds including pytest/worker overhead. All 1,640 observed `statsapi.get` calls started and completed without recorded API exceptions. There were no failed teams or inactive-player records. The attempt still failed correctly because its output substituted six roster player IDs. Its time is a completed-extraction diagnostic duration, **not a successful benchmark**.

For the three unprofiled attempts, completed-extraction wall time had a median of **974.738 seconds**, range **973.308–975.070 seconds**, and spread **1.762 seconds**. This is descriptive timing of the existing workload, not a valid successful-run latency baseline: **0/3 passed validation**, all three reproduced the identity errors, and output values were not equivalent. Successful-run median/range and candidate speedup are therefore unavailable. All 4,920 unprofiled wrapper calls completed without recorded API exceptions; no worker reported a timeout. B2's supervisor exit status remains unknown. These small samples do not establish a general failure rate.

### Diagnostic stage and endpoint timings

| Stage | Seconds |
| --- | ---: |
| Standings and rosters | 8.760 |
| Standings database write | 0.008 |
| Team mapping | 0.004 |
| Player extraction | 966.254 |
| Player persistence preparation and write | 0.013 |

Team extraction durations ranged from 49.467 to 79.385 seconds. The player stage accounts for 99.1% of wall time; database work is negligible in this run.

| Endpoint | Calls | Total seconds | Median seconds | p95 seconds | Maximum seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| `standings` | 3 | 1.401 | 0.465 | 0.496 | 0.496 |
| `team_roster` | 15 | 7.333 | 0.485 | 0.523 | 0.523 |
| `sports_players` | 811 | 552.677 | 0.679 | 0.714 | 1.211 |
| `person` | 811 | 404.406 | 0.498 | 0.536 | 0.704 |

Percentiles use the nearest-rank method; the standings sample is too small for a meaningful tail estimate. The 796 roster entries produced 811 input names because each of 15 formatted rosters added a blank final line. The observed call counts agree with one lookup and one stats call per input name, including the blank entries later excluded from output.

Request intervals totaled 965.817 seconds, approximately 99.1% of serial wall time. Name-lookup requests alone accounted for 56.7%. These are inclusive elapsed intervals, not CPU measurements; stage, team, request, and profile durations are nested and must not be summed together.

The diagnostic profile corroborates the request measurements:

- `lookup_player`: 811 calls, 560.868 seconds cumulative, 5.524 seconds self time.
- `player_stats`: 811 calls, 404.656 seconds cumulative.
- SSL reads, TLS handshakes, and socket connects: approximately 391.328, 363.789, and 177.510 seconds self time, respectively. Those blocking calls dominate the profile.
- Process CPU was only 4.46% of elapsed wall time, consistent with network waiting dominating local computation.

The recorder reported 0.360 seconds of API-event bookkeeping outside timed request intervals. That is not the complete instrumentation cost: profiling, checkpoint serialization, and other observer work also affect wall time. This run therefore cannot quantify the speedup of an unprofiled candidate.

### Unprofiled measurements

| Measurement | B1 | B2 | B3 |
| --- | ---: | ---: | ---: |
| Whole extraction (s) | 973.308 | 975.070 | 974.738 |
| Player extraction (s) | 964.791 | 966.424 | 966.214 |
| `sports_players` total (s) | 552.737 | 555.861 | 551.796 |
| `person` total (s) | 407.812 | 406.494 | 409.912 |
| API-event bookkeeping (s) | 0.368 | 0.344 | 0.392 |

Every benchmark made 3 standings, 15 roster, 811 name-lookup, and 811 player-stat calls. All completed every request without recorded API exceptions, persisted all rows, and reproduced the same six incorrect player IDs. These are useful observations of the existing workload, not correctness-qualified benchmark successes. Detailed endpoint medians, p95, maxima, parameter sets, stage/team timings, and errors remain in each run's summary.

### Correctness findings

The output has the expected 84 extraction columns, with actual order/dtypes preserved in the snapshot. Reopening SQLite confirmed all 796 stored rows, league values, other persisted values, and row multiplicity match the extracted CSV after normalizing SQL nulls to CSV empty fields. There are no duplicate `(team_id, player_id)` pairs. The 796 roster entries contain 760 distinct IDs, whereas the output contains only 758. Roster identity reconciliation found these substitutions:

| Team | Roster name | Expected ID | Returned ID |
| --- | --- | ---: | ---: |
| Houston Astros (117) | Luis Garcia | 677651 | 671277 |
| Texas Rangers (140) | Will Smith | 519293 | 669257 |
| Seattle Mariners (136) | Diego Castillo | 650895 | 660636 |
| Seattle Mariners (136) | Tommy La Stella | 600303 | 592206 |
| Seattle Mariners (136) | José Rodríguez | 642578 | 679563 |
| Chicago White Sox (145) | Carlos Pérez | 656024 | 542208 |

The Tommy La Stella output name was reduced to `La Stella`. The others retained the displayed roster name while returning the wrong ID. These observations are consistent with the inspected implementation: it keeps the last two name words, performs a broad text lookup, and selects the first result. Eight passing tests in `tests/unit/test_statsapi_extraction_identity.py` characterize ambiguous-name selection, loss of the multipart name's distinguishing first name, the blank-line lookup, and the existing `TypeError` inactive classification. The responses are synthetic fixtures, not a replay of uncaptured live `sports_players` payloads, so they establish the failure mechanisms without claiming to reconstruct every live match. Matching row counts and zero failed teams would have concealed these errors.

Output equivalence also fails beyond those identities. After alignment by `(team_id, player_id)` and column name, the following differences remain relative to B1:

| Run | Stat cells changed | Rows with changed stats | Stat columns changed | Column order matches B1 |
| --- | ---: | ---: | ---: | --- |
| Diagnostic | 1,021 | 125 | 63 | No |
| B2 | 904 | 112 | 54 | No |
| B3 | 2,848 | 178 | 72 | Yes |

These counts exclude `date`: all 796 date values also changed in B3 because it ran the next day. Membership multiplicity, column-name sets, and dtypes match across all four runs. Date normalization alone does not make their values equivalent.

A ninth characterization test confirms `_generate_player_stats()` overwrites repeated field names with the last value encountered. Therefore response group ordering can change flattened values such as `gamesPlayed`; live statistics can also change because no stats season is supplied. The retained data cannot distinguish every cause, so these differences remain reported rather than normalized away. Field-level counts are retained in [the snapshot comparison](logs/extraction-comparison.json), generated by `logs/compare_extraction.py`.

### Artifacts and reproducibility

The complete diagnostic artifacts are in [the attempt directory](logs/extraction-profiles/american_league-profile-workers1-20260924T190425Z-48fbc657/):

- [Summary and metadata](logs/extraction-profiles/american_league-profile-workers1-20260924T190425Z-48fbc657/summary.json)
- [Readable profile](logs/extraction-profiles/american_league-profile-workers1-20260924T190425Z-48fbc657/pstats.txt) and `extraction.prof`
- `events.jsonl`, `console.log`, `rosters.json`, `extraction.db`, and CSV/pickle standings and player snapshots
- [Post-run artifact audit](logs/extraction-profiles/american_league-profile-workers1-20260924T190425Z-48fbc657/analysis.json), produced by local `logs/analyze_extraction.py` using standard-library readers without extra API calls

Benchmark summaries and their adjacent artifacts are retained at:

- [B1 summary](logs/extraction-profiles/american_league-benchmark-workers1-20260924T192543Z-f09438d8/summary.json)
- [B2 summary](logs/extraction-profiles/american_league-benchmark-workers1-20260924T194157Z-9d1fce02/summary.json)
- [B3 summary](logs/extraction-profiles/american_league-benchmark-workers1-20260925T105612Z-3f0408da/summary.json)

All six saved source files in every attempt match their recorded hashes. All four attempts used identical measured source digests, and the worktree still matched those digests at the audit immediately after B3. Later harness changes and the rebase changed the current source: the conflicted production files now match `main`. The Git SHA alone is insufficient to reproduce the measured version because baseline persistence changes and the harness were uncommitted at measurement time; retain the per-attempt `source/` files.

The measured environment was Python 3.14.7, MLB-StatsAPI 1.9.0, requests 2.34.2, pandas 3.0.6, and pytest 9.1.1, on macOS 27.0 ARM64. The installed StatsAPI source SHA-256 was `c13cdad5a72e58e5174bfd1fd5b076dbeef9fdf3cf8b7926365c0f914a544f16`. The `person` hydration was `stats(group=[hitting,pitching,fielding],type=season,sportId=None),currentTeam`, without a season argument. Execution dates were September 24 for the diagnostic/B1/B2 and September 25 for B3.

### Harness changes after measurement

The four live attempts used the original frozen harness. After B3 completed, the audit added explicit extraction/validation status, imported module paths, stage/team request context, and separate started/completed/interrupted request counts. It also strengthened roster checks to retain multiplicity, compared all persisted values, and preserved primary errors when journal/checkpoint/artifact writing also fails. Launch failures now receive a terminal summary, and missing durations remain `null`.

The final harness constructs `DataExtractor` inside the timed database block, matching the script's order. In the measured harness, its empty-object construction and wrapper setup occurred before timing. After the rebase, the harness also calls `main`'s `ensure_dataframe_columns()` before both database writes. No profiling change alters extraction requests, transport policy, or retries. The updated paths were verified with offline fixtures and supervised child processes, not another live batch. The reported timings belong specifically to the saved original source; future comparisons on merged `main` need a new baseline and must run the same harness for both baseline and candidate.

## First candidate and acceptance target

The first candidate is the small roster helper carrying authoritative player IDs directly into `TeamStats`. It would remove the 811 repeated `sports_players` requests observed here and address the six incorrect identities. Treat changed IDs and restored full names as explicit correctness changes, with fixtures and an agreed reference; do not require preservation of known wrong identities or silently ignore mismatches.

The unprofiled lookup-request cost was 551.796–555.861 seconds, with median 552.737 seconds. Subtracting each attempt's lookup-request interval from its own wall time leaves 419.210–422.941 seconds (about seven minutes), before accounting for eliminated local lookup work, blank-entry stats requests, replacement helper work, or changed player identities. This is an estimate of removable work under the observed conditions, **not a promised runtime or measured speedup**. Inclusive `lookup_player` profile time must not be added to that request cost because it contains those same requests.

The two redundant standings requests represent only about 0.94 seconds in this diagnostic (total less the first request). Measure that independent change separately. Database tuning is not supported by these measurements. Defer concurrency until request removal and correctness have been evaluated; one worker remains the reference configuration.

No numerical acceptance target or worker count is selected: all benchmark attempts fail identity validation, stat values differ, and the batch had an overnight interruption. The observed 1.762-second spread is not sufficient evidence of controlled variability or correctness. The fixed-response tests establish the current name-resolution and flattening behavior; a future correctness reference must explicitly resolve those findings before setting a speed target. A candidate must improve whole-extraction wall time on comparable, correct work. Three repetitions alone cannot establish reliability guarantees.

## Reproduction

From this worktree, use the project's Python environment. If the named micromamba environment is not found under the current root prefix, activate its actual prefix first. Verify that imports resolve to this worktree.

```sh
export STATSAPI_PROFILE_OUTPUT_DIR=logs/extraction-profiles
python -m pytest -o addopts='' -m manual -s 'tests/integration/test_statsapi_extraction_performance.py::test_extraction_profile[american_league]'
python -m pytest -o addopts='' -m manual -s 'tests/integration/test_statsapi_extraction_performance.py::test_extraction_benchmark[american_league]'
```

The benchmark command executes one attempt; the declared batch repeats it three times sequentially, retaining nonzero exits. No automatic retries are added to the API client.

For exact reproduction of the recorded measurements, use an isolated checkout with the source files saved under an attempt's `source/` directory. The commands above use the final, strengthened harness and merged `main` code when run from the current worktree.

## Verification

Final verification passed **30 offline tests**: 18 harness tests, nine identity/flattening characterization tests, and three existing extraction unit tests. Both manual modes collect for both leagues (four cases). Configured pre-commit checks pass for all four new Python files, including mypy, Ruff, whitespace, and formatting. The live attempts fail for the identity differences documented above; those failures are retained rather than waived.

```sh
python -m pytest tests/unit/test_extraction_profile.py tests/unit/test_statsapi_extraction_identity.py tests/unit/test_statsapi_extraction_script.py
python -m pytest -o addopts='' --collect-only -q tests/integration/test_statsapi_extraction_performance.py
pre-commit run --files tests/integration/extraction_profile.py tests/integration/test_statsapi_extraction_performance.py tests/unit/test_extraction_profile.py tests/unit/test_statsapi_extraction_identity.py
```

The post-run audit verified request completion counts, source snapshot hashes, source stability, roster membership multiplicity, and persisted values. `git diff --check` passed for tracked changes, and the artifact directory is ignored by Git. No production optimization, full-DAG test, merge, push, or PR was performed for this phase.

### Phase 1 completion audit

| Requirement | Evidence and disposition |
| --- | --- |
| Separate worktree and branch from the requested source | `observability/extraction-phase1`, initial base `29124f1`; source snapshots retained |
| Direct diagnostic and benchmark harness, both leagues selectable | Four collected manual cases; no Airflow execution |
| Wall/CPU/stage/team/API measurements and diagnostic profile | Four attempt directories, summaries, journals, snapshots, and diagnostic profile/report |
| One diagnostic and three fixed benchmark attempts; retain failures | All four worker outcomes retained, with interruption and supervisor limitations disclosed |
| Correctness, persistence, and source audit | Per-attempt `analysis.json`, snapshot comparison, source digests, 30 passing offline tests |
| Dominant cost, first candidate, estimate, and acceptance decision | Repeated lookups dominate; roster IDs recommended; roughly 552–556 seconds removable request time; valid speed target deferred for explicit correctness/comparability reasons |
| Stop before optimization | No roster refactor, standings reuse, worker pool, season correction, or transport change implemented |

The Phase 1 investigation is complete. Its baseline conclusion is **inconclusive for correctness-qualified performance**, as allowed by the plan when attempts fail. National League live validation belongs to validation of a selected solution in later phases; only its manual cases were collected here. No further live attempts or optimization work are part of this deliverable.
