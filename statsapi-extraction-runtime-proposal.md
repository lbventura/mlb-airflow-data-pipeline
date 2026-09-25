# Proposal: improve `statsapi_extraction_script` runtime

Related issue: [#61 — `statsapi_extraction_script` performance](https://github.com/lbventura/mlb-airflow-data-pipeline/issues/61)

**Feedback 1**: This is a good first step but we want to measure the current performance before implementing any changes in order to understand where improvements can be made and by how much. See also **Feedback 3**.

## Findings

The extraction spends most of its time waiting on sequential StatsAPI requests:

- `DataExtractor.get_player_stats_per_league()` processes the 15 teams in a league one at a time.
- For every roster name, `TeamStats` calls `statsapi.lookup_player()` and then `statsapi.player_stats()`, also one player at a time. In the pinned MLB-StatsAPI 1.9.0 source, `lookup_player()` fetches the season's `sports_players` response and filters it locally. Repeating this for each player transfers and scans the same player list many times.
- `statsapi.roster()` already calls the `team_roster` endpoint, but its formatted string omits each roster entry's player ID. The extraction then looks the IDs up again by name.
- `set_league_division_standings()` calls `standings_data()` once for each of a league's three divisions. Its default `division="all"` returns all divisions, so the same standings request and parsing are repeated three times.

The wrapper uses synchronous `requests.get()` calls. A thread pool fits these I/O-bound requests; `asyncio` would require replacing or wrapping the synchronous client. See the [MLB-StatsAPI 1.9.0 source](https://github.com/toddrob99/MLB-StatsAPI/blob/v1.9.0/statsapi/__init__.py) and the project's [`statsapi_extraction_script.py`](mlb_airflow_data_pipeline/statsapi_extraction_script.py).

**Feedback 2**: Use threads rather than asyncio.

The regular cases in [`test_airflow.py`](tests/integration/test_airflow.py) check Airflow setup and the first league-selection task. The full-DAG case that reaches extraction is marked `manual`, and the test has no extraction-stage timings. Running that file would not identify the slow calls without invoking the manual full-DAG run.

**Feedback 3**: It is not required to run the full DAG in order to detect where `statsapi_extraction_script` is slow. Write tests and use the standard Python `cProfile` benchmarking library if necessary.

## Recommended changes

1. **Carry player IDs from the roster response.** Read the raw `team_roster` response (or add a small helper that returns structured roster entries) and use each entry's `person.id` and `person.fullName` directly. Confirm those fields in a representative response and retain the current name, inactive-player, and failed-team behavior. This removes one repeated name-lookup request per rostered player and avoids repeatedly downloading the season player list. If the roster response for the configured season lacks IDs, fetch `sports_players` once and build an ID lookup once rather than calling `lookup_player()` for every name.

**Feedback**: Add a small helper function, in accordance to the coding principles in @AGENTS.md

2. **Reuse the standings response.** Call `statsapi.standings_data()` once per league, then select the configured division IDs from that result. This cuts three identical standings requests to one.

3. **Add bounded team concurrency.** Use `concurrent.futures.ThreadPoolExecutor` in `get_player_stats_per_league()` to run independent team extractions concurrently. Start with `max_workers=4`; benchmark 2, 4, and 8 workers before increasing the cap. Each worker should own its `TeamStats` instance and return its result. Consume results in the main thread, assemble the DataFrame in the original team order, and keep the existing behavior where a non-`TypeError` failure marks that team failed while other teams continue. The worker cap limits simultaneous StatsAPI requests and avoids an unbounded burst.

4. **Measure before tuning further.** Record elapsed seconds and request counts for standings, roster loading, player lookups (until removed), player-stat retrieval, and database writes. Keep per-player detail out of routine logs; per-team durations and aggregate request counts are enough to find slow stages without producing excessive logs. Compare runs using the same league, season, and environment.

If bounded team concurrency does not meet the runtime target, use the timings to decide whether to parallelize player-stat requests through one shared bounded pool. Avoid nested pools, which can multiply the request concurrency unexpectedly.

## Safeguards and acceptance

- Begin with a small worker cap and increase it only if response times improve without an increase in API errors or failed teams. The MLB-StatsAPI client raises on non-success HTTP responses; do not add broad retries that could hide failures or amplify request volume.
- Keep DataFrame construction and shared result dictionaries in the main thread. Preserve the expected output columns, player IDs/names, inactive-player records, and per-team failure reporting.
- Add unit coverage for the concurrency cap, deterministic aggregation, the inactive-player path, and one team's failure not cancelling other teams. Use mocked StatsAPI calls for repeatable runtime tests; use an explicitly measured full extraction for the end-to-end comparison.
- Establish a baseline and then use a provisional goal of completing the same full-league extraction in under five minutes, with matching player coverage and no increase in failed teams. Adjust the target if the issue's reported runtime and the measured test run cover different extraction scopes.

## References

- [Issue #61](https://github.com/lbventura/mlb-airflow-data-pipeline/issues/61)
- [MLB-StatsAPI 1.9.0 source](https://github.com/toddrob99/MLB-StatsAPI/blob/v1.9.0/statsapi/__init__.py)
- [`statsapi_extraction_script.py`](mlb_airflow_data_pipeline/statsapi_extraction_script.py)
- [`test_airflow.py`](tests/integration/test_airflow.py)
