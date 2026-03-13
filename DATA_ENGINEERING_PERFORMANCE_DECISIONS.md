# Data Engineering Performance Decisions For Football Tracking Data

## Purpose

This project is designed around a warehouse-first question:

How do we keep raw football tracking data close enough to source fidelity for tactical and video validation work, while still making repeated analytical scans cheap in object storage, DuckDB, Databricks, and Snowflake?

The answer in this repository is not "derive every football metric up front."
It is:

- keep the base grain very low
- keep files physically large enough for efficient reads
- add only the derived columns that improve pruning, filtering, debugging, or analyst ergonomics
- make thresholds config-driven when domain experts need to tune them from video

That leads to a table that is still a tracking fact table, not a tactical-feature mart.

## Workload Assumptions

The main workload assumptions in this project are:

- tracking data is written once and read many times
- most reads are selective, not full-table scans
- the first filter is usually match-level
- secondary filters are usually period, time window, team side, pitch zone, or ball zone
- analysts often need short replay windows and frame-level validation
- cloud and lakehouse reads are often bottlenecked by bytes transferred and object-store latency, not only raw CPU

Those assumptions drive almost every performance decision below.

## Base Grain Choice

The curated dataset keeps this grain:

```text
1 row = 1 player in 1 frame in 1 match period for 1 team
```

This is deliberately not aggregated into possessions, sequences, actions, or match summaries.

Technical reason:

- aggregation would destroy replay fidelity
- event attribution usually needs frame-level inspection
- tactical logic changes more often than storage logic
- base tables should preserve the lowest stable analytical grain

That choice makes the dataset large, but it keeps downstream modeling flexible.

## Why Match Metadata Is Used For Pitch Dimensions

`pitch_length` and `pitch_width` come from each match metadata file when present, with config defaults only as fallback.

Technical reason:

- zone logic depends on the real scanned pitch dimensions
- player-ball distance and spatial windows are more trustworthy when the pitch scale is match-specific
- using one global pitch size would bias spatial features when scanned matches vary slightly

This is especially important for football tracking because small dimensional differences can shift zone boundaries and downstream frame interpretation.

## Derived Columns Kept In The Base Table

The project intentionally keeps only a small family of derived helper columns in the base output.

### `frame_bucket`

`frame_bucket = frame_id // frame_bucket_size`

Why it stays:

- short replay windows usually target a narrow frame range
- frame windows are a very common predicate
- this column materially improves time-local physical ordering

Why the default is `500`:

- at 25 fps, that is about 20 seconds
- it is fine enough for replay and sequence pruning
- it is coarse enough to avoid over-fragmenting layout

### `min_split`

`min_split = 5 * (floor(game_clock / 300) + 1)`

Why it stays:

- analysts often ask for "first 5 minutes", "minutes 10-15", or "added time"
- raw `game_clock` has very high cardinality
- a coarse time label makes group-bys and BI-style slicing cheaper and easier

Why it is not the primary time layout key:

- it is much coarser than `frame_bucket`
- it is better for analyst ergonomics than for fine-grained pruning

### `pitch_zone` and `ball_zone`

Why they stay:

- they turn continuous x/y coordinates into low-cardinality spatial filters
- common tactical questions are zone-based
- they are strong clustering candidates because they prune meaningfully without exploding state space

They are intentionally lightweight storage helpers, not a full tactical model.

### `player_ball_distance`

Why it stays:

- many questions start with ball proximity
- warehouses should not have to recompute Euclidean distance for every query
- it supports video validation, possession heuristics, pressure heuristics, and event-pattern detection

This is one of the most valuable helper columns in the table because it bridges raw tracking and football semantics without committing to one tactical interpretation.

### `has_ball_possession`

Why it stays:

- analysts often need a confident possession flag quickly
- deriving it on every query is repetitive
- the project uses a sustained-frame rule rather than a one-frame proximity rule

Why the sustained rule matters:

- a single close frame can mean pressure or defensive close-down
- requiring a run of close frames reduces false positives

This is a good example of a domain-aware derived column that is still storage-friendly.

### `player_speed_band`

Why it stays:

- analysts often query movement regimes, not exact floating-point speed
- a low-cardinality activity band is easier to group and filter than raw speed
- it supports later tactical marts and can help optional secondary clustering

Why it is config-driven:

- clubs and analysts use different movement thresholds
- the source speed field is metric, but operational definitions vary

## Event Pattern Detection In The Base Table

The project now adds a dynamic `event_type` helper column driven by `settings.patterns` in `config.yaml`.

This is intentionally not a full event-modeling engine.
It is a configurable frame-pattern detector for touch-and-release events such as passes and shots.

### Why It Belongs In The Curated Layer

- event review is a repeated task
- the same touch-distance and release-speed checks would otherwise be recomputed many times
- storing a lightweight candidate event label makes video validation and downstream marts faster
- keeping thresholds in YAML lets the football logic be tuned without code edits

### Detection Shape

The detector uses three ideas:

1. A start-band candidate:
   the player must reach a configurable player-ball distance band such as `0.80-0.85m`, and the final frame in that band before release is treated as the event frame.
2. A short future sequence:
   the next `frame_sequence` frames are scanned to confirm the ball has left the player and reached the configured speed.
3. An optional persistence window:
   once detected, the label can be kept for a small number of frames so replay review shows the event as a short sequence instead of a single instant.

This design exists for a technical reason:

- if detection only uses one frame, event labels become noisy
- if detection waits only for the release frame, the event appears too late for replay review
- using the last close-contact frame before release catches the touch point more accurately than labeling the later release frame itself

### Why Event Priority Is Speed-Led

Multiple event rules can overlap.
For example, a shot will often also satisfy pass-like release thresholds.

This project resolves overlaps by prioritizing higher-speed, higher-release-distance rules first.

Technical reason:

- more forceful events are typically more specific
- this avoids a fast shot being downgraded to a pass when both rules match

## Why The Project Prefers Clustering/Sorting Over Legacy Partitioning

For this data grain, the project prefers large files plus physical ordering over aggressive directory partitioning.

### Why not partition heavily

If you partition by match, period, team, frame bucket, and zones, you quickly create many tiny partition groups.

That causes:

- too many small files
- more metadata overhead
- slower planning
- lower scan throughput
- worse object-store behavior

Football tracking data is high volume row-wise, but individual match files are still not large enough to justify slicing them into many directory partitions.

### Why physical ordering works better here

Large Parquet files with good row-group statistics let engines skip data based on:

- file-level stats
- row-group min/max stats
- clustered locality inside the file

That gives most of the pruning benefit without the partition explosion.

## Why These Columns Are Strong Layout Keys

The current clustering/sort emphasis is:

- `opta_match_id`
- `period`
- `team`
- `frame_bucket`
- `pitch_zone`
- `ball_zone`

### `opta_match_id`

- most selective common filter
- keeps each match physically local
- improves file and row-group pruning immediately

### `period`

- tiny cardinality
- common secondary predicate
- preserves natural match chronology

### `team`

- tiny cardinality
- common tactical split
- improves locality for side-specific queries

### `frame_bucket`

- strongest fine-grained time-locality helper
- supports replay windows and short-sequence scans better than coarse minute buckets

### `pitch_zone` and `ball_zone`

- low-cardinality spatial filters
- strong tactical relevance
- useful for zone-based pruning without huge fragmentation

## Why Some Other Columns Are Not Primary Layout Keys

### `min_split`

Useful for analysis, but too coarse and too correlated with `frame_bucket` to be a better primary physical ordering key.

### `opta_player_id`

Useful analytically, but too high-cardinality as a leading layout key.
If used too early in the sort order, it harms time locality.

### `player_speed_band`

Useful as an optional late clustering key, but not as a leading one.
Movement bands are less important than match, time, and space for most scan patterns.

### `live`, `last_touch`, `player_number`

These are useful context columns, but weak physical layout keys because their pruning power is limited.

## File Size And Compaction Strategy

The project compacts outputs toward larger Parquet files instead of leaving one tiny processed file per source slice.

Why:

- larger files amortize object-store overhead better
- engines read large sequential byte ranges more efficiently
- scan planning is faster with fewer files

Compaction is especially valuable for football tracking because the source grain creates many rows per match, but operational pipelines often ingest matches incrementally.

## Why Row Groups Matter

Row groups are a second-level pruning surface inside each Parquet file.

Why a deliberate row-group size matters:

- smaller row groups improve pruning precision
- larger row groups reduce metadata overhead
- the right balance depends on repeated predicate patterns

This project uses row groups as part of the main performance strategy, alongside clustering and compaction.
That is why keeping physically ordered data in larger files is so important.

## Why `zstd` Is The Default Compression

`lz4` is often faster to compress.
That is true, but this repository defaults to `zstd` because the primary workload is read-heavy analytical access, not maximum local write throughput.

Why `zstd` is favored here:

- smaller Parquet files
- fewer bytes transferred from disk or object storage
- lower repeated scan cost
- better fit for write-once, read-many workloads

When `lz4` would be better:

- rapid local rewrite loops
- SSD-heavy local workflows
- situations where codec CPU time dominates and file size matters less

So the choice is not "zstd is always better."
It is "zstd is usually the better default for this project’s read pattern."

## Why Thresholds Live In `config.yaml`

The project pushes football-semantic thresholds into YAML whenever the thresholds are likely to be tuned from replay review.

That now includes:

- possession distance and minimum frames
- speed-band cutoffs
- event-pattern touch, leaving distance, speed, and frame sequence

Technical reason:

- the storage code remains stable
- analysts can tune semantics without touching Python
- experiments become reproducible because threshold choices are explicit in config

This is especially important for football tracking because "correct" thresholds often depend on provider behavior, frame rate, smoothing, and club-specific definitions.

## Why The Replay Tool Is Part Of The Performance Story

The HTML replay is not just a visualization convenience.
It is part of the validation loop for derived columns.

That matters because:

- possession flags need visual validation
- event detection thresholds need visual validation
- false positives are easier to catch in replay than in raw tables

Adding lightweight replay-aware derived fields reduces the time between a storage decision and a football-sense validation check.

## Tradeoffs Accepted In This Project

The project intentionally accepts these tradeoffs:

- base tables are wider than pure raw tracking tables
- some football semantics are baked in as configurable helpers
- the base table is still large because frame-level fidelity is preserved

In exchange, the project gets:

- cheaper repeated scans
- better replay/debug workflows
- less repeated SQL for common proximity and time-window questions
- easier downstream marts for tactical analysis

## Practical Tuning Guidance

If reads are slow:

- validate sort order and compaction before adding more partitions
- check whether file counts are growing too quickly
- only add new derived helpers when they save repeated downstream work

If event detection is noisy:

- lower `closest_frame` only if fallback touch frames are too loose when no event-specific start band is configured
- tune `start_distance_min` and `start_distance_max` when you want the event to begin in a tighter contact window
- raise `leaving_distance` if the ball is being tagged too early
- raise `speed` if soft touches are being misread as passes or shots
- increase `frame_sequence` only enough to confirm release, not so much that the start window becomes ambiguous

If analysts mostly work on time buckets:

- keep `min_split` for ergonomics
- still keep `frame_bucket` for layout unless scan windows are overwhelmingly coarse

## Summary

The performance model in this project is built on one principle:

Preserve the raw analytical grain, but aggressively optimize physical layout and a very small set of domain-aware helper columns.

For football tracking data, that is the best compromise between:

- source fidelity
- replay validation
- warehouse scan cost
- analyst usability
