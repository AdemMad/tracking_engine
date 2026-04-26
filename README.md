# tracking_engine

`tracking_engine` is a warehouse-first football tracking pipeline.
It converts raw Second Spectrum JSONL files into compact Parquet designed for fast ingestion and selective reads in Snowflake and Databricks.

The project computes football-analysis features such as:

- `is_high_intensity`
- `is_sprint`
- `is_pressing`
- `player_distance`
- `time_delta`
- `live_time_delta`

`pitch_zone` and `ball_zone` are intentionally kept because they are used as storage-layout helpers for clustering and pruning. `player_ball_distance` is also kept as a lightweight proximity filter helper so warehouses can answer questions like "players within 5m of the ball" without recomputing distance at query time. This version keeps the pipeline focused on data engineering: preserve raw tracking fidelity, derive only the columns that materially help warehouse layout or common warehouse filtering, and make downstream warehouse scans cheaper.

## Install

```bash
pip install -e .
```

## Quickstart

```python
from tracking_engine import TrackingPipeline

df = TrackingPipeline(
    storage="local",          # local, aws_s3, azure_blob, adls
).run("2562182")
```

The `model`, `save`, `output_name`, possession threshold, speed-band thresholds, event-pattern thresholds, and zone-column defaults can now all live in `config.yaml`, so the simplest usage is:

```python
from tracking_engine import TrackingPipeline

summary_df = TrackingPipeline(
    storage="aws_s3",
).run()
```

Process a selected list of matches instead of a whole directory:

```python
from tracking_engine import TrackingPipeline

df = TrackingPipeline(
    storage="local",
).run(["2562179", "2562180"])
```

When `run(save=True, output_name=...)` is used with no filename, the engine returns a compacted-output summary table rather than loading the entire multi-match dataset back into memory. Match identifiers can be passed as raw Opta IDs or filenames such as `2562179.jsonl` or `g2562179.parquet`.

If an existing source `.parquet` file is invalid or incomplete but the original `.jsonl` or `.ndjson` still exists, the engine will stream the JSON source directly instead of trying to rebuild a raw source Parquet first. This is intentionally safer for peak memory usage.

The active storage profile is read from `config.yaml`, which now defines:

- `read_from`
- `export_to`
- `metadata_from`
- `storage_options`

`config.yaml` can also define runtime defaults such as:

- `model`
- `save`
- `output_name`
- `ball_possession.distance_m`
- `ball_possession.min_frames`
- `speed_bands.*`
- `patterns.closest_frame`
- `patterns.<event_name>.*`
- `ball_possession.column_name`
- `player_ball_distance.column_name`
- `patterns.column_name`
- `zones.pitch_zone_column`
- `zones.ball_zone_column`
- zone geometry values such as `penalty_box_depth_m`

## Memory safety

This package is designed to avoid the two memory spikes that are most common with large nested tracking feeds:

- building a raw source `.parquet` cache from a very large `.jsonl`
- collecting many processed matches into one in-memory DataFrame before compaction

The current engine avoids both problems deliberately:

- it prefers an existing valid source `.parquet` when one already exists
- otherwise it streams `.jsonl` or `.ndjson` directly into the processed-match pipeline
- it writes one processed match at a time in multi-match `save=True` mode
- it compacts saved processed files afterward instead of concatenating all matches eagerly in memory
- it sorts once during final compacted-batch writing instead of sorting every intermediate file

In practical terms, that means the safest operational path for a directory run is:

```python
from tracking_engine import TrackingPipeline

summary_df = TrackingPipeline(
    storage="local",
    model="denormalized",
).run(save=True, output_name="tracking_compacted")
```

That path is intentionally optimized for lower peak memory, predictable compaction, and better recovery if a prior run left a broken source file behind.

## Package structure

The package is intentionally small. Each module has one clear responsibility:

| File | Responsibility |
|---|---|
| `tracking_engine/__init__.py` | Public exports and package version |
| `tracking_engine/pipeline.py` | Main orchestration layer: discover files, process matches, compact outputs, print timings |
| `tracking_engine/storage.py` | Storage profile loading, local/cloud source discovery, metadata lookup, and export staging |
| `tracking_engine/metadata.py` | Match metadata parsing, per-match pitch dimensions, and denormalized player/match enrichment |
| `tracking_engine/layout.py` | Warehouse layout logic: dtype narrowing, `frame_bucket`, output column order, sort/clustering constants |
| `tracking_engine/io.py` | File I/O: source validation, low-memory source resolution, lazy scanning, Parquet writing |
| `tracking_engine/logs.py` | Performance-log writing and DuckDB query helpers for JSONL execution logs |
| `tracking_engine/zones.py` | Spatial helper logic: `pitch_zone`, `ball_zone`, away-direction flipping |
| `tracking_engine/config.py` | Runtime configuration objects, YAML-driven defaults, pitch fallbacks, and zone boundary definitions |
| `config.yaml` | Named storage profiles plus runtime defaults such as model, save/output name, possession threshold, speed bands, event-pattern rules, and zone settings |
| `DATA_ENGINEERING_PERFORMANCE_DECISIONS.md` | Detailed rationale for storage layout, pruning, compression, compaction, and event-pattern design choices |
| `notebooks/duckdb_spatial_analysis.ipynb` | DuckDB performance and spatial analysis queries over curated Parquet outputs |
| `tests/test_pipeline.py` | Schema and behavior regression tests |

## Processing architecture

The package follows a simple pipeline:

1. Load the selected storage profile from `config.yaml`.
2. Resolve one match file, a list of matches, or discover all match files in the configured source location.
3. Choose the lowest-memory readable source path: valid Parquet if it already exists, otherwise stream JSONL/NDJSON directly.
4. Load match metadata and use it to get the match-specific `pitchLength` and `pitchWidth`.
5. Project only the shared frame-level fields needed downstream.
6. Explode player arrays into one row per player per frame.
7. Enrich rows with pitch dimensions and, for `model="denormalized"`, add `fixture`, `match_date`, `player_name`, and `player_position`.
8. Add spatial helper columns: `pitch_zone` and `ball_zone`.
9. Add storage/helper columns: compact dtypes, `frame_bucket`, `min_split`, `player_ball_distance`, `has_ball_possession`, and config-driven `event_type`.
10. Sort final warehouse outputs by warehouse-friendly keys.
11. Write detailed task timings to a JSONL performance log.
12. Return the DataFrame or write per-match and compacted Parquet outputs to the configured export location.

At the data-model level, the package is deliberately split into:

- raw business columns from the source
- a very small set of derived storage helpers
- no tactical or physical-analysis feature family in the base table

## What the pipeline outputs

The output schema is intentionally lean and warehouse-oriented.

Positional and distance fields are interpreted as metric fields. `pitch_length` and `pitch_width` now come from the match metadata file when it exists, with `StorageConfig.length` and `StorageConfig.width` used only as a fallback.
For timing fields, `game_clock` is treated as seconds and `wall_clock` is kept as the raw source integer timestamp, which in sample data behaves like milliseconds.

### Column dictionary

| Column | Meaning | Unit / measure | Notes |
|---|---|---|---|
| `opta_match_id` | Match identifier | none | Natural warehouse match key |
| `team` | Team side | categorical | `home` or `away` |
| `period` | Match segment | ordinal number | Typically half/period number |
| `frame_id` | Raw frame index | frames | Sequential tracking frame identifier |
| `frame_bucket` | Coarser frame grouping | frames per bucket | Derived as `frame_id // frame_bucket_size` |
| `game_clock` | Elapsed match time | seconds | Native match clock value |
| `min_split` | Coarser 5-minute game-clock bucket | upper-bound minute label | Derived as `5 * (floor(game_clock / 300) + 1)`, so `5=first 5 min`, `10=5-10 min`, `90=85-90 min`, `100=95-100 min`, etc. |
| `wall_clock` | Raw source wall time | source-native integer, typically milliseconds | Kept as raw ingest field |
| `live` | In-play flag | boolean | `True` / `False` |
| `last_touch` | Side that last touched the ball | categorical | Source possession-side context |
| `opta_player_id` | Numeric player identifier | none | Source player key |
| `player_id` | Stable player identifier | none | Source string/UUID-style player key |
| `player_number` | Shirt number | jersey number | Roster/context field |
| `player_x` | Player x-coordinate | meters | Metric tracking coordinate |
| `player_y` | Player y-coordinate | meters | Metric tracking coordinate |
| `player_z` | Player z-coordinate | meters | Height/elevation from source |
| `player_speed` | Player speed | meters per second, assumed metric source | Raw speed kept from source |
| `player_speed_band` | Config-driven player speed activity band | categorical | Derived from `player_speed` using configurable thresholds in `settings.speed_bands` |
| `ball_x` | Ball x-coordinate | meters | Metric tracking coordinate |
| `ball_y` | Ball y-coordinate | meters | Metric tracking coordinate |
| `ball_z` | Ball z-coordinate | meters | Ball height/elevation from source |
| `ball_speed` | Ball speed | meters per second, assumed metric source | Raw ball speed kept from source |
| `pitch_length` | Match-specific pitch length | meters | Loaded from metadata file, fallback to `StorageConfig.length` |
| `pitch_width` | Match-specific pitch width | meters | Loaded from metadata file, fallback to `StorageConfig.width` |
| `player_ball_distance` | Distance from player to ball | meters | Derived as Euclidean distance in x/y space |
| `has_ball_possession` | Whether the player is confidently in possession | boolean | `True` only when `player_ball_distance <= ball_possession_distance_m` for at least `ball_possession_min_frames` consecutive frames |
| `event_type` | Config-driven touch-and-release event label | categorical | Assigned from `settings.patterns` to the last configured close-contact frame before a short future release sequence confirms the event, and optionally persisted for a few frames afterward |
| `pitch_zone` | Player spatial zone label | categorical zone | Derived from `player_x`, `player_y` |
| `ball_zone` | Ball spatial zone label | categorical zone | Derived from `ball_x`, `ball_y` |

When `model="denormalized"`, the pipeline also adds:

| Column | Meaning | Unit / measure | Notes |
|---|---|---|---|
| `fixture` | Human-readable fixture label | text | Parsed from metadata `description`, e.g. `FUL - WHU` |
| `match_date` | Match date | calendar date | Built from metadata year/month/day |
| `player_name` | Human-readable player name | text | Joined from metadata on `opta_player_id` |
| `player_position` | Player position label | categorical | Joined from metadata on `opta_player_id` |

Row granularity is:

```text
1 row = 1 player in 1 frame in 1 match period for 1 team
```

That means this pipeline does not aggregate football events into summaries. It preserves the fine-grained tracking grain needed for a warehouse fact table.

One derived temporal layout column is added:

- `frame_bucket = frame_id // frame_bucket_size`

One derived analyst-friendly time bucket is also added:

- `min_split = 5 * (floor(game_clock / 300) + 1)`

Two derived clustering columns are also intentionally kept:

- `pitch_zone`
- `ball_zone`

One lightweight derived filter helper is also kept:

- `player_ball_distance = sqrt((player_x - ball_x)^2 + (player_y - ball_y)^2)`

One configurable player speed helper is also kept:

- `player_speed_band` is derived from `player_speed` using the ordered thresholds in `settings.speed_bands`
- default thresholds are `standing<=0.2`, `walking<=2.0`, `jogging<=4.0`, `running<=5.5`, `high_speed_running<=7.0`, else `sprinting`
- these defaults are expressed in meters per second, which matches the stored `player_speed` field

One lightweight boolean possession helper is also kept:

- `has_ball_possession = True` only when `player_ball_distance <= ball_possession_distance_m` for at least `ball_possession_min_frames` consecutive frames
- default threshold: `1.75` meters
- default minimum run length: `5` consecutive frames

Default `frame_bucket_size` is `500`, which is about 20 seconds at 25 fps. That is small enough to prune frame windows efficiently and large enough to avoid over-fragmenting the data.

### Measure notes

- No fields in the base table are stored in kilometers.
- Distances are treated as meters.
- Speeds are treated as meters per second when the upstream source is metric.
- `wall_clock` is intentionally left as a raw source timestamp rather than normalized into a new derived time unit.

## Grouping and granularity

The word "grouping" can mean two different things, and they are not the same here:

### 1. We do not use `GROUP BY` aggregation

We are not grouping rows to calculate summaries such as averages, counts, or totals.
We keep the raw tracking grain:

```text
match -> period -> frame -> team -> player
```

This is important because a warehouse base table should stay at the lowest useful grain. Aggregations belong in downstream marts, not in the base tracking table.

### 2. We do physically group data for storage efficiency

What is physically grouped:

- multiple small Parquet files are compacted into larger files around `300 MB`
- when `run()` is called without a filename, the engine discovers every tracking file in the configured `read_from` location
- those files are processed one match at a time and first saved as per-match processed Parquet files
- the saved per-match files are then lazily concatenated into compacted batch outputs near the target size
- rows are physically ordered by `opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone, frame_id, player_id`
- Snowflake and Databricks use clustering metadata to keep similar rows close together

What is not grouped:

- player rows are not collapsed into team summaries
- frames are not collapsed into minute summaries
- matches are not collapsed into season summaries

So the project keeps fine business granularity, but improves physical storage locality.

## Performance logging

Every run now appends detailed task timings to:

```text
<export_to>/tracking_engine_performance.jsonl
```

Each JSONL record includes fields such as:

- `logged_at_utc`
- `run_id`
- `run_mode`
- `opta_match_id`
- `task_scope`
- `task`
- `duration_seconds`
- `duration_text`
- `rows`
- `status`
- `source_status`
- `file_path`
- `source_file_path`
- `output_file_path`
- `batch_index`
- `match_count`
- `match_ids_csv`
- `teams`
- `save`

This makes it possible to analyze:

- the slowest task within a single match
- whether source resolution or row extraction dominates runtime
- which batch compactions are expensive
- whether save time or transform time is the main bottleneck

In low-memory directory `save=True` mode, the task boundaries are intentionally a bit different:

- `resolve_tracking_source`
- `build_lazy_match_output`
- `save_lazy_match`
- `compact_batch`

That is because the engine keeps each match lazy until write time to avoid materializing a large processed match table in memory.

### DuckDB log queries

The package exposes a helper:

```python
from tracking_engine import TrackingPipeline

pipeline = TrackingPipeline(
    storage="local",
    model="denormalized",
)

log_df = pipeline.query_logs("""
    SELECT
        opta_match_id,
        task,
        duration_seconds,
        rows,
        file_path
    FROM performance_logs
    ORDER BY duration_seconds DESC
""")
```

Useful analysis queries:

```sql
SELECT
    opta_match_id,
    task,
    duration_seconds,
    rows,
    file_path
FROM performance_logs
WHERE opta_match_id = 2562179
ORDER BY duration_seconds DESC;
```

```sql
SELECT
    task,
    COUNT(*) AS executions,
    ROUND(AVG(duration_seconds), 4) AS avg_seconds,
    ROUND(MAX(duration_seconds), 4) AS max_seconds
FROM performance_logs
GROUP BY task
ORDER BY avg_seconds DESC;
```

```sql
SELECT
    opta_match_id,
    ROUND(SUM(duration_seconds), 4) AS total_seconds
FROM performance_logs
WHERE task_scope = 'match'
  AND task <> 'match_total'
GROUP BY opta_match_id
ORDER BY total_seconds DESC;
```

## Why these performance choices were made

There are two kinds of performance in this project, and they are not identical:

- transformation performance: how fast the Python/Polars pipeline reads, reshapes, and writes data
- warehouse read performance: how cheaply Snowflake or Databricks can skip irrelevant data later

The design tries to improve both, but not every technique affects both equally.

### 1. Remove analytics-only columns

Columns like sprint flags, pressing flags, distance travelled, and live-time deltas are useful for football analysis, but they do not help Snowflake or Databricks skip irrelevant data. They also force extra math and branching on every row. Removing them improves:

- CPU time during transformation
- memory pressure during processing
- Parquet width
- Parquet size
- downstream scan cost

This is the biggest single performance win in the project because it cuts both compute cost and storage cost.

### 2. Keep the pipeline lazy until the end

The pipeline now uses Polars lazy execution for the full transformation path. That matters because:

- only the needed source columns are projected through the query plan
- explode/select work is fused before collection
- intermediate eager materialisations are avoided

For large tracking files, that lowers memory usage and usually shortens wall-clock processing time.

An important memory-safety choice now sits before the lazy plan:

- if a valid source Parquet already exists, the engine reuses it
- if only JSONL/NDJSON exists, the engine streams that JSON source directly
- it does not automatically build a raw nested source Parquet during normal processing

This was done because converting very large nested JSONL tracking files into raw Parquet can be the single highest peak-memory step in the whole pipeline.

### 3. Add only one clustering helper: `frame_bucket`

Frame-level sports tracking data is usually queried in slices such as:

- one match
- one half or period
- one team
- a time window or frame window

`frame_bucket` is useful because it turns a very high-cardinality frame stream into a coarser layout key that warehouses can cluster or Z-order effectively. It is derived purely for storage layout, not analysis.

### 4. Keep spatial clustering columns: `pitch_zone` and `ball_zone`

`pitch_zone` and `ball_zone` were brought back because they are useful physical layout columns, not just analytics columns.

Why they stay:

- they support spatial clustering and pruning
- they help group nearby pitch states together inside warehouse storage
- they are low-cardinality compared with raw coordinates
- they are more useful for clustering than many football-analysis flags

### 5. Sort rows by warehouse-friendly keys

Rows are sorted by:

```text
opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone, frame_id, player_id
```

This choice was made so adjacent rows in Parquet are also adjacent in the most common filter paths. Better locality improves row-group statistics and helps engines skip more data.

When processing a whole directory with `save=True`, the engine keeps intermediate per-match files unsorted and applies the warehouse sort once during final compacted-batch writing. This lowers peak memory usage while preserving the final warehouse-friendly row order.

### 6. Cast to compact dtypes

The pipeline casts several columns to narrower physical types:

- IDs and counters use integer types instead of generic wider types
- coordinates and speeds use `Float32`
- low-cardinality string columns use categorical encoding in-memory before write

Why this matters:

- smaller Parquet files
- fewer bytes read per query
- better cache behaviour
- faster serialization and deserialization

### 7. Write Parquet with row-group statistics

Processed Parquet is written with:

- `zstd` compression
- explicit row-group sizing
- statistics enabled

Statistics are important because Snowflake, Databricks, and other columnar engines use row-group metadata to skip sections of files when filter predicates do not match.

### 8. Compact small files before warehouse reads

One raw processed tracking file is about `24.7 MB`, which is small for warehouse execution. For Databricks and for staged Snowflake loads, compacting many of those files into files around `300 MB` is usually a strong performance optimization.

Why `~300 MB` was chosen:

- it reduces file-open and metadata overhead
- it keeps parallelism high without producing a small-file problem
- it is large enough for efficient scan throughput
- it is still small enough to preserve selective skipping when files stay sorted by the warehouse keys

At this file size, compacting about `10-12` match files together is a sensible target.

### 9. Print explicit task runtimes

The pipeline now prints:

- `opta_match_id` being processed
- whether JSONL was converted or an existing Parquet was reused
- duration for row extraction
- duration for zone generation
- duration for `frame_bucket` and dtype work
- duration for sorting
- duration for save
- total runtime per match
- total runtime per multi-match batch

This makes the engine operationally transparent when processing a large directory.

### 10. Stream compaction from saved per-match outputs

The multi-match save path now avoids building a full compacted batch in memory.

Instead it does this:

- process one match
- build a lazy per-match output without materializing the full processed table
- save that match to its own processed Parquet file
- repeat for the next match
- lazily scan the saved processed files for a batch
- sort the final compacted batch once
- stream the compacted batch to Parquet

This change was made because materializing a full multi-match batch in memory can fail with allocation errors on large directories.

The same memory-safety principle also applies to source files:

- the engine avoids auto-converting large JSONL source files to raw Parquet during normal processing
- it streams JSONL directly into the processed-match write path when that is the safer option

## Performance techniques used

These are the performance techniques intentionally used in this project:

- schema simplification
- analytics-column removal
- lazy execution with Polars
- projection pushdown
- column pruning
- low-memory source-path resolution
- compact physical dtypes
- categorical encoding for low-cardinality strings
- derived bucketing with `frame_bucket`
- pitch-zone clustering columns
- sort-based locality
- Parquet compression with `zstd`
- Parquet row-group statistics
- Parquet row-group sizing
- small-file compaction to `~300 MB`
- explicit runtime instrumentation
- Snowflake clustering
- Databricks liquid clustering
- Databricks `ZORDER` as fallback when liquid clustering is unavailable
- avoiding legacy over-partitioning on high-cardinality match IDs

### What each technique improves

| Technique | Improves local pipeline runtime | Improves downstream warehouse reads | Why |
|---|---|---|---|
| Analytics-column removal | yes | yes | Less per-row compute and narrower files |
| Lazy execution | yes | indirectly | Reduces unnecessary materialisation and memory movement |
| Projection pushdown | yes | no | Reads only needed source columns |
| Column pruning | yes | yes | Less data moved during transform and scan |
| Low-memory source-path resolution | yes | no | Reuses valid Parquet when available, otherwise streams JSON directly without forcing a raw source-Parquet build |
| Compact dtypes | yes | yes | Smaller memory footprint and smaller Parquet |
| `frame_bucket` | small local cost, large downstream benefit | yes | Better temporal pruning key than raw frame IDs alone |
| `player_ball_distance` | small local cost, medium downstream benefit | yes | Supports proximity filters without recomputing distance |
| `has_ball_possession` | small local cost, medium downstream benefit | yes | Turns a common proximity predicate into a direct boolean filter |
| `pitch_zone` / `ball_zone` | small local cost, large downstream benefit | yes | Low-cardinality spatial helpers for clustering |
| Sort-based locality | small local cost, large downstream benefit | yes | Better row-group locality and pruning |
| Parquet statistics | small write cost | yes | Enables better row-group skipping |
| Parquet compression | yes for I/O footprint | yes | Smaller files and fewer bytes read |
| Small-file compaction | no big effect on per-row math, large effect on scan efficiency | yes | Reduces file-open overhead |
| Runtime instrumentation | no | no | Operational visibility rather than speed |
| JSONL performance logging | tiny write overhead | no | Detailed run-by-run observability for bottleneck analysis |
| DuckDB log querying | no effect on pipeline runtime | no | Fast post-run performance analysis over the JSONL log |
| Snowflake / Databricks clustering strategy | no | yes | Physical layout optimization inside the warehouse |

Each of these exists for a different reason:

- some reduce CPU during transformation
- some reduce memory pressure
- some reduce file size
- some improve metadata pruning
- some improve scan locality
- some avoid small-file and small-partition overhead

## What changed and why

### Before

The project was closer to an analytics-first tracking transform:

- it generated sprint, pressing, and high-intensity flags
- it generated distance and time-delta metrics
- it treated zone logic as part of a broader analytics transform instead of an explicit clustering strategy
- it did not support an explicit "process all files and compact them" engine path
- it did not print per-task runtimes
- it carried more football-specific logic in the base table
- it documented Databricks layout less precisely for the actual file size

### After

The project is now warehouse-first:

- the base table keeps raw tracking columns
- only one derived layout helper remains: `frame_bucket`
- clustering helpers kept in the base table include `pitch_zone` and `ball_zone`
- one lightweight filter helper kept in the base table is `player_ball_distance`
- one lightweight boolean filter helper kept in the base table is `has_ball_possession`
- `run_lazy()` keeps the fully lazy path, while `run()` materializes timed stages for operational visibility
- dtypes are narrowed for smaller Parquet files
- the physical row order is optimized for common warehouse predicates
- the documentation now separates clustering from legacy partitioning
- file compaction guidance is aligned to the real file size of about `24.7 MB` per match file
- the engine can now process one named match or all files in the directory
- the engine prints explicit task runtimes while processing
- the multi-match save path now writes per-match files first and streams compaction from those saved files

### Why this was changed

It was changed because the previous design mixed two responsibilities:

- football analysis
- physical data engineering

For a data warehouse base table, those goals should be separated. The base table should optimize for:

- low compute cost
- narrow schema
- fast ingestion
- good pruning
- cheap scans

Football-analysis features are still valid, but they belong in downstream derived models where the business logic can evolve without bloating the core tracking table.

## What was learned

The main lessons from this refactor were:

- not every derived column helps performance; many only make the table wider
- some derived columns, such as `pitch_zone` and `ball_zone`, are worth keeping when they serve clustering rather than analysis
- a warehouse table should keep the lowest useful grain and optimize physical layout around that grain
- clustering and partitioning are different techniques and should not be treated as interchangeable
- `opta_match_id` is an excellent clustering key, but not automatically a good legacy partition key for small per-match files
- file compaction matters a lot once the source files are only around `24.7 MB`
- row ordering and row-group statistics are as important as compression
- for large compaction jobs, writing intermediate per-match files is safer than trying to materialize the whole compacted batch in memory
- separating base facts from downstream analytics features keeps the platform cleaner and faster

## Why this engine is worth building

For a football club, the value of this project is not only that it is fast. The value is that it solves a real platform problem:

- raw provider tracking files are nested and awkward to work with
- the data needs to stay reprocessable
- the silver layer needs to be stable and warehouse-ready
- the solution should not be locked to one warehouse vendor
- large directory runs need to be observable and memory-safe

That is why this engine is useful even before a warehouse modeling layer exists. It gives clubs a consistent normalized tracking table, detailed runtime visibility, and a portable ingestion path that can feed Snowflake, Databricks, or on-prem environments.

The most important architectural choice is that the engine is data-engineering-first rather than analysis-first:

- it preserves the fine tracking grain
- it keeps only the derived columns that materially help layout or filtering
- it avoids loading the base table with tactical feature logic
- it makes downstream modeling easier instead of trying to do everything at ingest time

## Polars versus dbt and ELT

This project is not intended as an argument against dbt. It is intended as a clear separation of responsibilities.

### What Polars is better at in this architecture

Polars is a strong fit for the raw-to-silver step because the incoming tracking source is nested and requires structural normalization:

- flattening nested player arrays
- standardizing raw source fields
- deriving a small set of storage helpers
- writing compacted warehouse-friendly files
- controlling memory outside the warehouse

For this exact stage, a Polars engine is often a better fit than pushing the whole transformation into warehouse SQL. It gives one transformation language across Snowflake, Databricks, local development, and on-prem execution. It also avoids forcing the most awkward structural reshaping step into the warehouse billing and execution model.

### What dbt is better at in this architecture

dbt becomes more valuable after the normalized silver tracking table exists. It is especially strong for:

- joining tracking with event data
- building conformed match, player, and team models
- incremental warehouse-native marts
- tests, lineage, and documentation
- business logic that should live close to the warehouse

In other words:

- Polars is strongest for raw ingestion and normalization
- dbt is strongest for warehouse modeling, semantic layers, and downstream marts

### Would pure dbt ELT be faster?

Not necessarily, and for this specific raw tracking normalization step, usually not end-to-end.

If the data is already structured inside Snowflake or Databricks tables, dbt can be extremely effective. But this engine is handling the awkward earlier step where the source is nested, wide, and operationally sensitive. A pure warehouse-first ELT rewrite would still need to:

- load the raw files first
- flatten nested tracking structures
- expand player arrays
- compute storage helper columns
- sort or cluster data for later reads

That can absolutely be done in a warehouse, but it is not automatically simpler, cheaper, or faster just because it runs inside Snowflake or Databricks. In practice, `4 matches in about 20 seconds` is already a strong result for this kind of normalization workload.

The practical rule is:

- use warehouse compute when the data is already in good table form
- use a dedicated ingestion engine when the hard problem is turning raw nested files into good table form

### Why this is not really an ETL versus ELT argument

The raw data can still be retained in object storage such as S3, Azure Blob, or similar storage for full reprocessing. That means the architecture still preserves the raw layer even if the normalized silver layer is later loaded into Snowflake or Databricks.

So the real decision is not just terminology. The real decision is where the heavy structural normalization runs:

- inside the warehouse
- or before the warehouse in a dedicated transformation engine

This project chooses the second option for the normalization step because it improves portability, control, and operational clarity.

## Recommended platform shape

The strongest design for clubs is usually a hybrid architecture:

1. Raw layer: provider tracking JSONL/NDJSON stored in object storage
2. Normalization layer: this Polars engine converts raw tracking into warehouse-ready silver Parquet
3. Warehouse load: silver data is loaded into Snowflake, Databricks, or another analytical platform
4. Modeling layer: dbt builds integrated models, tests, marts, and event-to-tracking joins

This becomes even more useful when synchronizing event data with tracking data. In that setup:

- the tracking engine prepares a clean, stable tracking fact table
- event ingestion prepares a clean event fact table
- dbt becomes the right place to align both on match, period, frame/time, player, and team context

That split keeps each tool in the place where it is strongest.

### Bottom line

This engine is worth building because it is not just a fast script. It is a reusable normalization layer that:

- protects the warehouse from the ugliest raw ingest work
- keeps the base tracking table lean and reprocessable
- stays portable across warehouse choices
- makes downstream modeling easier
- gives clubs a practical path to integrate tracking and event data cleanly

The recommended answer is not "Polars instead of dbt" or "dbt instead of Polars". The recommended answer is:

- Polars for raw-to-silver tracking normalization
- dbt for silver-to-gold warehouse modeling

Useful official references:

- dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models
- dbt Python models: https://docs.getdbt.com/docs/build/python-models
- dbt microbatch incremental models: https://docs.getdbt.com/docs/build/incremental-microbatch
- dbt supported platforms: https://docs.getdbt.com/docs/supported-data-platforms
- Snowflake semi-structured data loading: https://docs.snowflake.com/en/en/user-guide/data-load-prepare
- Snowflake semi-structured querying: https://docs.snowflake.com/en/user-guide/querying-semistructured

## Configuration

```python
from tracking_engine import StorageConfig, TrackingPipeline

cfg = StorageConfig(
    frame_bucket_size=500,
    parquet_row_group_size=250_000,
    parquet_compression="zstd",
    target_compacted_file_size_mb=300,
)

pipeline = TrackingPipeline(
    storage="local",
    model="denormalized",
    storage_config=cfg,
)

df = pipeline.run("2562179", save=True)
```

`TrackingPipeline` now gets source/export locations from `config.yaml`. A typical file looks like this:

```yaml
settings:
  model: denormalized
  save: true
  output_name: tracking_compacted
  ball_possession:
    column_name: has_ball_possession
    distance_m: 1.75
    min_frames: 5
  speed_bands:
    column_name: player_speed_band
    standing_max_m_s: 0.2
    walking_max_m_s: 2.0
    jogging_max_m_s: 4.0
    running_max_m_s: 5.5
    high_speed_running_max_m_s: 7.0
  patterns:
    column_name: event_type
    closest_frame: 0.85
    persist_frames: 5
    pass/shot/clearance:
      frame_sequence: 8
      start_distance_min: 0.8
      start_distance_max: 0.85
      leaving_distance: 1.5
      min_speed: 5.2
  player_ball_distance:
    column_name: player_ball_distance
  zones:
    pitch_zone_column: pitch_zone
    ball_zone_column: ball_zone
    penalty_box_depth_m: 16.5
    wide_channel_depth_m: 35.0
    penalty_box_side_inset_m: 13.84
    central_band_divisor: 6.0

local:
  read_from: 'C:\Users\adamm\Downloads\tracking_files\tracking'
  export_to: 'C:\Users\adamm\Downloads\tracking_files\tracking\curated'
  metadata_from: 'C:\Users\adamm\Downloads\tracking_files\metadata'

aws_s3:
  read_from: 's3://<bucket-name>/<key-name>/tracking'
  export_to: 'C:\Users\adamm\Downloads\tracking_files\tracking\curated'
  metadata_from: 'C:\Users\adamm\Downloads\tracking_files\metadata'
  storage_options:
    key: '<aws-access-key-id>'
    secret: '<aws-secret-access-key>'
    client_kwargs:
      region_name: '<aws-region>'

azure_blob:
  read_from: 'az://<container>/tracking'
  export_to: 'C:\Users\adamm\Downloads\tracking_files\tracking\curated'
  metadata_from: 'C:\Users\adamm\Downloads\tracking_files\metadata'
  storage_options:
    account_name: '<azure-storage-account>'
    account_key: '<azure-storage-key>'

adls:
  read_from: 'abfss://<container>@<account>.dfs.core.windows.net/tracking'
  export_to: 'C:\Users\adamm\Downloads\tracking_files\tracking\curated'
  metadata_from: 'C:\Users\adamm\Downloads\tracking_files\metadata'
  storage_options:
    account_name: '<adls-account-name>'
    account_key: '<adls-account-key>'
```

For remote profiles, `storage_options` is passed straight through to `fsspec`, so you can supply the auth keys your cloud driver expects. If you omit it, `s3fs` and `adlfs` fall back to their usual environment or managed-identity credential chain.

The most important `StorageConfig` fields are:

| Config field | Meaning |
|---|---|
| `frame_bucket_size` | Number of frames grouped into one `frame_bucket` |
| `ball_possession_distance_m` | Distance threshold used when testing for possession runs |
| `ball_possession_min_frames` | Minimum consecutive-frame run required before `has_ball_possession = True` |
| `player_speed_band_column` | Output column name for the derived player speed activity band |
| `event_type_column` | Output column name for the dynamic event-pattern helper |
| `parquet_row_group_size` | Target Parquet row-group size for write-time metadata layout |
| `parquet_compression` | Compression codec used for written Parquet |
| `target_compacted_file_size_mb` | Approximate target size for compacted multi-match outputs |
| `length` | Pitch length used for zone boundaries, in meters |
| `width` | Pitch width used for zone boundaries, in meters |

The most important YAML-driven runtime defaults are:

| YAML setting | Meaning |
|---|---|
| `settings.model` | Default output model when `TrackingPipeline(..., model=...)` is not passed |
| `settings.save` | Default save behavior when `run(save=...)` is not passed |
| `settings.output_name` | Default compacted output base name when `run(output_name=...)` is not passed |
| `settings.ball_possession.distance_m` | Threshold for the boolean possession helper |
| `settings.ball_possession.min_frames` | Consecutive-frame run length required for the boolean possession helper |
| `settings.ball_possession.column_name` | Output column name for the boolean possession helper |
| `settings.speed_bands.column_name` | Output column name for the derived player speed activity band |
| `settings.speed_bands.*_max_m_s` | Ordered meters-per-second thresholds used to classify `player_speed` |
| `settings.patterns.column_name` | Output column name for the event-pattern helper |
| `settings.patterns.closest_frame` | Fallback max touch-distance threshold used when an event-specific distance band is not set |
| `settings.patterns.persist_frames` | How many consecutive frames keep the detected event label, including the triggering touch frame |
| `settings.patterns.<event_name>.start_distance_min` | Lower bound of the player-ball distance band used when searching for the final close-contact frame before release |
| `settings.patterns.<event_name>.start_distance_max` | Upper bound of the player-ball distance band used when searching for the final close-contact frame before release |
| `settings.patterns.<event_name>.frame_sequence` | How many future frames to inspect after the touch frame when confirming the event |
| `settings.patterns.<event_name>.leaving_distance` | Player-ball distance that confirms the ball has left the player |
| `settings.patterns.<event_name>.min_speed` | Preferred ball-speed threshold key that confirms the configured event |
| `settings.patterns.<event_name>.speed` | Backward-compatible alias for the event ball-speed threshold |
| `settings.player_ball_distance.column_name` | Output column name for player-ball distance |
| `settings.zones.pitch_zone_column` | Output column name for the player zone helper |
| `settings.zones.ball_zone_column` | Output column name for the ball zone helper |
| `settings.zones.*` geometry values | Zone-boundary dimensions used when assigning zones |

## Lazy usage

If you want to keep the transformation lazy for downstream orchestration:

```python
from tracking_engine import TrackingPipeline

pipeline = TrackingPipeline(storage="local", model="normalized")
lazy_df = pipeline.run_lazy("2562179", teams=["home", "away"])
df = lazy_df.collect(streaming=True)
```

## Processing modes

Single match:

```python
pipeline.run("2562179", save=True)
```

Selected matches:

```python
pipeline.run(["2562179", "2562180"])
```

All files in the directory with compaction:

```python
pipeline.run(save=True, output_name="tracking_compacted")
```

That call saves:

- `1234567_processed.parquet`
- `1234568_processed.parquet`
- compacted outputs such as `tracking_compacted_batch_001.parquet`

And it returns a small summary DataFrame describing the compacted batch files.

## Snowflake guidance

Recommended clustering key:

```sql
ALTER TABLE tracking_data
CLUSTER BY (opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone);
```

Why this order:

- `opta_match_id` isolates a single match immediately
- `period` narrows the frame space further
- `team` keeps home and away slices co-located
- `frame_bucket` improves time-window pruning inside each slice
- `pitch_zone` and `ball_zone` add spatial clustering value inside each slice

## Databricks guidance

Recommended compaction target:

```text
~300 MB per Parquet file
```

Recommended layout for a new Databricks Delta table:

```sql
ALTER TABLE tracking_data
CLUSTER BY (opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone);
```

Recommended legacy partition columns at this match-file grain:

```text
none by default
```

Why this choice was made:

- `opta_match_id` absolutely is a strong Databricks layout key because queries will often filter to a match
- the important distinction is `CLUSTER BY` versus legacy `PARTITIONED BY`
- Databricks now recommends liquid clustering for new tables, and specifically calls out high-cardinality filter columns as a good fit
- legacy partitioning works best for low or known-cardinality columns and can create too many small partitions when one match is only `24.7 MB`
- compaction to `~300 MB` plus clustering on `opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone` gives the pruning benefit you want without fragmenting the table layout

If you are using older Databricks patterns without liquid clustering, `ZORDER BY (opta_match_id, period, team, frame_bucket, pitch_zone, ball_zone)` is the fallback, but it is not used together with liquid clustering.

If you have broader business columns upstream, such as `competition_id`, `season_id`, or `match_date`, those are better legacy Databricks partition candidates than `opta_match_id`.

## Common failure modes

These are the main operational failures this project is designed to avoid and how the current version handles them.

### `Allocation error : not enough memory`

Typical cause:

- a very large raw `.jsonl` is being converted into a nested source `.parquet`
- or many processed matches are being collected together before write

Why it used to happen:

- raw JSON-to-Parquet conversion can be the single highest peak-memory step in the whole workflow
- eager multi-match concatenation creates a second large peak during compaction

What the current pipeline does instead:

- reuses an existing valid source `.parquet` when possible
- otherwise streams `.jsonl` / `.ndjson` directly into the processed output flow
- writes one processed match at a time
- compacts saved Parquet files afterward

Operational guidance:

- prefer `run(save=True, output_name="tracking_compacted")` for whole-directory processing
- avoid custom workflows that collect many matches into one eager DataFrame
- if memory pressure still appears, reduce `target_compacted_file_size_mb`

### `parquet: File out of specification: A parquet file must contain a header and footer with at least 12 bytes`

Typical cause:

- an older run was interrupted and left behind a corrupt or incomplete `.parquet` file

What the current pipeline does:

- validates source `.parquet` files before using them
- if the `.parquet` is invalid but `.jsonl` or `.ndjson` still exists, it streams the JSON source directly
- uses atomic writes for generated Parquet outputs so a partially written final file is much less likely

Operational guidance:

- keep the original source `.jsonl` / `.ndjson` when possible
- if you see this error on an old source `.parquet`, the engine should now bypass it automatically when JSON is still available
- if only the corrupt `.parquet` remains and no JSON source exists, the file must be replaced from source

### Slow reruns after a successful first run

Typical cause:

- rerunning from JSON sources when reusable processed outputs or valid source Parquet files already exist

What improves rerun performance:

- valid source `.parquet` reuse
- saved per-match processed Parquet files
- compacted output batches near the configured target size
- the JSONL performance log, which makes bottlenecks easy to inspect with DuckDB

Useful debugging workflow:

```python
from tracking_engine import TrackingPipeline

pipeline = TrackingPipeline(
    storage="local",
    model="denormalized",
)

logs_df = pipeline.query_logs("""
    SELECT
        opta_match_id,
        task,
        duration_seconds,
        source_status,
        file_path
    FROM performance_logs
    ORDER BY duration_seconds DESC
""")
```

## What was intentionally removed

These features were removed on purpose to keep the project data-engineering-first:

- all sprint / high-intensity / pressing flags
- all frame-to-frame movement metrics
- all threshold configuration related to football analysis

If analytics features are ever needed again, the best design is to build them in a separate downstream mart instead of in the base tracking table.
