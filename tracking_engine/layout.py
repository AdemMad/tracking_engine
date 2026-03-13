import polars as pl

from .config import DEFAULT_STORAGE, EventPatternRule, StorageConfig

TARGET_COMPACTED_FILE_SIZE_MB = DEFAULT_STORAGE.target_compacted_file_size_mb
DATABRICKS_PARTITION_COLUMNS: tuple[str, ...] = ()
DATABRICKS_LIQUID_CLUSTER_COLUMNS = DEFAULT_STORAGE.databricks_liquid_cluster_columns
DATABRICKS_ZORDER_COLUMNS = DATABRICKS_LIQUID_CLUSTER_COLUMNS
SNOWFLAKE_CLUSTER_COLUMNS = DEFAULT_STORAGE.snowflake_cluster_columns
WAREHOUSE_SORT_COLUMNS = DEFAULT_STORAGE.warehouse_sort_columns


def _schema_names(frame: pl.DataFrame | pl.LazyFrame) -> set[str]:
    if isinstance(frame, pl.DataFrame):
        return set(frame.columns)
    return set(frame.collect_schema().names())


def _ordered_event_rules(cfg: StorageConfig) -> list[EventPatternRule]:
    indexed_rules = list(enumerate(cfg.event_pattern.events))
    indexed_rules.sort(
        key=lambda item: (
            -item[1].speed_m_s,
            -item[1].leaving_distance_m,
            item[1].frame_sequence,
            item[0],
        )
    )
    return [rule for _, rule in indexed_rules]


def _event_start_expr(
    cfg: StorageConfig,
    rule: EventPatternRule,
    run_group_columns: list[str],
) -> pl.Expr:
    distance_expr = pl.col(cfg.player_ball_distance_column)
    start_distance_min = (
        0.0 if rule.start_distance_min_m is None else rule.start_distance_min_m
    )
    start_distance_max = (
        cfg.event_pattern.closest_frame_m
        if rule.start_distance_max_m is None
        else rule.start_distance_max_m
    )
    candidate_band_expr = (
        distance_expr.ge(start_distance_min) & distance_expr.le(start_distance_max)
    )

    release_paths: list[pl.Expr] = []
    for release_offset in range(1, max(1, rule.frame_sequence) + 1):
        release_frame_expr = pl.col("frame_id").shift(-release_offset).over(run_group_columns)
        release_distance_expr = distance_expr.shift(-release_offset).over(run_group_columns)
        release_ball_speed_expr = pl.col("ball_speed").shift(-release_offset).over(run_group_columns)

        no_later_band_expr: pl.Expr = pl.lit(True)
        for offset in range(1, release_offset):
            later_frame_expr = pl.col("frame_id").shift(-offset).over(run_group_columns)
            later_distance_expr = distance_expr.shift(-offset).over(run_group_columns)
            later_band_expr = (
                later_frame_expr.eq(pl.col("frame_id") + offset).fill_null(False)
                & later_distance_expr.ge(start_distance_min).fill_null(False)
                & later_distance_expr.le(start_distance_max).fill_null(False)
            )
            no_later_band_expr = no_later_band_expr & later_band_expr.not_()

        release_paths.append(
            release_frame_expr.eq(pl.col("frame_id") + release_offset).fill_null(False)
            & release_distance_expr.ge(rule.leaving_distance_m).fill_null(False)
            & release_ball_speed_expr.ge(rule.speed_m_s).fill_null(False)
            & no_later_band_expr
        )

    release_path_expr = release_paths[0]
    if len(release_paths) > 1:
        release_path_expr = pl.any_horizontal(*release_paths)
    return candidate_band_expr & release_path_expr


def _event_type_expr(
    cfg: StorageConfig,
    run_group_columns: list[str],
) -> pl.Expr:
    ordered_rules = _ordered_event_rules(cfg)
    if not ordered_rules:
        return pl.lit(None, dtype=pl.String).alias(cfg.event_type_column)

    event_expr: pl.Expr = pl.lit(None, dtype=pl.String)
    for rule in reversed(ordered_rules):
        event_start_expr = _event_start_expr(cfg, rule, run_group_columns)
        future_release_checks: list[pl.Expr] = []
        for offset in range(1, max(1, rule.frame_sequence) + 1):
            future_frame_expr = pl.col("frame_id").shift(-offset).over(run_group_columns)
            future_distance_expr = (
                pl.col(cfg.player_ball_distance_column).shift(-offset).over(run_group_columns)
            )
            future_ball_speed_expr = pl.col("ball_speed").shift(-offset).over(run_group_columns)
            future_release_checks.append(
                future_frame_expr.eq(pl.col("frame_id") + offset).fill_null(False)
                & future_distance_expr.ge(rule.leaving_distance_m).fill_null(False)
                & future_ball_speed_expr.ge(rule.speed_m_s).fill_null(False)
            )

        release_expr = future_release_checks[0]
        if len(future_release_checks) > 1:
            release_expr = pl.any_horizontal(*future_release_checks)

        event_expr = (
            pl.when(event_start_expr & release_expr)
            .then(pl.lit(rule.name))
            .otherwise(event_expr)
        )

    persisted_event_exprs: list[pl.Expr] = [event_expr]
    for offset in range(1, max(1, cfg.event_pattern.persist_frames)):
        prior_event_expr = event_expr.shift(offset).over(run_group_columns)
        prior_frame_expr = pl.col("frame_id").shift(offset).over(run_group_columns)
        persisted_event_exprs.append(
            pl.when(prior_frame_expr.eq(pl.col("frame_id") - offset).fill_null(False))
            .then(prior_event_expr)
            .otherwise(None)
        )

    return pl.coalesce(persisted_event_exprs).cast(pl.Categorical).alias(cfg.event_type_column)


def add_storage_layout_columns(
    frame: pl.DataFrame | pl.LazyFrame,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Keep the output warehouse-first:
    - retain only raw tracking fields plus storage-layout helpers
    - cast to compact dtypes for smaller Parquet files
    - add frame_bucket for temporal pruning inside each match/period/team slice
    - add a 5-minute game-clock split for lower-cardinality time analysis
    - mark possession only after a player stays within the ball-distance threshold
      for a sustained run of consecutive frames
    """
    player_ball_distance_expr = (
        (
            (pl.col("player_x") - pl.col("ball_x")).pow(2)
            + (pl.col("player_y") - pl.col("ball_y")).pow(2)
        )
        .sqrt()
        .cast(pl.Float32)
    )
    player_identity_expr = pl.coalesce(
        [
            pl.col("player_id").cast(pl.Utf8),
            pl.col("opta_player_id").cast(pl.Utf8),
            pl.col("player_number").cast(pl.Utf8),
            pl.lit("__unknown_player__"),
        ]
    ).alias("__player_identity")
    possession_candidate_column = "__ball_possession_candidate"
    possession_run_id_column = "__ball_possession_run_id"
    possession_run_length_column = "__ball_possession_run_length"
    row_number_column = "__row_nr"
    run_group_columns = ["opta_match_id", "period", "team", "__player_identity"]
    needs_player_ball_distance = any(
        [
            cfg.derived_columns.player_ball_distance,
            cfg.derived_columns.has_ball_possession,
            cfg.derived_columns.event_type,
        ]
    )
    speed_band_expr = (
        pl.when(pl.col("player_speed").is_null())
        .then(None)
        .when(pl.col("player_speed") <= cfg.speed_band.standing_max_m_s)
        .then(pl.lit("standing"))
        .when(pl.col("player_speed") <= cfg.speed_band.walking_max_m_s)
        .then(pl.lit("walking_low_speed"))
        .when(pl.col("player_speed") <= cfg.speed_band.jogging_max_m_s)
        .then(pl.lit("jogging_moderate_running"))
        .when(pl.col("player_speed") <= cfg.speed_band.running_max_m_s)
        .then(pl.lit("running"))
        .when(pl.col("player_speed") <= cfg.speed_band.high_speed_running_max_m_s)
        .then(pl.lit("high_speed_running"))
        .otherwise(pl.lit("sprinting"))
        .cast(pl.Categorical)
    )

    frame = frame.with_row_count(row_number_column).with_columns(player_identity_expr)
    frame = frame.sort(run_group_columns + ["frame_id"])

    expressions: list[pl.Expr] = [
        pl.col("opta_match_id").cast(pl.Int64),
        pl.col("period").cast(pl.Int8),
        pl.col("frame_id").cast(pl.Int32),
        pl.col("game_clock").cast(pl.Float32),
        pl.col("wall_clock").cast(pl.Int64),
        pl.col("opta_player_id").cast(pl.Int64),
        pl.col("player_number").cast(pl.Int16),
        pl.col("player_x").cast(pl.Float32),
        pl.col("player_y").cast(pl.Float32),
        pl.col("player_z").cast(pl.Float32),
        pl.col("player_speed").cast(pl.Float32),
        pl.col("ball_x").cast(pl.Float32),
        pl.col("ball_y").cast(pl.Float32),
        pl.col("ball_z").cast(pl.Float32),
        pl.col("ball_speed").cast(pl.Float32),
        pl.col("live").cast(pl.Boolean),
        pl.col("team").cast(pl.Categorical),
        pl.col("last_touch").cast(pl.Categorical),
    ]
    if cfg.derived_columns.min_split:
        expressions.insert(
            4,
            ((((pl.col("game_clock") / 300.0).floor().cast(pl.Int16)) + 1) * 5)
            .cast(pl.Int16)
            .alias("min_split"),
        )
    if cfg.derived_columns.player_speed_band:
        expressions.append(speed_band_expr.alias(cfg.player_speed_band_column))
    if cfg.derived_columns.frame_bucket:
        expressions.append(
            (pl.col("frame_id") // cfg.frame_bucket_size).cast(pl.Int32).alias("frame_bucket")
        )
    if needs_player_ball_distance:
        expressions.append(player_ball_distance_expr.alias(cfg.player_ball_distance_column))

    schema_names = _schema_names(frame)
    if cfg.derived_columns.pitch_zone and cfg.pitch_zone_column in schema_names:
        expressions.append(pl.col(cfg.pitch_zone_column).cast(pl.Categorical))
    if cfg.derived_columns.ball_zone and cfg.ball_zone_column in schema_names:
        expressions.append(pl.col(cfg.ball_zone_column).cast(pl.Categorical))
    if "pitch_length" in schema_names:
        expressions.append(pl.col("pitch_length").cast(pl.Float32))
    if "pitch_width" in schema_names:
        expressions.append(pl.col("pitch_width").cast(pl.Float32))
    if "fixture" in schema_names:
        expressions.append(pl.col("fixture").cast(pl.Utf8))
    if "match_date" in schema_names:
        expressions.append(pl.col("match_date").cast(pl.Date))
    if "player_name" in schema_names:
        expressions.append(pl.col("player_name").cast(pl.Utf8))
    if "player_position" in schema_names:
        expressions.append(pl.col("player_position").cast(pl.Categorical))

    frame = frame.with_columns(expressions)
    if cfg.derived_columns.event_type:
        frame = frame.with_columns(_event_type_expr(cfg, run_group_columns))

    if cfg.derived_columns.has_ball_possession:
        frame = frame.with_columns(
            pl.col(cfg.player_ball_distance_column)
            .le(cfg.ball_possession_distance_m)
            .alias(possession_candidate_column)
        )

        group_changed_expr = pl.any_horizontal(
            *[(pl.col(column) != pl.col(column).shift(1)).fill_null(True) for column in run_group_columns]
        )
        frame_gap_expr = (pl.col("frame_id") - pl.col("frame_id").shift(1)).fill_null(1)
        possession_run_start_expr = (
            group_changed_expr
            | pl.col(possession_candidate_column).shift(1).not_().fill_null(True)
            | frame_gap_expr.ne(1)
        )

        frame = frame.with_columns(
            pl.when(pl.col(possession_candidate_column))
            .then(
                pl.when(possession_run_start_expr)
                .then(1)
                .otherwise(0)
                .cum_sum()
            )
            .otherwise(None)
            .alias(possession_run_id_column)
        )
        frame = frame.with_columns(
            pl.when(pl.col(possession_candidate_column))
            .then(pl.len().over(possession_run_id_column))
            .otherwise(0)
            .cast(pl.Int16)
            .alias(possession_run_length_column)
        )
        frame = frame.with_columns(
            (
                pl.col(possession_candidate_column)
                & pl.col(possession_run_length_column).ge(cfg.ball_possession_min_frames)
            ).alias(cfg.has_ball_possession_column)
        )

    drop_columns = [row_number_column, "__player_identity"]
    if cfg.derived_columns.has_ball_possession:
        drop_columns.extend(
            [
                possession_candidate_column,
                possession_run_id_column,
                possession_run_length_column,
            ]
        )

    return frame.sort(row_number_column).drop(drop_columns)


def select_output_columns(
    frame: pl.DataFrame | pl.LazyFrame,
    model: str,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> pl.DataFrame | pl.LazyFrame:
    columns = cfg.denormalized_output_columns if model == "denormalized" else cfg.normalized_output_columns
    existing = _schema_names(frame)
    return frame.select([pl.col(column) for column in columns if column in existing])
