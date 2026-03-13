from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ZoneConfig:
    pitch_zone_column: str = "pitch_zone"
    ball_zone_column: str = "ball_zone"
    penalty_box_depth_m: float = 16.5
    wide_channel_depth_m: float = 35.0
    penalty_box_side_inset_m: float = 13.84
    central_band_divisor: float = 6.0


@dataclass(frozen=True)
class SpeedBandConfig:
    column_name: str = "player_speed_band"
    standing_max_m_s: float = 0.2
    walking_max_m_s: float = 2.0
    jogging_max_m_s: float = 4.0
    running_max_m_s: float = 5.5
    high_speed_running_max_m_s: float = 7.0


@dataclass(frozen=True)
class EventPatternRule:
    name: str
    frame_sequence: int = 5
    start_distance_min_m: float | None = None
    start_distance_max_m: float | None = None
    leaving_distance_m: float = 1.5
    speed_m_s: float = 0.0


@dataclass(frozen=True)
class EventPatternConfig:
    column_name: str = "event_type"
    closest_frame_m: float = 0.2
    persist_frames: int = 1
    events: tuple[EventPatternRule, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DerivedColumnConfig:
    pitch_zone: bool = True
    ball_zone: bool = True
    frame_bucket: bool = True
    min_split: bool = True
    player_ball_distance: bool = True
    has_ball_possession: bool = True
    player_speed_band: bool = True
    event_type: bool = True


@dataclass(frozen=True)
class StorageConfig:
    """
    Warehouse-oriented configuration.

    The pipeline derives storage-layout helpers such as frame buckets and
    pitch zones, and it also controls Parquet write settings, compaction
    sizing, output defaults, and configurable helper-column names.
    """

    frame_bucket_size: int = 500
    player_ball_distance_column: str = "player_ball_distance"
    has_ball_possession_column: str = "has_ball_possession"
    ball_possession_distance_m: float = 1.75
    ball_possession_min_frames: int = 5
    parquet_row_group_size: int = 250_000
    parquet_compression: str = "zstd"
    target_compacted_file_size_mb: int = 300
    length: float = 105.0
    width: float = 68.0
    default_model: str = "normalized"
    default_save: bool = False
    default_output_name: str = "tracking_compacted"
    zone: ZoneConfig = field(default_factory=ZoneConfig)
    speed_band: SpeedBandConfig = field(default_factory=SpeedBandConfig)
    event_pattern: EventPatternConfig = field(default_factory=EventPatternConfig)
    derived_columns: DerivedColumnConfig = field(default_factory=DerivedColumnConfig)

    @property
    def pitch_zone_column(self) -> str:
        return self.zone.pitch_zone_column

    @property
    def ball_zone_column(self) -> str:
        return self.zone.ball_zone_column

    @property
    def player_speed_band_column(self) -> str:
        return self.speed_band.column_name

    @property
    def event_type_column(self) -> str:
        return self.event_pattern.column_name

    @property
    def x_min(self) -> float:
        return -self.length / 2

    @property
    def x_max(self) -> float:
        return self.length / 2

    @property
    def y_min(self) -> float:
        return -self.width / 2

    @property
    def y_max(self) -> float:
        return self.width / 2

    @property
    def warehouse_sort_columns(self) -> tuple[str, ...]:
        columns = ["opta_match_id", "period", "team"]
        if self.derived_columns.frame_bucket:
            columns.append("frame_bucket")
        if self.derived_columns.pitch_zone:
            columns.append(self.pitch_zone_column)
        if self.derived_columns.ball_zone:
            columns.append(self.ball_zone_column)
        columns.extend(["frame_id", "player_id"])
        return tuple(columns)

    @property
    def databricks_liquid_cluster_columns(self) -> tuple[str, ...]:
        columns = ["opta_match_id", "period", "team"]
        if self.derived_columns.frame_bucket:
            columns.append("frame_bucket")
        if self.derived_columns.pitch_zone:
            columns.append(self.pitch_zone_column)
        if self.derived_columns.ball_zone:
            columns.append(self.ball_zone_column)
        return tuple(columns)

    @property
    def snowflake_cluster_columns(self) -> tuple[str, ...]:
        return self.databricks_liquid_cluster_columns

    @property
    def normalized_output_columns(self) -> tuple[str, ...]:
        columns: list[str] = [
            "opta_match_id",
            "team",
            "period",
            "frame_id",
            "game_clock",
            "wall_clock",
            "live",
            "last_touch",
            "opta_player_id",
            "player_id",
            "player_number",
            "player_x",
            "player_y",
            "player_z",
            "player_speed",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_speed",
            "pitch_length",
            "pitch_width",
        ]
        if self.derived_columns.min_split:
            columns.insert(5, "min_split")
        if self.derived_columns.player_speed_band:
            columns.insert(columns.index("ball_x"), self.player_speed_band_column)
        if self.derived_columns.pitch_zone:
            columns.append(self.pitch_zone_column)
        if self.derived_columns.ball_zone:
            columns.append(self.ball_zone_column)
        if self.derived_columns.frame_bucket:
            columns.append("frame_bucket")
        if self.derived_columns.player_ball_distance:
            columns.append(self.player_ball_distance_column)
        if self.derived_columns.has_ball_possession:
            columns.append(self.has_ball_possession_column)
        if self.derived_columns.event_type:
            columns.append(self.event_type_column)
        return tuple(columns)

    @property
    def denormalized_output_columns(self) -> tuple[str, ...]:
        columns: list[str] = [
            "opta_match_id",
            "fixture",
            "match_date",
            "team",
            "period",
            "frame_id",
            "game_clock",
            "wall_clock",
            "live",
            "last_touch",
            "opta_player_id",
            "player_id",
            "player_name",
            "player_position",
            "player_number",
            "player_x",
            "player_y",
            "player_z",
            "player_speed",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_speed",
            "pitch_length",
            "pitch_width",
        ]
        if self.derived_columns.min_split:
            columns.insert(7, "min_split")
        if self.derived_columns.player_speed_band:
            columns.insert(columns.index("ball_x"), self.player_speed_band_column)
        if self.derived_columns.pitch_zone:
            columns.append(self.pitch_zone_column)
        if self.derived_columns.ball_zone:
            columns.append(self.ball_zone_column)
        if self.derived_columns.frame_bucket:
            columns.append("frame_bucket")
        if self.derived_columns.player_ball_distance:
            columns.append(self.player_ball_distance_column)
        if self.derived_columns.has_ball_possession:
            columns.append(self.has_ball_possession_column)
        if self.derived_columns.event_type:
            columns.append(self.event_type_column)
        return tuple(columns)


DEFAULT_STORAGE = StorageConfig()

ZONE_FLIP: dict[str, str] = {
    "att_box_left": "def_box_right",
    "att_box_right": "def_box_left",
    "att_box_center": "def_box_center",
    "att_wide_left": "def_wide_right",
    "att_wide_right": "def_wide_left",
    "att_halfspace_left": "def_halfspace_right",
    "att_halfspace_right": "def_halfspace_left",
    "att_halfspace_center": "def_halfspace_center",
    "att_center": "def_center",
    "mid_wide_left": "mid_wide_right",
    "mid_wide_right": "mid_wide_left",
    "mid_halfspace_left": "mid_halfspace_right",
    "mid_halfspace_right": "mid_halfspace_left",
    "mid_center": "mid_center",
    "def_box_left": "att_box_right",
    "def_box_right": "att_box_left",
    "def_box_center": "att_box_center",
    "def_wide_left": "att_wide_right",
    "def_wide_right": "att_wide_left",
    "def_center": "att_center",
}


def load_runtime_storage_config(
    config_path: str | Path,
    base: StorageConfig = DEFAULT_STORAGE,
) -> StorageConfig:
    """
    Load runtime defaults from config.yaml and merge them onto a base StorageConfig.

    Expected YAML shape:

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
      derived_columns:
        pitch_zone: true
        ball_zone: true
        frame_bucket: true
        min_split: true
        player_ball_distance: true
        has_ball_possession: true
        player_speed_band: true
        event_type: false
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
      pitch_defaults:
        length_m: 105.0
        width_m: 68.0
      parquet:
        row_group_size: 250000
        compression: zstd
        target_compacted_file_size_mb: 300
      frame_bucket_size: 500
    """
    path = Path(config_path)
    if not path.exists():
        return base

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    settings = dict(payload.get("settings") or {})
    zones = dict(settings.get("zones") or {})
    pitch_defaults = dict(settings.get("pitch_defaults") or {})
    parquet = dict(settings.get("parquet") or {})
    ball_possession = dict(settings.get("ball_possession") or {})
    player_ball_distance = dict(settings.get("player_ball_distance") or {})
    speed_bands = dict(settings.get("speed_bands") or {})
    derived_columns = _lookup_mapping(settings, "derived_columns", "process_columns")
    event_pattern_cfg = _load_event_pattern_config(settings, base.event_pattern)

    zone_cfg = replace(
        base.zone,
        pitch_zone_column=str(zones.get("pitch_zone_column", base.zone.pitch_zone_column)),
        ball_zone_column=str(zones.get("ball_zone_column", base.zone.ball_zone_column)),
        penalty_box_depth_m=float(
            zones.get("penalty_box_depth_m", base.zone.penalty_box_depth_m)
        ),
        wide_channel_depth_m=float(
            zones.get("wide_channel_depth_m", base.zone.wide_channel_depth_m)
        ),
        penalty_box_side_inset_m=float(
            zones.get("penalty_box_side_inset_m", base.zone.penalty_box_side_inset_m)
        ),
        central_band_divisor=float(
            zones.get("central_band_divisor", base.zone.central_band_divisor)
        ),
    )
    speed_band_cfg = replace(
        base.speed_band,
        column_name=str(speed_bands.get("column_name", base.speed_band.column_name)),
        standing_max_m_s=float(
            speed_bands.get("standing_max_m_s", base.speed_band.standing_max_m_s)
        ),
        walking_max_m_s=float(
            speed_bands.get("walking_max_m_s", base.speed_band.walking_max_m_s)
        ),
        jogging_max_m_s=float(
            speed_bands.get("jogging_max_m_s", base.speed_band.jogging_max_m_s)
        ),
        running_max_m_s=float(
            speed_bands.get("running_max_m_s", base.speed_band.running_max_m_s)
        ),
        high_speed_running_max_m_s=float(
            speed_bands.get(
                "high_speed_running_max_m_s",
                base.speed_band.high_speed_running_max_m_s,
            )
        ),
    )
    derived_column_cfg = replace(
        base.derived_columns,
        pitch_zone=_coerce_bool(
            derived_columns.get("pitch_zone"),
            base.derived_columns.pitch_zone,
        ),
        ball_zone=_coerce_bool(
            derived_columns.get("ball_zone"),
            base.derived_columns.ball_zone,
        ),
        frame_bucket=_coerce_bool(
            derived_columns.get("frame_bucket"),
            base.derived_columns.frame_bucket,
        ),
        min_split=_coerce_bool(
            derived_columns.get("min_split"),
            base.derived_columns.min_split,
        ),
        player_ball_distance=_coerce_bool(
            derived_columns.get("player_ball_distance"),
            base.derived_columns.player_ball_distance,
        ),
        has_ball_possession=_coerce_bool(
            derived_columns.get("has_ball_possession"),
            base.derived_columns.has_ball_possession,
        ),
        player_speed_band=_coerce_bool(
            derived_columns.get("player_speed_band"),
            base.derived_columns.player_speed_band,
        ),
        event_type=_coerce_bool(
            derived_columns.get("event_type"),
            base.derived_columns.event_type,
        ),
    )

    return replace(
        base,
        frame_bucket_size=int(settings.get("frame_bucket_size", base.frame_bucket_size)),
        player_ball_distance_column=str(
            player_ball_distance.get("column_name", base.player_ball_distance_column)
        ),
        has_ball_possession_column=str(
            ball_possession.get("column_name", base.has_ball_possession_column)
        ),
        ball_possession_distance_m=float(
            ball_possession.get("distance_m", base.ball_possession_distance_m)
        ),
        ball_possession_min_frames=int(
            ball_possession.get("min_frames", base.ball_possession_min_frames)
        ),
        parquet_row_group_size=int(
            parquet.get("row_group_size", base.parquet_row_group_size)
        ),
        parquet_compression=str(parquet.get("compression", base.parquet_compression)),
        target_compacted_file_size_mb=int(
            parquet.get(
                "target_compacted_file_size_mb",
                base.target_compacted_file_size_mb,
            )
        ),
        length=float(pitch_defaults.get("length_m", base.length)),
        width=float(pitch_defaults.get("width_m", base.width)),
        default_model=str(settings.get("model", base.default_model)),
        default_save=_coerce_bool(settings.get("save"), base.default_save),
        default_output_name=str(
            settings.get("output_name", base.default_output_name)
        ),
        zone=zone_cfg,
        speed_band=speed_band_cfg,
        event_pattern=event_pattern_cfg,
        derived_columns=derived_column_cfg,
    )


def _lookup_setting(mapping: dict[str, Any], *names: str, default: Any = None) -> Any:
    wanted = {name.casefold() for name in names}
    for key, value in mapping.items():
        if str(key).casefold() in wanted:
            return value
    return default


def _lookup_mapping(mapping: dict[str, Any], *names: str) -> dict[str, Any]:
    value = _lookup_setting(mapping, *names)
    if isinstance(value, dict):
        return dict(value)
    return {}


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "yes", "y", "on", "1"}:
            return True
        if normalized in {"false", "no", "n", "off", "0"}:
            return False
    return bool(value)


def _load_event_pattern_config(
    settings: dict[str, Any],
    base: EventPatternConfig,
) -> EventPatternConfig:
    patterns = _lookup_mapping(settings, "patterns", "pattern")
    raw_events = _lookup_setting(patterns, "events")
    if isinstance(raw_events, dict):
        event_items = list(dict(raw_events).items())
    else:
        event_items = [
            (key, value)
            for key, value in patterns.items()
            if str(key).casefold() not in {
                "column_name",
                "closest_frame",
                "closest_frame_m",
                "persist_frames",
                "hold_frames",
                "label_frames",
                "events",
            }
        ]

    rules: list[EventPatternRule] = []
    for event_name, raw_rule in event_items:
        if not isinstance(raw_rule, dict):
            continue
        label = str(_lookup_setting(raw_rule, "name", default=event_name)).strip()
        if not label:
            continue
        rules.append(
            EventPatternRule(
                name=label.casefold(),
                frame_sequence=max(
                    1,
                    int(_lookup_setting(raw_rule, "frame_sequence", default=5)),
                ),
                start_distance_min_m=_optional_float(
                    _lookup_setting(
                        raw_rule,
                        "start_distance_min",
                        "start_distance_min_m",
                    )
                ),
                start_distance_max_m=_optional_float(
                    _lookup_setting(
                        raw_rule,
                        "start_distance_max",
                        "start_distance_max_m",
                    )
                ),
                leaving_distance_m=float(
                    _lookup_setting(
                        raw_rule,
                        "leaving_distance",
                        "leaving_distance_m",
                        default=1.5,
                    )
                ),
                speed_m_s=float(
                    _lookup_setting(
                        raw_rule,
                        "min_speed",
                        "speed",
                        "speed_m_s",
                        default=0.0,
                    )
                ),
            )
        )

    column_name = _lookup_setting(patterns, "column_name", default=base.column_name)
    closest_frame = _lookup_setting(
        patterns,
        "closest_frame",
        "closest_frame_m",
        default=base.closest_frame_m,
    )
    persist_frames = _lookup_setting(
        patterns,
        "persist_frames",
        "hold_frames",
        "label_frames",
        default=base.persist_frames,
    )
    return replace(
        base,
        column_name=base.column_name if column_name is None else str(column_name),
        closest_frame_m=float(closest_frame),
        persist_frames=max(1, int(persist_frames)),
        events=tuple(rules),
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
