from pathlib import Path

import fsspec
import polars as pl

from tracking_engine import TrackingPipeline
from tracking_engine.config import load_runtime_storage_config
from tracking_engine.storage import StorageManager, load_storage_profile


def _player(opta_player_id: int | str, player_id: str, number: int, x: float, y: float, speed: float) -> dict:
    return {
        "optaId": opta_player_id,
        "playerId": player_id,
        "number": number,
        "xyz": [x, y, 0.0],
        "speed": speed,
    }


def _ball(x: float, y: float, speed: float) -> dict:
    return {
        "xyz": [x, y, 0.0],
        "speed": speed,
    }


def _write_config(base_dir: Path) -> tuple[Path, Path, Path]:
    tracking_dir = base_dir / "tracking"
    curated_dir = base_dir / "curated"
    metadata_dir = base_dir / "metadata"
    tracking_dir.mkdir()
    curated_dir.mkdir()
    metadata_dir.mkdir()

    config_path = base_dir / "config.yaml"
    config_path.write_text(
        "\n".join([
            "settings:",
            "  model: denormalized",
            "  save: true",
            "  output_name: tracking_compacted",
            "  frame_bucket_size: 500",
            "  ball_possession:",
            "    column_name: has_ball_possession",
            "    distance_m: 1.75",
            "    min_frames: 5",
            "  speed_bands:",
            "    column_name: player_speed_band",
            "    standing_max_m_s: 0.2",
            "    walking_max_m_s: 2.0",
            "    jogging_max_m_s: 4.0",
            "    running_max_m_s: 5.5",
            "    high_speed_running_max_m_s: 7.0",
            "  derived_columns:",
            "    pitch_zone: true",
            "    ball_zone: true",
            "    frame_bucket: true",
            "    min_split: true",
            "    player_ball_distance: true",
            "    has_ball_possession: true",
            "    player_speed_band: true",
            "    event_type: true",
            "  patterns:",
            "    column_name: event_type",
            "    closest_frame: 0.2",
            "    pass:",
            "      frame_sequence: 5",
            "      leaving_distance: 1.2",
            "      speed: 5.2",
            "    shot:",
            "      frame_sequence: 5",
            "      leaving_distance: 1.5",
            "      speed: 11.2",
            "  player_ball_distance:",
            "    column_name: player_ball_distance",
            "  zones:",
            "    pitch_zone_column: pitch_zone",
            "    ball_zone_column: ball_zone",
            "    penalty_box_depth_m: 16.5",
            "    wide_channel_depth_m: 35.0",
            "    penalty_box_side_inset_m: 13.84",
            "    central_band_divisor: 6.0",
            "  pitch_defaults:",
            "    length_m: 105.0",
            "    width_m: 68.0",
            "  parquet:",
            "    row_group_size: 250000",
            "    compression: zstd",
            "    target_compacted_file_size_mb: 300",
            "local:",
            f"  read_from: '{tracking_dir}'",
            f"  export_to: '{curated_dir}'",
            f"  metadata_from: '{metadata_dir}'",
            "aws_s3:",
            "  read_from: 's3://example-bucket/tracking'",
            f"  export_to: '{curated_dir}'",
            f"  metadata_from: '{metadata_dir}'",
            "  storage_options:",
            "    key: 'test-key'",
            "    secret: 'test-secret'",
            "    client_kwargs:",
            "      region_name: 'eu-west-2'",
        ]),
        encoding="utf-8",
    )
    return config_path, tracking_dir, metadata_dir


def _write_metadata(metadata_dir: Path, match_id: int) -> None:
    (metadata_dir / f"g{match_id}_SecondSpectrum_Metadata.json").write_text(
        """
{
  "description": "FUL - WHU : 2026-3-4",
  "year": 2026,
  "month": 3,
  "day": 4,
  "pitchLength": 99.998779296875,
  "pitchWidth": 64.99860382080078,
  "homePlayers": [
    {"name": "A. Iwobi", "position": "LW", "optaId": "10"},
    {"name": "Josh David King", "position": "CAM", "optaId": "11"}
  ],
  "awayPlayers": [
    {"name": "T. Soucek", "position": "CM", "optaId": "20"}
  ]
}
""".strip(),
        encoding="utf-8",
    )


def _write_tracking_source(path: Path) -> None:
    raw = pl.DataFrame({
        "period": [1, 1],
        "frameIdx": [0, 500],
        "gameClock": [0.0, 20.0],
        "wallClock": [1000, 21000],
        "ball": [_ball(0.0, 0.0, 0.0), _ball(1.0, 1.0, 1.2)],
        "homePlayers": [
            [_player(10, "home-1", 17, 0.5, 0.5, 4.0)],
            [_player(10, "home-1", 17, 2.0, 3.0, 4.5)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -1.0, -2.0, 3.5)],
            [_player(20, "away-1", 28, -2.0, -3.0, 3.8)],
        ],
        "live": [True, False],
        "lastTouch": ["home", "away"],
    })
    if path.suffix == ".jsonl":
        raw.write_ndjson(path)
    else:
        raw.write_parquet(path)


def _write_sustained_possession_source(path: Path) -> None:
    raw = pl.DataFrame({
        "period": [1, 1, 1, 1, 1, 1],
        "frameIdx": [0, 1, 2, 3, 4, 5],
        "gameClock": [0.0, 0.04, 0.08, 0.12, 0.16, 0.20],
        "wallClock": [1000, 1040, 1080, 1120, 1160, 1200],
        "ball": [
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
        ],
        "homePlayers": [
            [_player(10, "home-1", 17, 1.0, 0.0, 3.0), _player(11, "home-2", 11, 1.4, 0.0, 2.8)],
            [_player(10, "home-1", 17, 1.0, 0.0, 3.1), _player(11, "home-2", 11, 1.4, 0.0, 2.9)],
            [_player(10, "home-1", 17, 1.0, 0.0, 3.2), _player(11, "home-2", 11, 1.4, 0.0, 3.0)],
            [_player(10, "home-1", 17, 1.0, 0.0, 3.3), _player(11, "home-2", 11, 1.4, 0.0, 3.1)],
            [_player(10, "home-1", 17, 1.0, 0.0, 3.4), _player(11, "home-2", 11, 2.2, 0.0, 3.2)],
            [_player(10, "home-1", 17, 2.5, 0.0, 3.5), _player(11, "home-2", 11, 2.2, 0.0, 3.3)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
        ],
        "live": [True, True, True, True, True, True],
        "lastTouch": ["home", "home", "home", "home", "home", "home"],
    })
    if path.suffix == ".jsonl":
        raw.write_ndjson(path)
    else:
        raw.write_parquet(path)


def _write_speed_band_source(path: Path) -> None:
    raw = pl.DataFrame({
        "period": [1, 1, 1, 1, 1, 1],
        "frameIdx": [0, 1, 2, 3, 4, 5],
        "gameClock": [0.0, 0.04, 0.08, 0.12, 0.16, 0.20],
        "wallClock": [1000, 1040, 1080, 1120, 1160, 1200],
        "ball": [
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
            _ball(0.0, 0.0, 0.0),
        ],
        "homePlayers": [
            [_player(10, "home-1", 17, 0.5, 0.5, 0.1)],
            [_player(10, "home-1", 17, 0.5, 0.5, 0.6)],
            [_player(10, "home-1", 17, 0.5, 0.5, 2.0)],
            [_player(10, "home-1", 17, 0.5, 0.5, 4.0)],
            [_player(10, "home-1", 17, 0.5, 0.5, 6.0)],
            [_player(10, "home-1", 17, 0.5, 0.5, 7.5)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
            [_player(20, "away-1", 28, -8.0, -8.0, 0.0)],
        ],
        "live": [True, True, True, True, True, True],
        "lastTouch": ["home", "home", "home", "home", "home", "home"],
    })
    if path.suffix == ".jsonl":
        raw.write_ndjson(path)
    else:
        raw.write_parquet(path)


def _write_event_pattern_source(path: Path) -> None:
    raw = pl.DataFrame({
        "period": [1, 1, 1, 1, 1, 1],
        "frameIdx": [0, 1, 2, 3, 4, 5],
        "gameClock": [0.0, 0.04, 0.08, 0.12, 0.16, 0.20],
        "wallClock": [1000, 1040, 1080, 1120, 1160, 1200],
        "ball": [
            _ball(0.0, 0.0, 0.2),
            _ball(0.0, 0.0, 0.3),
            _ball(1.6, 0.0, 6.4),
            _ball(2.0, 0.0, 0.2),
            _ball(2.0, 0.0, 0.3),
            _ball(4.1, 0.0, 12.5),
        ],
        "homePlayers": [
            [_player(10, "home-1", 17, 0.7, 0.0, 3.0)],
            [_player(10, "home-1", 17, 0.1, 0.0, 3.1)],
            [_player(10, "home-1", 17, 0.1, 0.0, 3.2)],
            [_player(10, "home-1", 17, 2.5, 0.0, 3.3)],
            [_player(10, "home-1", 17, 2.1, 0.0, 3.4)],
            [_player(10, "home-1", 17, 2.1, 0.0, 3.5)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
        ],
        "live": [True, True, True, True, True, True],
        "lastTouch": ["home", "home", "home", "home", "home", "home"],
    })
    if path.suffix == ".jsonl":
        raw.write_ndjson(path)
    else:
        raw.write_parquet(path)


def _write_band_start_event_source(path: Path) -> None:
    raw = pl.DataFrame({
        "period": [1, 1, 1, 1, 1],
        "frameIdx": [0, 1, 2, 3, 4],
        "gameClock": [0.0, 0.04, 0.08, 0.12, 0.16],
        "wallClock": [1000, 1040, 1080, 1120, 1160],
        "ball": [
            _ball(0.0, 0.0, 0.2),
            _ball(0.0, 0.0, 0.3),
            _ball(0.0, 0.0, 0.4),
            _ball(0.0, 0.0, 6.5),
            _ball(0.0, 0.0, 6.8),
        ],
        "homePlayers": [
            [_player(10, "home-1", 17, 1.2, 0.0, 3.0)],
            [_player(10, "home-1", 17, 0.83, 0.0, 3.1)],
            [_player(10, "home-1", 17, 0.84, 0.0, 3.2)],
            [_player(10, "home-1", 17, 1.7, 0.0, 3.3)],
            [_player(10, "home-1", 17, 2.1, 0.0, 3.4)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
            [_player(20, "away-1", 28, -8.0, -8.0, 3.5)],
        ],
        "live": [True, True, True, True, True],
        "lastTouch": ["home", "home", "home", "home", "home"],
    })
    if path.suffix == ".jsonl":
        raw.write_ndjson(path)
    else:
        raw.write_parquet(path)


def test_pipeline_normalized_model_uses_metadata_pitch_dimensions(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    _write_tracking_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    assert df.columns == [
        "opta_match_id",
        "team",
        "period",
        "frame_id",
        "game_clock",
        "min_split",
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
        "player_speed_band",
        "ball_x",
        "ball_y",
        "ball_z",
        "ball_speed",
        "pitch_length",
        "pitch_width",
        "pitch_zone",
        "ball_zone",
        "frame_bucket",
        "player_ball_distance",
        "has_ball_possession",
        "event_type",
    ]
    assert abs(df.get_column("pitch_length").unique().item() - 99.998779296875) < 0.001
    assert abs(df.get_column("pitch_width").unique().item() - 64.99860382080078) < 0.001
    assert df.get_column("min_split").unique().to_list() == [5]
    assert sorted(df.get_column("frame_bucket").to_list()) == [0, 0, 1, 1]
    assert df.get_column("has_ball_possession").unique().to_list() == [False]
    assert sorted(df.get_column("player_speed_band").unique().to_list()) == [
        "jogging_moderate_running",
        "running",
    ]
    assert df.get_column("event_type").drop_nulls().to_list() == []


def test_pipeline_denormalized_model_adds_metadata_fields(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    _write_tracking_source(tracking_dir / "g123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="denormalized",
        config_path=config_path,
    ).run("123", save=False)

    assert df.columns[:15] == [
        "opta_match_id",
        "fixture",
        "match_date",
        "team",
        "period",
        "frame_id",
        "game_clock",
        "min_split",
        "wall_clock",
        "live",
        "last_touch",
        "opta_player_id",
        "player_id",
        "player_name",
        "player_position",
    ]
    home_row = df.filter((pl.col("team") == "home") & (pl.col("opta_player_id") == 10)).row(0, named=True)
    assert home_row["fixture"] == "FUL - WHU"
    assert str(home_row["match_date"]) == "2026-03-04"
    assert home_row["player_name"] == "A. Iwobi"
    assert home_row["player_position"] == "LW"


def test_pipeline_uses_configurable_helper_column_names_from_yaml(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("model: denormalized", "model: normalized")
        .replace("column_name: has_ball_possession", "column_name: is_in_possession")
        .replace("distance_m: 1.75", "distance_m: 2.5")
        .replace("min_frames: 5", "min_frames: 2")
        .replace("column_name: player_speed_band", "column_name: speed_band")
        .replace("column_name: player_ball_distance", "column_name: distance_to_ball_m")
        .replace("pitch_zone_column: pitch_zone", "pitch_zone_column: player_pitch_zone")
        .replace("ball_zone_column: ball_zone", "ball_zone_column: tracked_ball_zone"),
        encoding="utf-8",
    )
    pl.DataFrame({
        "period": [1, 1],
        "frameIdx": [0, 1],
        "gameClock": [0.0, 0.04],
        "wallClock": [1000, 1040],
        "ball": [_ball(0.0, 0.0, 0.0), _ball(1.0, 1.0, 1.2)],
        "homePlayers": [
            [_player(10, "home-1", 17, 0.5, 0.5, 4.0)],
            [_player(10, "home-1", 17, 1.5, 2.0, 4.5)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -3.0, -3.0, 3.5)],
            [_player(20, "away-1", 28, -3.0, -3.0, 3.8)],
        ],
        "live": [True, False],
        "lastTouch": ["home", "away"],
    }).write_parquet(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(storage="local", config_path=config_path).run("123", save=False)

    assert "distance_to_ball_m" in df.columns
    assert "is_in_possession" in df.columns
    assert "speed_band" in df.columns
    assert "player_pitch_zone" in df.columns
    assert "tracked_ball_zone" in df.columns
    assert "player_ball_distance" not in df.columns
    assert "player_speed_band" not in df.columns
    assert "has_ball_possession" not in df.columns
    assert "pitch_zone" not in df.columns
    assert "ball_zone" not in df.columns
    possession_by_player_frame = (
        df.select(["team", "frame_id", "is_in_possession"])
        .sort(["team", "frame_id"])
        .to_dicts()
    )
    assert possession_by_player_frame == [
        {"team": "away", "frame_id": 0, "is_in_possession": False},
        {"team": "away", "frame_id": 1, "is_in_possession": False},
        {"team": "home", "frame_id": 0, "is_in_possession": True},
        {"team": "home", "frame_id": 1, "is_in_possession": True},
    ]


def test_pipeline_can_disable_derived_columns_for_performance_management(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("    pitch_zone: true", "    pitch_zone: false")
        .replace("    ball_zone: true", "    ball_zone: false")
        .replace("    frame_bucket: true", "    frame_bucket: false")
        .replace("    min_split: true", "    min_split: false")
        .replace("    player_ball_distance: true", "    player_ball_distance: false")
        .replace("    has_ball_possession: true", "    has_ball_possession: false")
        .replace("    player_speed_band: true", "    player_speed_band: false")
        .replace("    event_type: true", "    event_type: 'false'"),
        encoding="utf-8",
    )
    _write_tracking_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    cfg = load_runtime_storage_config(config_path)
    assert cfg.warehouse_sort_columns == (
        "opta_match_id",
        "period",
        "team",
        "frame_id",
        "player_id",
    )

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    assert df.columns == [
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
    assert "event_type" not in df.columns
    assert "frame_bucket" not in df.columns
    assert "player_ball_distance" not in df.columns
    assert "has_ball_possession" not in df.columns
    assert "player_speed_band" not in df.columns
    assert "pitch_zone" not in df.columns
    assert "ball_zone" not in df.columns
    assert "min_split" not in df.columns


def test_pipeline_requires_five_consecutive_close_frames_for_possession(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    _write_sustained_possession_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="denormalized",
        config_path=config_path,
    ).run("123", save=False)

    home_1 = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-1"))
        .select(["frame_id", "player_ball_distance", "has_ball_possession"])
        .sort("frame_id")
        .to_dicts()
    )
    home_2 = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-2"))
        .select(["frame_id", "player_ball_distance", "has_ball_possession"])
        .sort("frame_id")
        .to_dicts()
    )

    assert [row["has_ball_possession"] for row in home_1] == [True, True, True, True, True, False]
    assert [row["has_ball_possession"] for row in home_2] == [False, False, False, False, False, False]
    assert all(float(row["player_ball_distance"]) <= 1.75 for row in home_1[:5])
    assert all(float(row["player_ball_distance"]) <= 1.75 for row in home_2[:4])


def test_pipeline_uses_configurable_speed_band_thresholds_from_yaml(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("standing_max_m_s: 0.2", "standing_max_m_s: 0.3")
        .replace("walking_max_m_s: 2.0", "walking_max_m_s: 1.0")
        .replace("jogging_max_m_s: 4.0", "jogging_max_m_s: 3.0")
        .replace("running_max_m_s: 5.5", "running_max_m_s: 4.5")
        .replace("high_speed_running_max_m_s: 7.0", "high_speed_running_max_m_s: 6.5"),
        encoding="utf-8",
    )
    _write_speed_band_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    home_bands = (
        df.filter(pl.col("team") == "home")
        .select(["frame_id", "player_speed", "player_speed_band"])
        .sort("frame_id")
        .to_dicts()
    )
    assert home_bands == [
        {"frame_id": 0, "player_speed": 0.1, "player_speed_band": "standing"},
        {"frame_id": 1, "player_speed": 0.6, "player_speed_band": "walking_low_speed"},
        {"frame_id": 2, "player_speed": 2.0, "player_speed_band": "jogging_moderate_running"},
        {"frame_id": 3, "player_speed": 4.0, "player_speed_band": "running"},
        {"frame_id": 4, "player_speed": 6.0, "player_speed_band": "high_speed_running"},
        {"frame_id": 5, "player_speed": 7.5, "player_speed_band": "sprinting"},
    ]


def test_pipeline_detects_configured_pass_and_shot_patterns(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    _write_event_pattern_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    detected = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-1"))
        .select(["frame_id", "event_type"])
        .sort("frame_id")
        .to_dicts()
    )
    assert detected == [
        {"frame_id": 0, "event_type": None},
        {"frame_id": 1, "event_type": "pass"},
        {"frame_id": 2, "event_type": None},
        {"frame_id": 3, "event_type": None},
        {"frame_id": 4, "event_type": "shot"},
        {"frame_id": 5, "event_type": None},
    ]


def test_pipeline_assigns_event_to_last_distance_band_frame_before_release(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("closest_frame: 0.2", "closest_frame: 0.85\n    persist_frames: 2")
        .replace("    pass:", "    pass/shot/clearance:")
        .replace("      frame_sequence: 5", "      frame_sequence: 4", 1)
        .replace("      leaving_distance: 1.2", "      start_distance_min: 0.8\n      start_distance_max: 0.85\n      leaving_distance: 1.5", 1)
        .replace("      speed: 5.2", "      min_speed: 5.2", 1)
        .replace("    shot:\n      frame_sequence: 5\n      leaving_distance: 1.5\n      speed: 11.2\n", ""),
        encoding="utf-8",
    )
    _write_band_start_event_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    detected = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-1"))
        .select(["frame_id", "event_type"])
        .sort("frame_id")
        .to_dicts()
    )
    assert detected == [
        {"frame_id": 0, "event_type": None},
        {"frame_id": 1, "event_type": None},
        {"frame_id": 2, "event_type": "pass/shot/clearance"},
        {"frame_id": 3, "event_type": "pass/shot/clearance"},
        {"frame_id": 4, "event_type": None},
    ]


def test_pipeline_supports_dynamic_event_names_and_column_name_from_yaml(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("column_name: event_type", "column_name: detected_event")
        .replace("    pass:", "    release:")
        .replace("    shot:", "    finish:"),
        encoding="utf-8",
    )
    _write_event_pattern_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    assert "detected_event" in df.columns
    assert "event_type" not in df.columns
    detected = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-1"))
        .select(["frame_id", "detected_event"])
        .sort("frame_id")
        .to_dicts()
    )
    assert detected == [
        {"frame_id": 0, "detected_event": None},
        {"frame_id": 1, "detected_event": "release"},
        {"frame_id": 2, "detected_event": None},
        {"frame_id": 3, "detected_event": None},
        {"frame_id": 4, "detected_event": "finish"},
        {"frame_id": 5, "detected_event": None},
    ]


def test_pipeline_persists_detected_event_labels_for_configured_frames(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("frame_sequence: 5", "frame_sequence: 3", 1)
        .replace("frame_sequence: 5", "frame_sequence: 3", 1)
        .replace("closest_frame: 0.2", "closest_frame: 0.2\n    persist_frames: 3"),
        encoding="utf-8",
    )
    _write_event_pattern_source(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    detected = (
        df.filter((pl.col("team") == "home") & (pl.col("player_id") == "home-1"))
        .select(["frame_id", "event_type"])
        .sort("frame_id")
        .to_dicts()
    )
    assert detected == [
        {"frame_id": 0, "event_type": None},
        {"frame_id": 1, "event_type": "pass"},
        {"frame_id": 2, "event_type": "pass"},
        {"frame_id": 3, "event_type": "pass"},
        {"frame_id": 4, "event_type": "shot"},
        {"frame_id": 5, "event_type": "shot"},
    ]


def test_event_pattern_config_accepts_min_speed_alias_and_pass_shot_clearance_label(tmp_path: Path) -> None:
    config_path, _, _ = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("    pass:", "    pass/shot/clearance:")
        .replace("      speed: 5.2", "      min_speed: 5.2"),
        encoding="utf-8",
    )

    cfg = load_runtime_storage_config(config_path)

    labels = [rule.name for rule in cfg.event_pattern.events]
    speeds = {rule.name: rule.speed_m_s for rule in cfg.event_pattern.events}
    assert "pass/shot/clearance" in labels
    assert speeds["pass/shot/clearance"] == 5.2


def test_pipeline_adds_min_split_for_time_analysis(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    raw = pl.DataFrame({
        "period": [1, 1],
        "frameIdx": [0, 7500],
        "gameClock": [10.0, 320.0],
        "wallClock": [1000, 321000],
        "ball": [_ball(0.0, 0.0, 0.0), _ball(1.0, 1.0, 1.2)],
        "homePlayers": [
            [_player(10, "home-1", 17, 0.5, 0.5, 4.0)],
            [_player(10, "home-1", 17, 2.0, 3.0, 4.5)],
        ],
        "awayPlayers": [
            [_player(20, "away-1", 28, -1.0, -2.0, 3.5)],
            [_player(20, "away-1", 28, -2.0, -3.0, 3.8)],
        ],
        "live": [True, False],
        "lastTouch": ["home", "away"],
    })
    raw.write_parquet(tracking_dir / "123.parquet")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run("123", save=False)

    buckets = (
        df.select(["frame_id", "min_split"])
        .unique()
        .sort("frame_id")
        .to_dicts()
    )
    assert buckets == [
        {"frame_id": 0, "min_split": 5},
        {"frame_id": 7500, "min_split": 10},
    ]


def test_storage_profile_passes_storage_options_to_fsspec(tmp_path: Path, monkeypatch) -> None:
    config_path, _, metadata_dir = _write_config(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace(f"  export_to: '{tmp_path / 'curated'}'", "  export_to: 's3://example-bucket/curated'")
        .replace(f"  metadata_from: '{metadata_dir}'", "  metadata_from: 's3://example-bucket/metadata'"),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_url_to_fs(uri: str, **kwargs):
        captured["uri"] = uri
        captured["kwargs"] = kwargs
        return object(), "example-bucket/tracking"

    monkeypatch.setattr(fsspec.core, "url_to_fs", fake_url_to_fs)

    profile = load_storage_profile(config_path=config_path, storage="aws_s3")
    manager = StorageManager(profile)
    manager._filesystem(profile.read_from)

    assert captured["uri"] == "s3://example-bucket/tracking"
    assert captured["kwargs"] == {
        "key": "test-key",
        "secret": "test-secret",
        "client_kwargs": {"region_name": "eu-west-2"},
    }


def test_run_many_accepts_match_id_list_and_numeric_filenames(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    _write_tracking_source(tracking_dir / "123.parquet")
    _write_tracking_source(tracking_dir / "124.parquet")
    _write_metadata(metadata_dir, 123)
    _write_metadata(metadata_dir, 124)

    df = TrackingPipeline(
        storage="local",
        model="normalized",
        config_path=config_path,
    ).run([123, "124.parquet"], save=False)

    assert sorted(df.get_column("opta_match_id").unique().to_list()) == [123, 124]


def test_run_many_save_exports_to_curated_directory(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    curated_dir = tmp_path / "curated"
    _write_tracking_source(tracking_dir / "123.parquet")
    _write_tracking_source(tracking_dir / "124.parquet")
    _write_metadata(metadata_dir, 123)
    _write_metadata(metadata_dir, 124)

    pipeline = TrackingPipeline(
        storage="local",
        config_path=config_path,
    )
    summary = pipeline.run()

    assert summary.get_column("rows").to_list() == [8]
    assert (curated_dir / "123_processed.parquet").exists()
    assert (curated_dir / "124_processed.parquet").exists()
    assert (curated_dir / "tracking_compacted_batch_001.parquet").exists()

    log_df = pipeline.query_logs("""
        SELECT task
        FROM performance_logs
        ORDER BY logged_at_utc, task
    """)
    assert "load_match_metadata" in log_df.get_column("task").to_list()
    assert "save_lazy_match" in log_df.get_column("task").to_list()
    assert "compact_batch" in log_df.get_column("task").to_list()


def test_run_many_save_streams_jsonl_without_building_source_parquet(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    curated_dir = tmp_path / "curated"
    _write_tracking_source(tracking_dir / "123.jsonl")
    _write_tracking_source(tracking_dir / "124.jsonl")
    _write_metadata(metadata_dir, 123)
    _write_metadata(metadata_dir, 124)

    pipeline = TrackingPipeline(storage="local", config_path=config_path)
    summary = pipeline.run()

    assert summary.get_column("rows").to_list() == [8]
    assert not (tracking_dir / "123.parquet").exists()
    assert not (tracking_dir / "124.parquet").exists()
    assert (curated_dir / "tracking_compacted_batch_001.parquet").exists()

    log_df = pipeline.query_logs("""
        SELECT task, source_status
        FROM performance_logs
        ORDER BY logged_at_utc, task
    """)
    assert "resolve_tracking_source" in log_df.get_column("task").to_list()
    assert "streamed_jsonl" in log_df.get_column("source_status").drop_nulls().to_list()


def test_denormalized_join_casts_string_opta_player_ids_to_int(tmp_path: Path) -> None:
    config_path, tracking_dir, metadata_dir = _write_config(tmp_path)
    raw = pl.DataFrame({
        "period": [1],
        "frameIdx": [0],
        "gameClock": [0.0],
        "wallClock": [1000],
        "ball": [_ball(0.0, 0.0, 0.0)],
        "homePlayers": [[_player("10", "home-1", 17, 0.5, 0.5, 4.0)]],
        "awayPlayers": [[_player("20", "away-1", 28, -1.0, -2.0, 3.5)]],
        "live": [True],
        "lastTouch": ["home"],
    })
    raw.write_ndjson(tracking_dir / "123.jsonl")
    _write_metadata(metadata_dir, 123)

    df = TrackingPipeline(
        storage="local",
        model="denormalized",
        config_path=config_path,
    ).run("123", save=False)

    player_names = {
        row["opta_player_id"]: row["player_name"]
        for row in df.select(["opta_player_id", "player_name"]).to_dicts()
    }
    assert player_names == {
        10: "A. Iwobi",
        20: "T. Soucek",
    }
