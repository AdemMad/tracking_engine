from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from tracking_engine.config import load_runtime_storage_config
from tracking_engine.storage import DEFAULT_CONFIG_PATH, load_storage_profile


@dataclass(frozen=True)
class FocusSelector:
    field_name: str
    value: str

    @property
    def label(self) -> str:
        return f"{self.field_name}={self.value}"

    def matches(self, row: dict[str, Any]) -> bool:
        candidate = row.get(self.field_name)
        if candidate is None:
            return False
        if self.field_name == "player_name":
            return str(candidate).strip().casefold() == self.value.strip().casefold()
        return str(candidate) == self.value


@dataclass(frozen=True)
class FrameSnapshot:
    fixture: str | None
    match_date: str | None
    period: int
    frame_id: int
    game_clock: float | None
    wall_clock: int | None
    live: bool
    last_touch: str | None
    ball_x: float
    ball_y: float
    ball_z: float | None
    ball_speed: float | None
    rows: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Animate one processed tracking match in 2D so you can visually validate "
            "ball distance and possession frame by frame."
        )
    )
    parser.add_argument("match_id", type=int, help="Opta match id to animate, for example 2562179.")
    parser.add_argument(
        "--config-path",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to config.yaml. Defaults to ./config.yaml.",
    )
    parser.add_argument(
        "--storage",
        default="local",
        help="Storage profile from config.yaml used to resolve the curated export directory.",
    )
    parser.add_argument(
        "--parquet",
        default=None,
        help="Optional parquet file, directory, or glob override instead of using config.yaml export_to.",
    )
    parser.add_argument("--period", type=int, default=1, help="Match period to animate. Defaults to 1.")
    parser.add_argument(
        "--clock-start",
        "--game-clock-start",
        dest="clock_start",
        type=float,
        default=None,
        help="Optional game_clock start in seconds, for example 0.",
    )
    parser.add_argument(
        "--clock-end",
        "--game-clock-end",
        dest="clock_end",
        type=float,
        default=None,
        help="Optional game_clock end in seconds, for example 20.",
    )
    parser.add_argument(
        "--frame-start",
        type=int,
        default=0,
        help="First frame_id to include when no game_clock window is supplied. Defaults to 0.",
    )
    parser.add_argument(
        "--frame-end",
        type=int,
        default=None,
        help="Last frame_id to include when no game_clock window is supplied.",
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=250,
        help="Maximum frame window when frame-end is omitted and no game_clock window is supplied. Defaults to 250.",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=1,
        help="Animate every Nth frame. Defaults to 1.",
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=12,
        help="Playback speed for the animation. Defaults to 12 fps.",
    )
    parser.add_argument(
        "--only-live",
        action="store_true",
        help="Keep only rows where live=True.",
    )
    parser.add_argument(
        "--show-player-labels",
        "--show-numbers",
        dest="show_numbers",
        action="store_true",
        help="Start the replay with player shirt numbers and name labels visible.",
    )
    parser.add_argument(
        "--ball-tail",
        type=int,
        default=8,
        help="How many previous ball positions to leave as a trail. Defaults to 8.",
    )
    parser.add_argument(
        "--focus-tail",
        type=int,
        default=8,
        help="How many previous focus-player positions to leave as a trail. Defaults to 8.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path. Defaults to a self-contained HTML replay if omitted.",
    )
    focus_group = parser.add_mutually_exclusive_group()
    focus_group.add_argument(
        "--focus-opta-player-id",
        "--focus-opta-id",
        type=int,
        dest="focus_opta_player_id",
        default=None,
        help="Highlight one player by opta_player_id.",
    )
    focus_group.add_argument(
        "--focus-player-id",
        default=None,
        help="Highlight one player by player_id.",
    )
    focus_group.add_argument(
        "--focus-player-name",
        default=None,
        help="Highlight one player by exact player_name match.",
    )
    return parser.parse_args()


def resolve_focus_selector(args: argparse.Namespace) -> FocusSelector | None:
    if args.focus_opta_player_id is not None:
        return FocusSelector("opta_player_id", str(args.focus_opta_player_id))
    if args.focus_player_id:
        return FocusSelector("player_id", args.focus_player_id)
    if args.focus_player_name:
        return FocusSelector("player_name", args.focus_player_name)
    return None


def default_frame_end(frame_start: int, frame_end: int | None, max_frames: int, step: int) -> int:
    if frame_end is not None:
        return frame_end
    safe_step = max(1, step)
    safe_max_frames = max(1, max_frames)
    return frame_start + (safe_max_frames * safe_step) - 1


def resolve_parquet_inputs(config_path: str | Path, storage: str, parquet_override: str | None) -> list[str]:
    if parquet_override:
        raw = Path(parquet_override)
        if raw.is_dir():
            compacted = sorted(raw.glob("tracking_compacted*.parquet"))
            if compacted:
                return [path.as_posix() for path in compacted]
            processed = sorted(raw.glob("*_processed.parquet"))
            if processed:
                return [path.as_posix() for path in processed]
            return [path.as_posix() for path in sorted(raw.glob("*.parquet"))]
        return [parquet_override]

    profile = load_storage_profile(config_path=config_path, storage=storage)
    if "://" in profile.export_to:
        raise ValueError(
            "test_video.py expects a local curated parquet directory. "
            "Use --parquet to point at a local file or directory."
        )

    export_dir = Path(profile.export_to)
    if not export_dir.exists():
        raise FileNotFoundError(f"Curated parquet directory not found: {export_dir}")

    compacted = sorted(export_dir.glob("tracking_compacted*.parquet"))
    if compacted:
        return [path.as_posix() for path in compacted]

    processed = sorted(export_dir.glob("*_processed.parquet"))
    if processed:
        return [path.as_posix() for path in processed]

    parquet_files = sorted(export_dir.glob("*.parquet"))
    if parquet_files:
        return [path.as_posix() for path in parquet_files]

    raise FileNotFoundError(f"No parquet files found in {export_dir}")


def canonical_columns(cfg, schema_names: set[str]) -> list[pl.Expr]:
    distance_col = cfg.player_ball_distance_column
    possession_col = cfg.has_ball_possession_column
    opta_player_column = "opta_player_id" if "opta_player_id" in schema_names else "opta_id"

    expressions: list[pl.Expr] = [
        pl.col("opta_match_id"),
        optional_column(schema_names, "fixture", pl.String),
        optional_column(schema_names, "match_date", pl.String),
        pl.col("team"),
        pl.col("period"),
        pl.col("frame_id"),
        optional_column(schema_names, "game_clock", pl.Float64),
        optional_column(schema_names, "wall_clock", pl.Int64),
        optional_column(schema_names, "live", pl.Boolean, default=False),
        optional_column(schema_names, "last_touch", pl.String),
        optional_column(schema_names, opta_player_column, pl.Int64).alias("opta_player_id"),
        optional_column(schema_names, "player_id", pl.String),
        optional_column(schema_names, "player_name", pl.String),
        optional_column(schema_names, "player_position", pl.String),
        optional_column(schema_names, "player_number", pl.Int16),
        pl.col("player_x").cast(pl.Float64),
        pl.col("player_y").cast(pl.Float64),
        optional_column(schema_names, "player_z", pl.Float64),
        optional_column(schema_names, "player_speed", pl.Float64),
        optional_column(schema_names, cfg.player_speed_band_column, pl.String).alias("player_speed_band"),
        pl.col("ball_x").cast(pl.Float64),
        pl.col("ball_y").cast(pl.Float64),
        optional_column(schema_names, "ball_z", pl.Float64),
        optional_column(schema_names, "ball_speed", pl.Float64),
        optional_column(schema_names, "pitch_length", pl.Float64, default=cfg.length),
        optional_column(schema_names, "pitch_width", pl.Float64, default=cfg.width),
        optional_column(schema_names, cfg.pitch_zone_column, pl.String).alias("pitch_zone"),
        optional_column(schema_names, cfg.ball_zone_column, pl.String).alias("ball_zone"),
        optional_column(schema_names, "frame_bucket", pl.Int32),
        optional_column(schema_names, distance_col, pl.Float64).alias("player_ball_distance"),
        optional_column(schema_names, possession_col, pl.Boolean, default=False).alias("has_ball_possession"),
        optional_column(schema_names, cfg.event_type_column, pl.String).alias("event_type"),
    ]
    return expressions


def optional_column(
    schema_names: set[str],
    name: str,
    dtype: pl.DataType,
    *,
    default: Any = None,
) -> pl.Expr:
    if name in schema_names:
        return pl.col(name)
    return pl.lit(default, dtype=dtype).alias(name)


def load_tracking_window(args: argparse.Namespace) -> tuple[pl.DataFrame, float, float]:
    cfg = load_runtime_storage_config(args.config_path)
    parquet_inputs = resolve_parquet_inputs(args.config_path, args.storage, args.parquet)

    scan = pl.scan_parquet(parquet_inputs)
    schema_names = set(scan.collect_schema().names())
    if "opta_match_id" not in schema_names:
        raise ValueError("Expected curated parquet files with an opta_match_id column.")

    filtered = scan.filter(
        (pl.col("opta_match_id") == args.match_id)
        & (pl.col("period") == args.period)
    )
    if args.clock_start is not None:
        filtered = filtered.filter(pl.col("game_clock") >= args.clock_start)
    if args.clock_end is not None:
        filtered = filtered.filter(pl.col("game_clock") <= args.clock_end)
    if args.clock_start is None and args.clock_end is None:
        frame_end = default_frame_end(args.frame_start, args.frame_end, args.max_frames, args.step)
        filtered = filtered.filter(
            (pl.col("frame_id") >= args.frame_start)
            & (pl.col("frame_id") <= frame_end)
        )
    if args.only_live and "live" in schema_names:
        filtered = filtered.filter(pl.col("live"))

    df = (
        filtered
        .select(canonical_columns(cfg, schema_names))
        .sort(["period", "frame_id", "team", "player_number", "player_id"])
        .collect()
    )
    if df.is_empty():
        raise FileNotFoundError(
            "No curated tracking rows found for the selected match/period/time window. "
            "Try widening the frame range or pointing --parquet at the right dataset."
        )

    if args.step > 1:
        frame_ids = (
            df.select("frame_id")
            .unique()
            .sort("frame_id")
            .get_column("frame_id")
            .to_list()
        )
        keep = set(frame_ids[:: args.step])
        df = df.filter(pl.col("frame_id").is_in(list(keep)))

    pitch_length = first_non_null(df.get_column("pitch_length"), cfg.length)
    pitch_width = first_non_null(df.get_column("pitch_width"), cfg.width)
    return df, pitch_length, pitch_width


def first_non_null(series: pl.Series, fallback: float) -> float:
    values = series.drop_nulls().to_list()
    if not values:
        return float(fallback)
    return float(values[0])


def build_snapshots(df: pl.DataFrame) -> list[FrameSnapshot]:
    snapshots: list[FrameSnapshot] = []
    bucket: list[dict[str, Any]] = []
    current_key: tuple[int, int] | None = None

    for row in df.iter_rows(named=True):
        key = (int(row["period"]), int(row["frame_id"]))
        if current_key is None:
            current_key = key
        if key != current_key:
            snapshots.append(snapshot_from_rows(bucket))
            bucket = []
            current_key = key
        bucket.append(row)

    if bucket:
        snapshots.append(snapshot_from_rows(bucket))
    return snapshots


def snapshot_from_rows(rows: list[dict[str, Any]]) -> FrameSnapshot:
    first = rows[0]
    return FrameSnapshot(
        fixture=str(first["fixture"]) if first.get("fixture") is not None else None,
        match_date=str(first["match_date"]) if first.get("match_date") is not None else None,
        period=int(first["period"]),
        frame_id=int(first["frame_id"]),
        game_clock=to_float(first.get("game_clock")),
        wall_clock=to_int(first.get("wall_clock")),
        live=bool(first.get("live") or False),
        last_touch=str(first["last_touch"]) if first.get("last_touch") is not None else None,
        ball_x=to_float(first.get("ball_x"), 0.0),
        ball_y=to_float(first.get("ball_y"), 0.0),
        ball_z=to_float(first.get("ball_z")),
        ball_speed=to_float(first.get("ball_speed")),
        rows=rows,
    )


def to_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    return float(value)


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def focus_row(rows: list[dict[str, Any]], selector: FocusSelector | None) -> dict[str, Any] | None:
    if selector is None:
        return nearest_player_row(rows)
    for row in rows:
        if selector.matches(row):
            return row
    return None


def nearest_player_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [row for row in rows if row.get("player_ball_distance") is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda row: float(row["player_ball_distance"]))


def validate_focus_selector(snapshots: list[FrameSnapshot], selector: FocusSelector | None) -> None:
    if selector is None:
        return
    for snapshot in snapshots:
        if focus_row(snapshot.rows, selector) is not None:
            return
    available = sorted({
        str(row["player_name"])
        for snapshot in snapshots
        for row in snapshot.rows
        if row.get("player_name") is not None
    })
    preview = ", ".join(available[:12]) if available else "no named players found"
    raise ValueError(f"Could not find {selector.label} in the selected window. Available names: {preview}")


def print_focus_transitions(snapshots: list[FrameSnapshot], selector: FocusSelector | None) -> None:
    if selector is None:
        return

    print(f"Tracking focus player: {selector.label}")
    previous_state: bool | None = None
    for snapshot in snapshots:
        row = focus_row(snapshot.rows, selector)
        if row is None:
            continue
        current_state = bool(row.get("has_ball_possession") or False)
        if previous_state is None or current_state != previous_state:
            print(
                "  "
                f"period={snapshot.period} frame={snapshot.frame_id} "
                f"clock={format_clock(snapshot.game_clock)} "
                f"distance_to_ball_m={format_metric(row.get('player_ball_distance'))} "
                f"has_ball_possession={current_state}"
            )
            previous_state = current_state


def format_metric(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"


def format_clock(value: float | None) -> str:
    if value is None:
        return "n/a"
    total_seconds = max(0.0, value)
    minutes = int(total_seconds // 60)
    seconds = total_seconds - (minutes * 60)
    return f"{minutes:02d}:{seconds:05.2f}"


def draw_pitch(ax, pitch_length: float, pitch_width: float) -> None:
    from matplotlib.patches import Circle, Rectangle

    x_min = -pitch_length / 2
    x_max = pitch_length / 2
    y_min = -pitch_width / 2
    y_max = pitch_width / 2

    ax.set_facecolor("#2E8B57")
    ax.add_patch(Rectangle((x_min, y_min), pitch_length, pitch_width, fill=False, ec="white", lw=2))
    ax.plot([0, 0], [y_min, y_max], color="white", lw=2)
    ax.add_patch(Circle((0, 0), 9.15, fill=False, ec="white", lw=2))
    ax.add_patch(Circle((0, 0), 0.3, color="white"))

    penalty_depth = 16.5
    six_yard_depth = 5.5
    penalty_width = 40.32
    six_yard_width = 18.32

    ax.add_patch(Rectangle((x_min, -penalty_width / 2), penalty_depth, penalty_width, fill=False, ec="white", lw=2))
    ax.add_patch(Rectangle((x_max - penalty_depth, -penalty_width / 2), penalty_depth, penalty_width, fill=False, ec="white", lw=2))
    ax.add_patch(Rectangle((x_min, -six_yard_width / 2), six_yard_depth, six_yard_width, fill=False, ec="white", lw=2))
    ax.add_patch(Rectangle((x_max - six_yard_depth, -six_yard_width / 2), six_yard_depth, six_yard_width, fill=False, ec="white", lw=2))
    ax.add_patch(Circle((x_min + 11, 0), 0.3, color="white"))
    ax.add_patch(Circle((x_max - 11, 0), 0.3, color="white"))

    ax.set_xlim(x_min - 3, x_max + 3)
    ax.set_ylim(y_min - 3, y_max + 6)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])


def animate_match(
    snapshots: list[FrameSnapshot],
    pitch_length: float,
    pitch_width: float,
    *,
    match_id: int,
    selector: FocusSelector | None,
    show_numbers: bool,
    fps: int,
    ball_tail: int,
    focus_tail: int,
    output_path: str | None,
) -> None:
    try:
        import matplotlib.pyplot as plt
        from matplotlib.animation import FuncAnimation
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required for test_video.py. Install it with "
            "`pip install -e .[viz]` or `pip install matplotlib pillow`."
        ) from exc

    fig, ax = plt.subplots(figsize=(12, 8))
    home_color = "#1f77b4"
    away_color = "#d62728"
    ball_color = "#111111"
    focus_color = "#ffd166"

    def update(frame_index: int):
        snapshot = snapshots[frame_index]
        ax.clear()
        draw_pitch(ax, pitch_length, pitch_width)

        home_rows = [row for row in snapshot.rows if str(row.get("team")) == "home"]
        away_rows = [row for row in snapshot.rows if str(row.get("team")) == "away"]
        possession_rows = [row for row in snapshot.rows if bool(row.get("has_ball_possession") or False)]
        current_focus = focus_row(snapshot.rows, selector)
        reference_row = current_focus if current_focus is not None else nearest_player_row(snapshot.rows)

        plot_team(ax, home_rows, home_color, "Home")
        plot_team(ax, away_rows, away_color, "Away")

        if possession_rows:
            ax.scatter(
                [float(row["player_x"]) for row in possession_rows],
                [float(row["player_y"]) for row in possession_rows],
                s=260,
                facecolors="none",
                edgecolors=focus_color,
                linewidths=2.0,
                zorder=5,
                label="has_ball_possession",
            )

        ball_positions = snapshots[max(0, frame_index - max(1, ball_tail) + 1): frame_index + 1]
        ax.plot(
            [frame.ball_x for frame in ball_positions],
            [frame.ball_y for frame in ball_positions],
            color="#f4a261",
            lw=2.0,
            alpha=0.8,
            zorder=6,
        )
        ax.scatter([snapshot.ball_x], [snapshot.ball_y], c=ball_color, s=90, zorder=7, label="Ball")

        if reference_row is not None:
            rx = float(reference_row["player_x"])
            ry = float(reference_row["player_y"])
            ax.scatter([rx], [ry], c=focus_color, s=220, edgecolors="black", linewidths=1.5, zorder=8)
            ax.plot([rx, snapshot.ball_x], [ry, snapshot.ball_y], color=focus_color, lw=1.8, ls="--", zorder=7)

        if selector is not None:
            focus_positions = focus_history(snapshots, frame_index, selector, focus_tail)
            if len(focus_positions) >= 2:
                ax.plot(
                    [point[0] for point in focus_positions],
                    [point[1] for point in focus_positions],
                    color=focus_color,
                    lw=2.0,
                    alpha=0.75,
                    zorder=6,
                )

        if show_numbers:
            annotate_player_numbers(ax, home_rows, away_rows)

        title = (
            f"{snapshot.fixture or f'Match {match_id}'}"
            f" | {snapshot.match_date or 'n/a'}"
            f" | period {snapshot.period}"
            f" | frame {snapshot.frame_id}"
            f" | clock {format_clock(snapshot.game_clock)}"
        )
        ax.set_title(title, fontsize=13, color="white", pad=12)

        overlay_lines = [
            f"live={snapshot.live}  last_touch={snapshot.last_touch or 'n/a'}  ball_speed_mps={format_metric(snapshot.ball_speed)}",
            f"ball=({snapshot.ball_x:.2f}, {snapshot.ball_y:.2f})  ball_z={format_metric(snapshot.ball_z)}",
        ]
        if reference_row is not None:
            reference_name = (
                reference_row.get("player_name")
                or reference_row.get("player_id")
                or reference_row.get("opta_player_id")
            )
            overlay_lines.append(
                f"focus={reference_name}  team={reference_row.get('team')}  "
                f"distance_to_ball_m={format_metric(reference_row.get('player_ball_distance'))}  "
                f"has_ball_possession={bool(reference_row.get('has_ball_possession') or False)}"
            )

        ax.text(
            -pitch_length / 2,
            pitch_width / 2 + 3.2,
            "\n".join(overlay_lines),
            ha="left",
            va="bottom",
            color="white",
            fontsize=10,
            bbox={"facecolor": "#123524", "alpha": 0.85, "edgecolor": "none", "pad": 8},
        )
        ax.legend(loc="upper right")

    animation = FuncAnimation(
        fig,
        update,
        frames=len(snapshots),
        interval=1000 / max(1, fps),
        repeat=False,
        blit=False,
    )

    if output_path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix.lower() == ".gif":
            animation.save(output, writer="pillow", fps=max(1, fps))
        else:
            animation.save(output, fps=max(1, fps))
        print(f"Saved animation to {output}")
        plt.close(fig)
        return

    plt.show()


def default_output_path(
    match_id: int,
    period: int,
    selector: FocusSelector | None,
    explicit_output: str | None,
    *,
    clock_start: float | None,
    clock_end: float | None,
) -> str:
    if explicit_output:
        return explicit_output

    suffix = ""
    if clock_start is not None or clock_end is not None:
        suffix += f"_clock_{slugify(format_window_value(clock_start, 'start'))}_{slugify(format_window_value(clock_end, 'end'))}"
    if selector is not None:
        suffix += f"_{slugify(selector.field_name)}_{slugify(selector.value)}"
    return str(Path("replays") / f"match_{match_id}_period_{period}{suffix}.html")


def slugify(value: str) -> str:
    safe = []
    for char in value:
        if char.isalnum():
            safe.append(char.lower())
        else:
            safe.append("_")
    slug = "".join(safe).strip("_")
    return slug or "focus"


def format_window_value(value: float | None, fallback: str) -> str:
    if value is None:
        return fallback
    if float(value).is_integer():
        return str(int(value))
    return str(value).replace(".", "_")


def describe_window(args: argparse.Namespace) -> str:
    if args.clock_start is not None or args.clock_end is not None:
        return (
            "game_clock="
            f"{format_window_value(args.clock_start, 'start')}s.."
            f"{format_window_value(args.clock_end, 'end')}s"
        )
    frame_end = default_frame_end(args.frame_start, args.frame_end, args.max_frames, args.step)
    return f"frame_id={args.frame_start}..{frame_end}"


def build_timeline_seconds(snapshots: list[FrameSnapshot], fallback_fps: int) -> list[float]:
    if not snapshots:
        return []

    clock_values = [snapshot.game_clock for snapshot in snapshots]
    if all(value is not None for value in clock_values):
        return normalise_timeline([float(value) for value in clock_values], fallback_fps)

    wall_values = [snapshot.wall_clock for snapshot in snapshots]
    if all(value is not None for value in wall_values):
        base = float(wall_values[0])
        return normalise_timeline([(float(value) - base) / 1000.0 for value in wall_values], fallback_fps)

    step_s = 1.0 / max(1, fallback_fps)
    return [index * step_s for index in range(len(snapshots))]


def normalise_timeline(values: list[float], fallback_fps: int) -> list[float]:
    base = values[0]
    min_step = 1.0 / max(1, fallback_fps)
    normalised: list[float] = []
    previous = -min_step
    for raw_value in values:
        current = max(0.0, raw_value - base)
        if current <= previous:
            current = previous + min_step
        normalised.append(current)
        previous = current
    return normalised


def build_replay_payload(
    snapshots: list[FrameSnapshot],
    pitch_length: float,
    pitch_width: float,
    *,
    cfg,
    visible_zones: list[str],
    match_id: int,
    selector: FocusSelector | None,
    show_numbers: bool,
    fallback_fps: int,
    ball_tail: int,
    focus_tail: int,
) -> dict[str, Any]:
    times = build_timeline_seconds(snapshots, fallback_fps)
    title = snapshots[0].fixture or f"Match {match_id}"

    frames: list[dict[str, Any]] = []
    for snapshot, time_s in zip(snapshots, times, strict=False):
        current_focus = focus_row(snapshot.rows, selector)
        reference_row = current_focus if current_focus is not None else nearest_player_row(snapshot.rows)
        frames.append({
            "fixture": snapshot.fixture or title,
            "match_date": snapshot.match_date,
            "period": snapshot.period,
            "frame_id": snapshot.frame_id,
            "game_clock": snapshot.game_clock,
            "wall_clock": snapshot.wall_clock,
            "live": snapshot.live,
            "last_touch": snapshot.last_touch,
            "time_s": time_s,
            "ball": {
                "x": snapshot.ball_x,
                "y": snapshot.ball_y,
                "z": snapshot.ball_z,
                "speed": snapshot.ball_speed,
            },
            "home": [serialise_player(row) for row in snapshot.rows if str(row.get("team")) == "home"],
            "away": [serialise_player(row) for row in snapshot.rows if str(row.get("team")) == "away"],
            "reference": serialise_reference(reference_row),
        })

    return {
        "meta": {
            "match_id": match_id,
            "title": title,
            "match_date": snapshots[0].match_date,
            "pitch_length": pitch_length,
            "pitch_width": pitch_width,
            "zone_config": {
                "penalty_box_depth_m": cfg.zone.penalty_box_depth_m,
                "wide_channel_depth_m": cfg.zone.wide_channel_depth_m,
                "penalty_box_side_inset_m": cfg.zone.penalty_box_side_inset_m,
                "central_band_divisor": cfg.zone.central_band_divisor,
            },
            "visible_zones": visible_zones,
            "show_numbers": show_numbers,
            "ball_tail": max(1, ball_tail),
            "focus_tail": max(1, focus_tail),
            "focus_label": selector.label if selector is not None else "nearest player to ball",
        },
        "frames": frames,
        "total_duration_s": times[-1] if times else 0.0,
    }


def serialise_player(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": player_key(row),
        "team": str(row.get("team")),
        "opta_player_id": to_int(row.get("opta_player_id")),
        "player_id": str(row["player_id"]) if row.get("player_id") is not None else None,
        "name": str(row["player_name"]) if row.get("player_name") is not None else None,
        "position": str(row["player_position"]) if row.get("player_position") is not None else None,
        "number": to_int(row.get("player_number")),
        "x": to_float(row.get("player_x"), 0.0),
        "y": to_float(row.get("player_y"), 0.0),
        "z": to_float(row.get("player_z")),
        "speed": to_float(row.get("player_speed")),
        "speed_band": str(row["player_speed_band"]) if row.get("player_speed_band") is not None else None,
        "distance_to_ball": to_float(row.get("player_ball_distance")),
        "has_ball_possession": bool(row.get("has_ball_possession") or False),
        "event_type": str(row["event_type"]) if row.get("event_type") is not None else None,
    }


def serialise_reference(reference_row: dict[str, Any] | None) -> dict[str, Any] | None:
    if reference_row is None:
        return None
    return {
        "key": player_key(reference_row),
        "team": str(reference_row.get("team")),
        "label": reference_row.get("player_name")
        or reference_row.get("player_id")
        or reference_row.get("opta_player_id"),
        "distance_to_ball": to_float(reference_row.get("player_ball_distance")),
        "has_ball_possession": bool(reference_row.get("has_ball_possession") or False),
    }


def player_key(row: dict[str, Any]) -> str:
    identifier = (
        row.get("player_id")
        or row.get("opta_player_id")
        or row.get("player_number")
        or row.get("player_name")
        or "unknown"
    )
    return f"{row.get('team')}::{identifier}"


def save_html_replay(output_path: str | Path, payload: dict[str, Any]) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload_json = json.dumps(payload, separators=(",", ":"))
    html = html_template().replace("__PAYLOAD__", payload_json)
    output.write_text(html, encoding="utf-8")
    print(f"Saved smooth HTML replay to {output}")


def html_template() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Tracking Replay</title>
  <style>
    :root {
      --bg: #08121c;
      --panel: #102131;
      --panel-2: #183247;
      --text: #f3f7fb;
      --muted: #b4c4d4;
      --accent: #ffd166;
      --home: #46a6ff;
      --away: #ff6b6b;
      --ball: #111111;
      --pitch: #2e8b57;
      --pitch-dark: #267449;
      --line: #ffffff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      background: radial-gradient(circle at top, #17354a 0%, var(--bg) 45%);
      color: var(--text);
    }
    .shell {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px;
    }
    .panel {
      background: linear-gradient(180deg, rgba(24, 50, 71, 0.98), rgba(16, 33, 49, 0.95));
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.28);
    }
    .header {
      display: flex;
      gap: 16px;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 14px;
    }
    .title {
      font-size: 1.35rem;
      font-weight: 700;
      margin: 0 0 6px;
    }
    .meta, .focus-line, .event-line {
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.45;
    }
    .event-line {
      color: #ffe3a1;
      min-height: 1.4em;
    }
    .controls {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin: 14px 0 18px;
    }
    button, select {
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      font: inherit;
      cursor: pointer;
    }
    button {
      background: var(--accent);
      color: #0b1620;
      font-weight: 700;
    }
    button.secondary {
      background: rgba(255, 255, 255, 0.12);
      color: var(--text);
    }
    input[type="range"] {
      flex: 1 1 340px;
      accent-color: var(--accent);
    }
    .speed {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 18px;
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .zone-buttons {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-top: 12px;
    }
    .zone-button {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 7px 11px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.06);
      color: var(--muted);
      border: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 0.82rem;
      cursor: pointer;
      transition: background 140ms ease, border-color 140ms ease, color 140ms ease;
    }
    .zone-button.active {
      background: rgba(255, 209, 102, 0.18);
      color: var(--text);
      border-color: rgba(255, 209, 102, 0.42);
    }
    .player-card {
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      min-height: 78px;
      line-height: 1.5;
    }
    .swatch {
      display: inline-block;
      width: 12px;
      height: 12px;
      border-radius: 999px;
      margin-right: 6px;
      vertical-align: middle;
    }
    canvas {
      width: 100%;
      height: auto;
      display: block;
      background: linear-gradient(180deg, var(--pitch), var(--pitch-dark));
      border-radius: 18px;
      border: 1px solid rgba(255, 255, 255, 0.12);
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="panel">
      <div class="header">
        <div>
          <div class="title" id="title"></div>
          <div class="meta" id="meta"></div>
          <div class="focus-line" id="focusLine"></div>
          <div class="event-line" id="eventLine"></div>
        </div>
      </div>
      <div class="controls">
        <button id="playButton" type="button">Play</button>
        <button id="stopButton" class="secondary" type="button">Stop</button>
        <button id="labelToggleButton" class="secondary" type="button">Show Labels</button>
        <input id="timeline" type="range" min="0" max="1000" value="0">
        <div class="speed">
          <span>Speed</span>
          <select id="speedSelect">
            <option value="0.5">0.5x</option>
            <option value="1" selected>1.0x</option>
            <option value="1.5">1.5x</option>
            <option value="2">2.0x</option>
          </select>
        </div>
      </div>
      <canvas id="pitch" width="1120" height="760"></canvas>
      <div class="legend">
        <span><span class="swatch" style="background: var(--home);"></span>Home players</span>
        <span><span class="swatch" style="background: var(--away);"></span>Away players</span>
        <span><span class="swatch" style="background: var(--ball);"></span>Ball</span>
        <span><span class="swatch" style="background: transparent; border: 2px solid var(--accent);"></span>Possession flag / focus</span>
      </div>
      <div class="zone-buttons" id="zoneButtons"></div>
      <div class="player-card" id="playerCard">Click a player circle to inspect their distance to the ball and x/y/z coordinates.</div>
    </div>
  </div>
  <script>
    const payload = __PAYLOAD__;
    const meta = payload.meta;
    const frames = payload.frames;
    const totalDuration = Math.max(payload.total_duration_s || 0, 0.001);
    const canvas = document.getElementById("pitch");
    const ctx = canvas.getContext("2d");
    const titleEl = document.getElementById("title");
    const metaEl = document.getElementById("meta");
    const focusEl = document.getElementById("focusLine");
    const eventEl = document.getElementById("eventLine");
    const playButton = document.getElementById("playButton");
    const stopButton = document.getElementById("stopButton");
    const labelToggleButton = document.getElementById("labelToggleButton");
    const timeline = document.getElementById("timeline");
    const speedSelect = document.getElementById("speedSelect");
    const zoneButtons = document.getElementById("zoneButtons");
    const playerCard = document.getElementById("playerCard");
    const margin = 36;

    let isPlaying = false;
    let playhead = 0;
    let lastTimestamp = null;
    let selectedPlayerKey = null;
    let selectedZoneName = null;
    let currentRenderState = null;
    let showLabels = Boolean(meta.show_numbers);

    const zoneConfig = meta.zone_config;
    const zonePalette = {
      def_box_left: "rgba(162, 184, 217, 0.12)",
      def_box_center: "rgba(157, 196, 210, 0.12)",
      def_box_right: "rgba(162, 184, 217, 0.12)",
      def_wide_left: "rgba(180, 199, 222, 0.10)",
      def_center: "rgba(190, 214, 225, 0.10)",
      def_wide_right: "rgba(180, 199, 222, 0.10)",
      mid_wide_left: "rgba(214, 223, 196, 0.10)",
      mid_halfspace_left: "rgba(220, 228, 205, 0.10)",
      mid_center: "rgba(228, 234, 214, 0.10)",
      mid_halfspace_right: "rgba(220, 228, 205, 0.10)",
      mid_wide_right: "rgba(214, 223, 196, 0.10)",
      att_wide_left: "rgba(229, 206, 188, 0.10)",
      att_halfspace_left: "rgba(235, 214, 198, 0.10)",
      att_halfspace_center: "rgba(240, 220, 205, 0.10)",
      att_halfspace_right: "rgba(235, 214, 198, 0.10)",
      att_wide_right: "rgba(229, 206, 188, 0.10)",
      att_box_left: "rgba(239, 196, 179, 0.12)",
      att_box_center: "rgba(244, 205, 191, 0.12)",
      att_box_right: "rgba(239, 196, 179, 0.12)",
    };

    titleEl.textContent = meta.title + " | " + (meta.match_date || "date n/a");

    function formatMetric(value) {
      return value === null || value === undefined ? "n/a" : Number(value).toFixed(2);
    }

    function formatClock(value) {
      if (value === null || value === undefined) {
        return "n/a";
      }
      const total = Math.max(0, Number(value));
      const minutes = Math.floor(total / 60);
      const seconds = total - (minutes * 60);
      return String(minutes).padStart(2, "0") + ":" + seconds.toFixed(2).padStart(5, "0");
    }

    function xToPx(x) {
      return margin + ((x + (meta.pitch_length / 2)) / meta.pitch_length) * (canvas.width - margin * 2);
    }

    function yToPx(y) {
      return canvas.height - margin - ((y + (meta.pitch_width / 2)) / meta.pitch_width) * (canvas.height - margin * 2);
    }

    function syncPlayButton() {
      playButton.textContent = isPlaying ? "Pause" : "Play";
    }

    function syncLabelToggleButton() {
      labelToggleButton.textContent = showLabels ? "Hide Labels" : "Show Labels";
    }

    function playerDisplayLabel(player) {
      return [player.number, player.name].filter(Boolean).join(" ") ||
        player.player_id ||
        player.opta_player_id ||
        player.key;
    }

    function zoneDefinitions() {
      const xMin = -meta.pitch_length / 2;
      const xMax = meta.pitch_length / 2;
      const yMin = -meta.pitch_width / 2;
      const yMax = meta.pitch_width / 2;
      const x1 = xMin + zoneConfig.penalty_box_depth_m;
      const x2 = xMin + zoneConfig.wide_channel_depth_m;
      const x3 = xMax - zoneConfig.wide_channel_depth_m;
      const x4 = xMax - zoneConfig.penalty_box_depth_m;
      const y1 = yMin + zoneConfig.penalty_box_side_inset_m;
      const y2 = -meta.pitch_width / zoneConfig.central_band_divisor;
      const y3 = meta.pitch_width / zoneConfig.central_band_divisor;
      const y4 = yMax - zoneConfig.penalty_box_side_inset_m;

      return [
        { name: "def_box_left", x0: xMin, x1, y0: yMin, y1 },
        { name: "def_box_center", x0: xMin, x1, y0: y1, y1: y4 },
        { name: "def_box_right", x0: xMin, x1, y0: y4, y1: yMax },
        { name: "def_wide_left", x0: x1, x1: x2, y0: yMin, y1: y2 },
        { name: "def_center", x0: x1, x1: x2, y0: y2, y1: y3 },
        { name: "def_wide_right", x0: x1, x1: x2, y0: y3, y1: yMax },
        { name: "mid_wide_left", x0: x2, x1: x3, y0: yMin, y1: y1 },
        { name: "mid_halfspace_left", x0: x2, x1: x3, y0: y1, y1: y2 },
        { name: "mid_center", x0: x2, x1: x3, y0: y2, y1: y3 },
        { name: "mid_halfspace_right", x0: x2, x1: x3, y0: y3, y1: y4 },
        { name: "mid_wide_right", x0: x2, x1: x3, y0: y4, y1: yMax },
        { name: "att_wide_left", x0: x3, x1: x4, y0: yMin, y1: y1 },
        { name: "att_halfspace_left", x0: x3, x1: x4, y0: y1, y1: y2 },
        { name: "att_halfspace_center", x0: x3, x1: x4, y0: y2, y1: y3 },
        { name: "att_halfspace_right", x0: x3, x1: x4, y0: y3, y1: y4 },
        { name: "att_wide_right", x0: x3, x1: x4, y0: y4, y1: yMax },
        { name: "att_box_left", x0: x4, x1: xMax, y0: yMin, y1 },
        { name: "att_box_center", x0: x4, x1: xMax, y0: y1, y1: y4 },
        { name: "att_box_right", x0: x4, x1: xMax, y0: y4, y1: yMax },
      ];
    }

    function drawZoneOverlay() {
      for (const zone of zoneDefinitions()) {
        const left = xToPx(zone.x0);
        const right = xToPx(zone.x1);
        const top = yToPx(zone.y1);
        const bottom = yToPx(zone.y0);
        const width = right - left;
        const height = bottom - top;
        if (selectedZoneName === zone.name) {
          ctx.fillStyle = zonePalette[zone.name] || "rgba(255,255,255,0.14)";
          ctx.fillRect(left, top, width, height);
          ctx.strokeStyle = "rgba(255, 209, 102, 0.78)";
          ctx.lineWidth = 2;
        } else {
          ctx.strokeStyle = "rgba(255, 255, 255, 0.08)";
          ctx.lineWidth = 1;
        }
        ctx.strokeRect(left, top, width, height);
      }
    }

    function buildZoneButtons() {
      const zones = (meta.visible_zones || []).slice().sort();
      zoneButtons.innerHTML = zones.map((zone) => (
        '<button type="button" class="zone-button' +
        (selectedZoneName === zone ? " active" : "") +
        '" data-zone="' + zone + '">' + zone + "</button>"
      )).join("");
    }

    function drawPitch() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue("--pitch");
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      drawZoneOverlay();

      const xMin = xToPx(-meta.pitch_length / 2);
      const xMax = xToPx(meta.pitch_length / 2);
      const yMin = yToPx(meta.pitch_width / 2);
      const yMax = yToPx(-meta.pitch_width / 2);
      const pitchWidthPx = xMax - xMin;
      const pitchHeightPx = yMax - yMin;

      ctx.strokeStyle = "#ffffff";
      ctx.lineWidth = 3;
      ctx.strokeRect(xMin, yMin, pitchWidthPx, pitchHeightPx);

      ctx.beginPath();
      ctx.moveTo((xMin + xMax) / 2, yMin);
      ctx.lineTo((xMin + xMax) / 2, yMax);
      ctx.stroke();

      const centerRadius = (9.15 / meta.pitch_width) * pitchHeightPx;
      ctx.beginPath();
      ctx.arc((xMin + xMax) / 2, (yMin + yMax) / 2, centerRadius, 0, Math.PI * 2);
      ctx.stroke();

      drawBox(xMin, yMin, pitchHeightPx, 16.5, 40.32, false);
      drawBox(xMax, yMin, pitchHeightPx, 16.5, 40.32, true);
      drawBox(xMin, yMin, pitchHeightPx, 5.5, 18.32, false);
      drawBox(xMax, yMin, pitchHeightPx, 5.5, 18.32, true);
    }

    function drawBox(edgeX, yMin, pitchHeightPx, depthM, widthM, fromRight) {
      const depthPx = (depthM / meta.pitch_length) * (canvas.width - margin * 2);
      const widthPx = (widthM / meta.pitch_width) * pitchHeightPx;
      const top = (canvas.height - widthPx) / 2;
      const left = fromRight ? edgeX - depthPx : edgeX;
      ctx.strokeRect(left, top, depthPx, widthPx);
    }

    function framePairForTime(timeS) {
      if (frames.length === 1) {
        return { current: frames[0], next: frames[0], alpha: 0, index: 0 };
      }
      for (let i = 0; i < frames.length - 1; i += 1) {
        const current = frames[i];
        const next = frames[i + 1];
        if (timeS <= next.time_s) {
          const span = Math.max(next.time_s - current.time_s, 0.0001);
          return {
            current,
            next,
            alpha: Math.max(0, Math.min(1, (timeS - current.time_s) / span)),
            index: i,
          };
        }
      }
      const lastIndex = frames.length - 1;
      return { current: frames[lastIndex], next: frames[lastIndex], alpha: 0, index: lastIndex };
    }

    function toMap(players) {
      const map = new Map();
      for (const player of players) {
        map.set(player.key, player);
      }
      return map;
    }

    function interpolatePlayers(currentPlayers, nextPlayers, alpha) {
      const nextMap = toMap(nextPlayers);
      return currentPlayers.map((player) => {
        const next = nextMap.get(player.key);
        if (!next) {
          return player;
        }
        return {
          ...player,
          x: player.x + ((next.x - player.x) * alpha),
          y: player.y + ((next.y - player.y) * alpha),
          z: player.z !== null && next.z !== null ? player.z + ((next.z - player.z) * alpha) : player.z,
        };
      });
    }

    function interpolateBall(currentBall, nextBall, alpha) {
      return {
        x: currentBall.x + ((nextBall.x - currentBall.x) * alpha),
        y: currentBall.y + ((nextBall.y - currentBall.y) * alpha),
        z: currentBall.z,
        speed: currentBall.speed,
      };
    }

    function drawPlayers(players, color) {
      for (const player of players) {
        const px = xToPx(player.x);
        const py = yToPx(player.y);
        ctx.fillStyle = color;
        ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.arc(px, py, 12, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();

        if (player.has_ball_possession) {
          ctx.strokeStyle = "#ffd166";
          ctx.lineWidth = 3;
          ctx.beginPath();
          ctx.arc(px, py, 17, 0, Math.PI * 2);
          ctx.stroke();
        }

        if (showLabels && player.number !== null && player.number !== undefined) {
          ctx.fillStyle = "#ffffff";
          ctx.font = "12px Segoe UI";
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillText(String(player.number), px, py);
        }

        if (showLabels) {
          const labelParts = [];
          if (player.number !== null && player.number !== undefined) {
            labelParts.push(String(player.number));
          }
          if (player.name) {
            labelParts.push(String(player.name));
          }
          if (labelParts.length > 0) {
            const label = labelParts.join(" ");
            ctx.font = "12px Segoe UI";
            ctx.textAlign = "center";
            ctx.textBaseline = "bottom";
            const textWidth = ctx.measureText(label).width;
            const boxWidth = textWidth + 12;
            const labelY = py - 19;
            ctx.fillStyle = "rgba(8, 18, 28, 0.78)";
            ctx.fillRect(px - (boxWidth / 2), labelY - 14, boxWidth, 16);
            ctx.fillStyle = "#ffffff";
            ctx.fillText(label, px, labelY - 2);
          }
        }
      }
    }

    function drawBall(ball) {
      ctx.fillStyle = "#f4a261";
      ctx.beginPath();
      ctx.arc(xToPx(ball.x), yToPx(ball.y), 5, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = "#111111";
      ctx.beginPath();
      ctx.arc(xToPx(ball.x), yToPx(ball.y), 3.4, 0, Math.PI * 2);
      ctx.fill();
    }

    function drawTail(framesSubset, kind, selectorKey) {
      ctx.strokeStyle = kind === "ball" ? "#f4a261" : "#ffd166";
      ctx.lineWidth = kind === "ball" ? 2.5 : 2;
      ctx.globalAlpha = 0.75;
      ctx.beginPath();
      let started = false;
      for (const frame of framesSubset) {
        let point = null;
        if (kind === "ball") {
          point = frame.ball;
        } else if (frame.reference && frame.reference.key === selectorKey) {
          const player = [...frame.home, ...frame.away].find((candidate) => candidate.key === selectorKey);
          if (player) {
            point = player;
          }
        }
        if (!point) {
          continue;
        }
        const px = xToPx(point.x);
        const py = yToPx(point.y);
        if (!started) {
          ctx.moveTo(px, py);
          started = true;
        } else {
          ctx.lineTo(px, py);
        }
      }
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    function drawReference(reference, allPlayers, ball) {
      if (!reference) {
        return;
      }
      const player = allPlayers.find((candidate) => candidate.key === reference.key);
      if (!player) {
        return;
      }
      const px = xToPx(player.x);
      const py = yToPx(player.y);
      const bx = xToPx(ball.x);
      const by = yToPx(ball.y);

      ctx.strokeStyle = "#ffd166";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(bx, by);
      ctx.stroke();
    }

    function distanceBetween(player, ball) {
      return Math.sqrt(Math.pow(player.x - ball.x, 2) + Math.pow(player.y - ball.y, 2));
    }

    function updatePlayerCard(selectedPlayer, ball) {
      if (!selectedPlayer) {
        playerCard.textContent = "Click a player circle to inspect their distance to the ball and x/y/z coordinates.";
        return;
      }
      const label = playerDisplayLabel(selectedPlayer);
      playerCard.textContent =
        "Selected: " + label +
        " | team=" + selectedPlayer.team +
        " | speed_mps=" + formatMetric(selectedPlayer.speed) +
        " | speed_band=" + (selectedPlayer.speed_band || "n/a") +
        " | event_type=" + (selectedPlayer.event_type || "n/a") +
        " | distance_to_ball_m=" + formatMetric(distanceBetween(selectedPlayer, ball)) +
        " | x=" + formatMetric(selectedPlayer.x) +
        " | y=" + formatMetric(selectedPlayer.y) +
        " | z=" + formatMetric(selectedPlayer.z);
    }

    function updateEventLine(players) {
      const frameEvents = players.filter((player) => player.event_type);
      if (frameEvents.length === 0) {
        eventEl.textContent = "";
        return;
      }
      eventEl.textContent = frameEvents.map((player) => (
        "event=" + player.event_type + " | player=" + playerDisplayLabel(player)
      )).join("   ||   ");
    }

    function render() {
      drawPitch();
      const pair = framePairForTime(playhead);
      const ball = interpolateBall(pair.current.ball, pair.next.ball, pair.alpha);
      const home = interpolatePlayers(pair.current.home, pair.next.home, pair.alpha);
      const away = interpolatePlayers(pair.current.away, pair.next.away, pair.alpha);
      const allPlayers = [...home, ...away];

      const ballTailFrames = frames.slice(Math.max(0, pair.index - meta.ball_tail + 1), pair.index + 1);
      drawTail(ballTailFrames, "ball", null);
      if (pair.current.reference && pair.current.reference.key) {
        const focusTailFrames = frames.slice(Math.max(0, pair.index - meta.focus_tail + 1), pair.index + 1);
        drawTail(focusTailFrames, "focus", pair.current.reference.key);
      }

      drawPlayers(home, getComputedStyle(document.documentElement).getPropertyValue("--home"));
      drawPlayers(away, getComputedStyle(document.documentElement).getPropertyValue("--away"));
      drawReference(pair.current.reference, allPlayers, ball);
      drawBall(ball);
      currentRenderState = { allPlayers, ball };

      metaEl.textContent =
        "date=" + (pair.current.match_date || "n/a") +
        " | period=" + pair.current.period +
        " | frame=" + pair.current.frame_id +
        " | clock=" + formatClock(pair.current.game_clock) +
        " | live=" + pair.current.live +
        " | last_touch=" + (pair.current.last_touch || "n/a");

      if (pair.current.reference) {
        focusEl.textContent =
          "focus=" + meta.focus_label +
          " | player=" + pair.current.reference.label +
          " | distance_to_ball_m=" + formatMetric(pair.current.reference.distance_to_ball) +
          " | has_ball_possession=" + Boolean(pair.current.reference.has_ball_possession) +
          " | ball_speed_mps=" + formatMetric(pair.current.ball.speed);
      } else {
        focusEl.textContent = "focus=" + meta.focus_label + " | no player found in this frame";
      }

      const selectedPlayer = selectedPlayerKey
        ? allPlayers.find((candidate) => candidate.key === selectedPlayerKey) || null
        : null;
      updatePlayerCard(selectedPlayer, ball);
      updateEventLine(allPlayers);

      syncPlayButton();
      syncLabelToggleButton();
      timeline.value = String(Math.round((playhead / totalDuration) * 1000));
    }

    function tick(timestamp) {
      if (lastTimestamp === null) {
        lastTimestamp = timestamp;
      }
      const delta = (timestamp - lastTimestamp) / 1000;
      lastTimestamp = timestamp;
      if (isPlaying) {
        playhead += delta * Number(speedSelect.value);
        if (playhead >= totalDuration) {
          playhead = totalDuration;
          isPlaying = false;
        }
      }
      render();
      requestAnimationFrame(tick);
    }

    playButton.addEventListener("click", () => {
      if (isPlaying) {
        isPlaying = false;
      } else {
        if (playhead >= totalDuration) {
          playhead = 0;
        }
        isPlaying = true;
      }
      render();
    });

    stopButton.addEventListener("click", () => {
      isPlaying = false;
      playhead = 0;
      render();
    });

    labelToggleButton.addEventListener("click", () => {
      showLabels = !showLabels;
      render();
    });

    timeline.addEventListener("input", () => {
      isPlaying = false;
      playhead = (Number(timeline.value) / 1000) * totalDuration;
      render();
    });

    canvas.addEventListener("click", (event) => {
      if (!currentRenderState) {
        return;
      }
      const rect = canvas.getBoundingClientRect();
      const scaleX = canvas.width / rect.width;
      const scaleY = canvas.height / rect.height;
      const clickX = (event.clientX - rect.left) * scaleX;
      const clickY = (event.clientY - rect.top) * scaleY;

      let hit = null;
      let bestDistance = Infinity;
      for (const player of currentRenderState.allPlayers) {
        const px = xToPx(player.x);
        const py = yToPx(player.y);
        const distance = Math.sqrt(Math.pow(px - clickX, 2) + Math.pow(py - clickY, 2));
        if (distance <= 16 && distance < bestDistance) {
          hit = player;
          bestDistance = distance;
        }
      }
      selectedPlayerKey = hit ? hit.key : null;
      render();
    });

    zoneButtons.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const zone = target.dataset.zone;
      if (!zone) {
        return;
      }
      selectedZoneName = selectedZoneName === zone ? null : zone;
      buildZoneButtons();
      render();
    });

    buildZoneButtons();
    render();
    requestAnimationFrame(tick);
  </script>
</body>
</html>
"""


def focus_history(
    snapshots: list[FrameSnapshot],
    frame_index: int,
    selector: FocusSelector,
    focus_tail: int,
) -> list[tuple[float, float]]:
    history: list[tuple[float, float]] = []
    start = max(0, frame_index - max(1, focus_tail) + 1)
    for snapshot in snapshots[start: frame_index + 1]:
        row = focus_row(snapshot.rows, selector)
        if row is None:
            continue
        history.append((float(row["player_x"]), float(row["player_y"])))
    return history


def plot_team(ax, rows: list[dict[str, Any]], color: str, label: str) -> None:
    if not rows:
        return
    ax.scatter(
        [float(row["player_x"]) for row in rows],
        [float(row["player_y"]) for row in rows],
        c=color,
        s=120,
        alpha=0.92,
        edgecolors="white",
        linewidths=0.8,
        zorder=4,
        label=label,
    )


def annotate_player_numbers(ax, home_rows: list[dict[str, Any]], away_rows: list[dict[str, Any]]) -> None:
    for row in home_rows + away_rows:
        number = row.get("player_number")
        if number is None:
            continue
        ax.text(
            float(row["player_x"]),
            float(row["player_y"]),
            str(number),
            ha="center",
            va="center",
            fontsize=8,
            color="white",
            zorder=9,
        )


def main() -> int:
    args = parse_args()
    cfg = load_runtime_storage_config(args.config_path)
    if (
        args.clock_start is not None
        and args.clock_end is not None
        and args.clock_end < args.clock_start
    ):
        raise ValueError("--clock-end must be greater than or equal to --clock-start.")

    df, pitch_length, pitch_width = load_tracking_window(args)
    snapshots = build_snapshots(df)
    if not snapshots:
        raise FileNotFoundError("No frame snapshots were built from the selected match window.")

    selector = resolve_focus_selector(args)
    validate_focus_selector(snapshots, selector)

    print(
        f"Loaded match_id={args.match_id} period={args.period} "
        f"window={describe_window(args)} "
        f"frames={len(snapshots)} pitch=({pitch_length:.2f}m x {pitch_width:.2f}m)"
    )
    print_focus_transitions(snapshots, selector)

    output_path = default_output_path(
        args.match_id,
        args.period,
        selector,
        args.output,
        clock_start=args.clock_start,
        clock_end=args.clock_end,
    )
    if Path(output_path).suffix.lower() == ".html":
        payload = build_replay_payload(
            snapshots,
            pitch_length,
            pitch_width,
            cfg=cfg,
            visible_zones=sorted(df.get_column("pitch_zone").drop_nulls().unique().to_list()),
            match_id=args.match_id,
            selector=selector,
            show_numbers=args.show_numbers,
            fallback_fps=args.fps,
            ball_tail=args.ball_tail,
            focus_tail=args.focus_tail,
        )
        save_html_replay(output_path, payload)
        return 0

    animate_match(
        snapshots,
        pitch_length,
        pitch_width,
        match_id=args.match_id,
        selector=selector,
        show_numbers=args.show_numbers,
        fps=args.fps,
        ball_tail=args.ball_tail,
        focus_tail=args.focus_tail,
        output_path=output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
