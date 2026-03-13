from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from .config import DEFAULT_STORAGE, StorageConfig
from .storage import StorageManager

DEFAULT_MODEL = "normalized"
SUPPORTED_MODELS = ("normalized", "denormalized")


@dataclass(frozen=True)
class MatchMetadata:
    opta_match_id: int
    pitch_length: float
    pitch_width: float
    fixture: str | None
    match_date: date | None
    player_rows: list[dict[str, object]]
    source_path: Path | None = None


def load_match_metadata(
    storage_manager: StorageManager,
    opta_match_id: int,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> MatchMetadata:
    metadata_path = storage_manager.resolve_metadata_path(opta_match_id)
    if metadata_path is None:
        return MatchMetadata(
            opta_match_id=opta_match_id,
            pitch_length=cfg.length,
            pitch_width=cfg.width,
            fixture=None,
            match_date=None,
            player_rows=[],
            source_path=None,
        )

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    fixture, match_date = _parse_match_context(payload)
    return MatchMetadata(
        opta_match_id=opta_match_id,
        pitch_length=float(payload.get("pitchLength", cfg.length)),
        pitch_width=float(payload.get("pitchWidth", cfg.width)),
        fixture=fixture,
        match_date=match_date,
        player_rows=_parse_players(payload),
        source_path=metadata_path,
    )


def enrich_tracking_rows(
    frame: pl.LazyFrame,
    metadata: MatchMetadata,
    model: str = DEFAULT_MODEL,
) -> pl.LazyFrame:
    enriched = frame.with_columns([
        pl.lit(metadata.pitch_length).cast(pl.Float32).alias("pitch_length"),
        pl.lit(metadata.pitch_width).cast(pl.Float32).alias("pitch_width"),
    ])

    if model != "denormalized":
        return enriched

    if metadata.fixture is not None:
        enriched = enriched.with_columns(pl.lit(metadata.fixture).alias("fixture"))
    if metadata.match_date is not None:
        enriched = enriched.with_columns(pl.lit(metadata.match_date).cast(pl.Date).alias("match_date"))
    if metadata.player_rows:
        player_dim = (
            pl.DataFrame(metadata.player_rows)
            .with_columns(pl.col("opta_player_id").cast(pl.Int64, strict=False))
            .lazy()
        )
        enriched = enriched.with_columns(pl.col("opta_player_id").cast(pl.Int64, strict=False))
        enriched = enriched.join(player_dim, on="opta_player_id", how="left")
    return enriched


def _parse_match_context(payload: dict[str, object]) -> tuple[str | None, date | None]:
    description = str(payload.get("description") or "").strip()
    fixture = None
    if description:
        fixture = description.split(":", 1)[0].strip()

    year = payload.get("year")
    month = payload.get("month")
    day = payload.get("day")
    if year is not None and month is not None and day is not None:
        return fixture, date(int(year), int(month), int(day))

    if ":" in description:
        raw_date = description.split(":", 1)[1].strip()
        parts = raw_date.split("-")
        if len(parts) == 3:
            return fixture, date(int(parts[0]), int(parts[1]), int(parts[2]))
    return fixture, None


def _parse_players(payload: dict[str, object]) -> list[dict[str, object]]:
    player_rows: dict[int, dict[str, object]] = {}
    for team_key in ("homePlayers", "awayPlayers"):
        for player in payload.get(team_key, []) or []:
            opta_player_id = player.get("optaId")
            if opta_player_id is None:
                continue
            try:
                numeric_opta_player_id = int(opta_player_id)
            except (TypeError, ValueError):
                continue
            player_rows[numeric_opta_player_id] = {
                "opta_player_id": numeric_opta_player_id,
                "player_name": player.get("name"),
                "player_position": player.get("position"),
            }
    return list(player_rows.values())

