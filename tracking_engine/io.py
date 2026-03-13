from pathlib import Path

import polars as pl

from .config import DEFAULT_STORAGE, StorageConfig


def _temporary_parquet_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.tmp{path.suffix}")


def _replace_file(temp_path: Path, final_path: Path) -> None:
    if temp_path.exists():
        temp_path.replace(final_path)


def is_valid_parquet(path: Path) -> bool:
    """
    Check whether a Parquet file is structurally readable by Polars.
    """
    path = Path(path)
    if not path.exists() or path.stat().st_size < 12:
        return False
    try:
        (
            pl.scan_parquet(str(path))
            .select(pl.len().alias("row_count"))
            .collect(streaming=True)
        )
        return True
    except Exception:
        return False


def jsonl_to_parquet(
    jsonl_path: Path,
    parquet_path: Path,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> None:
    """
    Stream a JSONL file to Parquet without loading everything into memory.
    """
    parquet_path = Path(parquet_path)
    temp_path = _temporary_parquet_path(parquet_path)
    if temp_path.exists():
        temp_path.unlink()
    pl.scan_ndjson(str(jsonl_path)).sink_parquet(
        str(temp_path),
        compression=cfg.parquet_compression,
    )
    _replace_file(temp_path, parquet_path)
    print(f"[tracking_engine] Wrote {parquet_path} ({parquet_path.stat().st_size / 1e6:.1f} MB)")


def count_parquet_rows(path: Path) -> int:
    """Read a Parquet row count with a lightweight lazy scan."""
    return int(
        pl.scan_parquet(str(path))
        .select(pl.len().alias("row_count"))
        .collect(streaming=True)
        .get_column("row_count")
        .item()
    )


def ensure_parquet_source(
    jsonl_path: Path,
    parquet_path: Path,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> Path:
    """
    Return a readable Parquet source path.

    If an existing Parquet file is corrupt or incomplete and a JSONL source
    exists, rebuild the Parquet file from JSONL.
    """
    jsonl_path = Path(jsonl_path)
    parquet_path = Path(parquet_path)

    if parquet_path.exists() and is_valid_parquet(parquet_path):
        return parquet_path

    if parquet_path.exists() and not is_valid_parquet(parquet_path):
        if not jsonl_path.exists():
            raise ValueError(
                f"Parquet source is invalid and no JSONL source exists to rebuild it: {parquet_path}"
            )
        print(
            "[tracking_engine] Invalid parquet detected; rebuilding from JSONL "
            f"source={jsonl_path.name}"
        )
        jsonl_to_parquet(jsonl_path, parquet_path, cfg)
        return parquet_path

    if jsonl_path.exists():
        print(f"[tracking_engine] Converting {jsonl_path.name} -> parquet...")
        jsonl_to_parquet(jsonl_path, parquet_path, cfg)
        return parquet_path

    raise FileNotFoundError(f"No readable tracking source found: {jsonl_path} or {parquet_path}")


def resolve_tracking_source(
    jsonl_path: Path,
    parquet_path: Path,
    ndjson_path: Path | None = None,
) -> tuple[Path, str]:
    """
    Return the safest readable source path with the lowest peak-memory cost.

    Selection order:
    - valid Parquet source if it already exists
    - JSONL/NDJSON source streamed directly if available

    This deliberately avoids auto-converting large nested JSONL files to raw
    Parquet during normal pipeline execution, because that conversion can become
    the highest peak-memory step in the whole run.
    """
    jsonl_path = Path(jsonl_path)
    parquet_path = Path(parquet_path)
    ndjson_path = None if ndjson_path is None else Path(ndjson_path)

    if parquet_path.exists() and is_valid_parquet(parquet_path):
        return parquet_path, "reused_parquet"

    if jsonl_path.exists():
        if parquet_path.exists():
            print(
                "[tracking_engine] Invalid or unreadable parquet detected; "
                f"streaming JSONL directly source={jsonl_path.name}"
            )
            return jsonl_path, "streamed_jsonl_invalid_parquet"
        return jsonl_path, "streamed_jsonl"

    if ndjson_path is not None and ndjson_path.exists():
        if parquet_path.exists():
            print(
                "[tracking_engine] Invalid or unreadable parquet detected; "
                f"streaming NDJSON directly source={ndjson_path.name}"
            )
            return ndjson_path, "streamed_ndjson_invalid_parquet"
        return ndjson_path, "streamed_ndjson"

    if parquet_path.exists():
        raise ValueError(
            f"Parquet source is invalid and no JSONL/NDJSON source exists to replace it: {parquet_path}"
        )

    raise FileNotFoundError(
        f"No readable tracking source found: {jsonl_path}, "
        f"{ndjson_path if ndjson_path is not None else '<no ndjson path>'}, or {parquet_path}"
    )


def scan_tracking(path: Path) -> pl.LazyFrame:
    """
    Lazily scan a tracking file. Supports .parquet and .jsonl/.ndjson.
    """
    path = Path(path)
    if path.suffix == ".parquet":
        return pl.scan_parquet(str(path))
    if path.suffix in (".jsonl", ".ndjson"):
        return pl.scan_ndjson(str(path))
    raise ValueError(f"Unsupported file type: {path.suffix}. Use .parquet, .jsonl, or .ndjson")


def read_tracking(path: Path) -> pl.DataFrame:
    """Read and collect a tracking file eagerly."""
    return scan_tracking(path).collect(streaming=True)


def write_tracking(
    df: pl.DataFrame,
    path: Path,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> None:
    """Write a processed tracking DataFrame to Parquet with row-group statistics."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _temporary_parquet_path(path)
    if temp_path.exists():
        temp_path.unlink()
    df.rechunk().write_parquet(
        str(temp_path),
        compression=cfg.parquet_compression,
        statistics=True,
        row_group_size=cfg.parquet_row_group_size,
    )
    _replace_file(temp_path, path)
    print(f"[tracking_engine] Saved {path} ({path.stat().st_size / 1e6:.1f} MB, {len(df):,} rows)")


def write_tracking_lazy(
    lf: pl.LazyFrame,
    path: Path,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> None:
    """Stream a lazy tracking result directly to Parquet."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _temporary_parquet_path(path)
    if temp_path.exists():
        temp_path.unlink()
    lf.sink_parquet(
        str(temp_path),
        compression=cfg.parquet_compression,
        statistics=True,
        row_group_size=cfg.parquet_row_group_size,
    )
    _replace_file(temp_path, path)
    print(f"[tracking_engine] Saved {path} ({path.stat().st_size / 1e6:.1f} MB)")
