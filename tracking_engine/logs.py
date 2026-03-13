import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import polars as pl

DEFAULT_PERFORMANCE_LOG_FILE_NAME = "tracking_engine_performance.jsonl"
DEFAULT_PERFORMANCE_LOG_QUERY = """
SELECT
    logged_at_utc,
    run_id,
    run_mode,
    opta_match_id,
    task_scope,
    task,
    duration_seconds,
    duration_text,
    rows,
    status,
    source_status,
    file_path,
    source_file_path,
    output_file_path,
    batch_index,
    match_count,
    match_ids_csv,
    teams,
    save
FROM performance_logs
ORDER BY logged_at_utc, COALESCE(opta_match_id, -1), task
"""


def new_run_id() -> str:
    return uuid4().hex


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_performance_log(log_path: Path, record: dict[str, object]) -> None:
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    serializable_record: dict[str, object] = {}
    for key, value in record.items():
        if isinstance(value, Path):
            serializable_record[key] = str(value)
        elif isinstance(value, tuple):
            serializable_record[key] = list(value)
        else:
            serializable_record[key] = value

    with log_path.open("a", encoding="utf-8") as fh:
        json.dump(serializable_record, fh, ensure_ascii=True)
        fh.write("\n")


def query_performance_logs(
    log_path: Path,
    sql: str | None = None,
) -> pl.DataFrame:
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError(
            "duckdb is required to query performance logs. Install the package with duckdb available."
        ) from exc

    log_path = Path(log_path)
    if not log_path.exists():
        raise FileNotFoundError(f"Performance log file not found: {log_path}")

    escaped_path = str(log_path.resolve()).replace("\\", "/").replace("'", "''")
    con = duckdb.connect()
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW performance_logs AS "
        f"SELECT * FROM read_json_auto('{escaped_path}')"
    )
    result = con.execute(sql or DEFAULT_PERFORMANCE_LOG_QUERY)
    rows = result.fetchall()
    columns = [item[0] for item in result.description]
    if not rows:
        return pl.DataFrame({column: [] for column in columns})
    return pl.DataFrame(rows, schema=columns, orient="row")
