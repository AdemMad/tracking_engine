from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Literal, Sequence

import polars as pl

from .config import DEFAULT_STORAGE, StorageConfig, load_runtime_storage_config
from .io import (
    count_parquet_rows,
    resolve_tracking_source,
    scan_tracking,
    write_tracking,
    write_tracking_lazy,
)
from .layout import add_storage_layout_columns, select_output_columns
from .logs import (
    append_performance_log,
    new_run_id,
    query_performance_logs,
    utc_now_iso,
)
from .metadata import SUPPORTED_MODELS, enrich_tracking_rows, load_match_metadata
from .storage import DEFAULT_CONFIG_PATH, StorageManager, extract_match_id
from .zones import add_zone_columns

Team = Literal["home", "away"]


class TrackingPipeline:
    """
    Warehouse-first tracking data pipeline.

    The pipeline supports:
    - local or cloud-backed storage profiles configured in config.yaml
    - normalized or denormalized output models
    - single-match, many-match, or whole-directory processing
    """

    def __init__(
        self,
        *,
        storage: str = "local",
        model: str | None = None,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        tracking_dir: str | Path | None = None,
        export_dir: str | Path | None = None,
        metadata_dir: str | Path | None = None,
        storage_config: StorageConfig = DEFAULT_STORAGE,
        log_path: str | Path | None = None,
    ):
        self.cfg = load_runtime_storage_config(config_path, storage_config)
        self.model = self._validate_model(self.cfg.default_model if model is None else model)
        self.storage_name = storage
        self.storage_manager = StorageManager.from_yaml(
            config_path=config_path,
            storage=storage,
            tracking_dir=tracking_dir,
            export_dir=export_dir,
            metadata_dir=metadata_dir,
        )
        self.log_path = (
            Path(log_path) if log_path is not None else self.storage_manager.default_log_path()
        )

    def run_lazy(
        self,
        filename: str | int,
        teams: list[Team] | None = None,
    ) -> pl.LazyFrame:
        selected_teams = self._validate_teams(teams)
        match_files = self.storage_manager.resolve_match_files(filename)
        source_path, _ = resolve_tracking_source(
            match_files.jsonl_path,
            match_files.parquet_path,
            match_files.ndjson_path,
        )
        metadata = load_match_metadata(self.storage_manager, match_files.opta_match_id, self.cfg)
        return self._build_lazy_match_output(
            source_path,
            match_files.opta_match_id,
            selected_teams,
            metadata,
            sort_output=True,
        )

    def run(
        self,
        filename: str | int | Sequence[str | int] | None = None,
        save: bool | None = None,
        teams: list[Team] | None = None,
        output_name: str | None = None,
    ) -> pl.DataFrame:
        save = self.cfg.default_save if save is None else save
        run_id = new_run_id()
        if filename is None:
            return self.run_many(save=save, teams=teams, output_name=output_name, run_id=run_id)
        if self._is_match_sequence(filename):
            return self.run_many(
                matches=list(filename),
                save=save,
                teams=teams,
                output_name=output_name,
                run_id=run_id,
            )
        return self._run_one(
            filename,
            save=save,
            teams=teams,
            run_id=run_id,
            run_mode="single_match",
        )

    def run_many(
        self,
        matches: Sequence[str | int] | None = None,
        *,
        save: bool | None = None,
        teams: list[Team] | None = None,
        output_name: str | None = None,
        run_id: str | None = None,
    ) -> pl.DataFrame:
        run_id = new_run_id() if run_id is None else run_id
        save = self.cfg.default_save if save is None else save
        selected_teams = self._validate_teams(teams)
        discover_start = perf_counter()
        match_ids = self._discover_match_ids(matches)
        discover_duration = perf_counter() - discover_start
        if not match_ids:
            raise FileNotFoundError(
                f"No tracking source files found in {self.storage_manager.read_from}. "
                "Expected files like 2562179.jsonl or g2562179.parquet."
            )

        self._log_task(
            run_id=run_id,
            run_mode="multi_match",
            task_scope="pipeline",
            task="discover_match_files",
            duration=discover_duration,
            match_count=len(match_ids),
            teams=selected_teams,
            save=save,
        )

        batch_build_start = perf_counter()
        batches = self._build_compaction_batches(match_ids)
        batch_build_duration = perf_counter() - batch_build_start
        self._log_task(
            run_id=run_id,
            run_mode="multi_match",
            task_scope="pipeline",
            task="build_compaction_batches",
            duration=batch_build_duration,
            match_count=len(match_ids),
            teams=selected_teams,
            save=save,
        )
        batch_base_name = output_name or self.cfg.default_output_name

        print(
            "[tracking_engine] START multi-match processing "
            f"matches={len(match_ids)} estimated_batches={len(batches)} "
            f"target_compacted_file_size_mb={self.cfg.target_compacted_file_size_mb}"
        )

        if save:
            return self._run_many_streaming_save(match_ids, selected_teams, batch_base_name, run_id)

        total_start = perf_counter()
        all_batches: list[pl.DataFrame] = []
        for batch_index, batch_match_ids in enumerate(batches, start=1):
            batch_start = perf_counter()
            print(
                "[tracking_engine] START batch "
                f"batch={batch_index}/{len(batches)} matches={len(batch_match_ids)} "
                f"match_ids={batch_match_ids}"
            )

            match_frames = [
                self._run_one(
                    match_id,
                    save=False,
                    teams=selected_teams,
                    run_id=run_id,
                    run_mode="multi_match",
                )
                for match_id in batch_match_ids
            ]
            batch_df = pl.concat(match_frames).sort(list(self.cfg.warehouse_sort_columns))

            batch_duration = perf_counter() - batch_start
            print(
                "[tracking_engine] DONE batch "
                f"batch={batch_index}/{len(batches)} rows={len(batch_df):,} "
                f"duration={batch_duration:.2f}s"
            )
            self._log_task(
                run_id=run_id,
                run_mode="multi_match",
                task_scope="batch",
                task="batch_total",
                duration=batch_duration,
                file_path=self.storage_manager.export_to,
                rows=len(batch_df),
                batch_index=batch_index,
                match_count=len(batch_match_ids),
                match_ids_csv=",".join(batch_match_ids),
                teams=selected_teams,
                save=save,
            )
            all_batches.append(batch_df)

        combined = pl.concat(all_batches) if len(all_batches) > 1 else all_batches[0]
        total_duration = perf_counter() - total_start
        print(
            "[tracking_engine] DONE multi-match processing "
            f"matches={len(match_ids)} rows={len(combined):,} duration={total_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode="multi_match",
            task_scope="pipeline",
            task="multi_match_total",
            duration=total_duration,
            file_path=self.storage_manager.export_to,
            rows=len(combined),
            match_count=len(match_ids),
            match_ids_csv=",".join(match_ids),
            teams=selected_teams,
            save=save,
        )
        return combined

    def _run_many_streaming_save(
        self,
        match_ids: list[str],
        teams: list[Team],
        batch_base_name: str,
        run_id: str,
    ) -> pl.DataFrame:
        total_start = perf_counter()
        current_batch_paths: list[Path] = []
        current_batch_matches: list[int] = []
        current_batch_rows = 0
        current_batch_size_mb = 0.0
        batch_index = 1
        summaries: list[dict[str, object]] = []

        for match_id in match_ids:
            processed_local_path, _ = self.storage_manager.prepare_output_path(
                f"{match_id}_processed.parquet",
                intermediate=not self.storage_manager.is_local_export,
            )
            processed_summary = self._save_processed_match_lazy(
                match_id,
                teams=teams,
                run_id=run_id,
                run_mode="multi_match",
                output_path=processed_local_path,
            )
            opta_match_id = int(processed_summary["opta_match_id"])

            processed_size_mb = float(processed_summary["size_mb"])
            if (
                current_batch_paths
                and current_batch_size_mb + processed_size_mb > self.cfg.target_compacted_file_size_mb
            ):
                summaries.append(
                    self._compact_saved_batch(
                        batch_index,
                        current_batch_paths,
                        current_batch_matches,
                        current_batch_rows,
                        batch_base_name,
                        run_id,
                        teams,
                    )
                )
                batch_index += 1
                current_batch_paths = []
                current_batch_matches = []
                current_batch_rows = 0
                current_batch_size_mb = 0.0

            current_batch_paths.append(processed_local_path)
            current_batch_matches.append(opta_match_id)
            current_batch_rows += int(processed_summary["rows"])
            current_batch_size_mb += processed_size_mb

        if current_batch_paths:
            summaries.append(
                self._compact_saved_batch(
                    batch_index,
                    current_batch_paths,
                    current_batch_matches,
                    current_batch_rows,
                    batch_base_name,
                    run_id,
                    teams,
                )
            )

        total_duration = perf_counter() - total_start
        print(
            "[tracking_engine] DONE multi-match processing "
            f"matches={len(match_ids)} batches={len(summaries)} duration={total_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode="multi_match",
            task_scope="pipeline",
            task="multi_match_total",
            duration=total_duration,
            file_path=self.storage_manager.export_to,
            match_count=len(match_ids),
            match_ids_csv=",".join(match_ids),
            teams=teams,
            save=True,
        )
        return pl.DataFrame(summaries)

    def _compact_saved_batch(
        self,
        batch_index: int,
        processed_paths: list[Path],
        match_ids: list[int],
        row_count: int,
        batch_base_name: str,
        run_id: str,
        teams: list[Team],
    ) -> dict[str, object]:
        batch_start = perf_counter()
        print(
            "[tracking_engine] START compact_batch "
            f"batch={batch_index} matches={len(match_ids)} match_ids={match_ids}"
        )

        if batch_index == 1 and len(processed_paths) == 1:
            output_filename = f"{batch_base_name}.parquet"
        else:
            output_filename = f"{batch_base_name}_batch_{batch_index:03d}.parquet"

        output_path, output_reference = self.storage_manager.prepare_output_path(output_filename)
        batch_lazy = pl.concat([pl.scan_parquet(str(path)) for path in processed_paths]).sort(
            list(self.cfg.warehouse_sort_columns)
        )
        final_output_reference = self._write_output_lazy(batch_lazy, output_path, output_reference)

        batch_duration = perf_counter() - batch_start
        size_mb = output_path.stat().st_size / 1e6
        print(
            "[tracking_engine] DONE compact_batch "
            f"batch={batch_index} rows={row_count:,} duration={batch_duration:.2f}s "
            f"path={final_output_reference}"
        )
        self._log_task(
            run_id=run_id,
            run_mode="multi_match",
            task_scope="batch",
            task="compact_batch",
            duration=batch_duration,
            file_path=final_output_reference,
            output_file_path=final_output_reference,
            rows=row_count,
            batch_index=batch_index,
            match_count=len(match_ids),
            match_ids_csv=",".join(str(match_id) for match_id in match_ids),
            teams=teams,
            save=True,
        )
        return {
            "batch_index": batch_index,
            "output_path": final_output_reference,
            "match_count": len(match_ids),
            "matches": ",".join(str(match_id) for match_id in match_ids),
            "rows": row_count,
            "size_mb": round(size_mb, 2),
            "duration_s": round(batch_duration, 2),
        }

    def _run_one(
        self,
        filename: str | int,
        *,
        save: bool | None = None,
        teams: list[Team] | None = None,
        save_path: Path | None = None,
        return_df: bool = True,
        run_id: str | None = None,
        run_mode: str = "single_match",
    ) -> pl.DataFrame | None:
        run_id = new_run_id() if run_id is None else run_id
        save = self.cfg.default_save if save is None else save
        selected_teams = self._validate_teams(teams)
        total_start = perf_counter()
        match_files = self.storage_manager.resolve_match_files(filename)
        opta_match_id = match_files.opta_match_id

        print(f"[tracking_engine] START opta_match_id={opta_match_id}")

        resolve_start = perf_counter()
        source_path, source_status = resolve_tracking_source(
            match_files.jsonl_path,
            match_files.parquet_path,
            match_files.ndjson_path,
        )
        source_display_path = match_files.display_for(source_path)
        resolve_duration = perf_counter() - resolve_start
        print(
            "[tracking_engine] task=resolve_tracking_source "
            f"opta_match_id={opta_match_id} duration={resolve_duration:.2f}s "
            f"status={source_status}"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="resolve_tracking_source",
            duration=resolve_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            source_status=source_status,
            teams=selected_teams,
            save=save,
        )

        metadata_start = perf_counter()
        metadata = load_match_metadata(self.storage_manager, opta_match_id, self.cfg)
        metadata_duration = perf_counter() - metadata_start
        print(
            "[tracking_engine] task=load_match_metadata "
            f"opta_match_id={opta_match_id} duration={metadata_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="load_match_metadata",
            duration=metadata_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            teams=selected_teams,
            save=save,
        )

        extract_start = perf_counter()
        frame_rows = self._build_match_rows_lazy(source_path, opta_match_id, selected_teams).collect(
            streaming=True
        )
        extract_duration = perf_counter() - extract_start
        print(
            "[tracking_engine] task=extract_rows "
            f"opta_match_id={opta_match_id} duration={extract_duration:.2f}s rows={len(frame_rows):,}"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="extract_rows",
            duration=extract_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )

        metadata_enrich_start = perf_counter()
        frame_rows = enrich_tracking_rows(frame_rows.lazy(), metadata, self.model).collect(streaming=True)
        metadata_enrich_duration = perf_counter() - metadata_enrich_start
        print(
            "[tracking_engine] task=metadata_enrichment "
            f"opta_match_id={opta_match_id} duration={metadata_enrich_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="metadata_enrichment",
            duration=metadata_enrich_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )

        zone_start = perf_counter()
        frame_rows = add_zone_columns(frame_rows, self.cfg)
        zone_duration = perf_counter() - zone_start
        print(
            "[tracking_engine] task=zone_columns "
            f"opta_match_id={opta_match_id} duration={zone_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="zone_columns",
            duration=zone_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )

        storage_start = perf_counter()
        frame_rows = add_storage_layout_columns(frame_rows, self.cfg)
        frame_rows = select_output_columns(frame_rows, self.model, self.cfg)
        storage_duration = perf_counter() - storage_start
        print(
            "[tracking_engine] task=frame_bucket_and_types "
            f"opta_match_id={opta_match_id} duration={storage_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="frame_bucket_and_types",
            duration=storage_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )

        sort_start = perf_counter()
        frame_rows = frame_rows.sort(list(self.cfg.warehouse_sort_columns))
        sort_duration = perf_counter() - sort_start
        print(
            "[tracking_engine] task=sort "
            f"opta_match_id={opta_match_id} duration={sort_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="sort",
            duration=sort_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )

        final_output_reference: str | None = None
        if save:
            save_start = perf_counter()
            if save_path is None:
                output_filename = f"{extract_match_id(filename)}_processed.parquet"
                output_path, output_reference = self.storage_manager.prepare_output_path(output_filename)
            else:
                output_path = save_path
                output_reference = str(save_path)
            final_output_reference = self._write_output(frame_rows, output_path, output_reference)
            save_duration = perf_counter() - save_start
            print(
                "[tracking_engine] task=save "
                f"opta_match_id={opta_match_id} duration={save_duration:.2f}s path={final_output_reference}"
            )
            self._log_task(
                run_id=run_id,
                run_mode=run_mode,
                task_scope="match",
                task="save",
                duration=save_duration,
                opta_match_id=opta_match_id,
                file_path=final_output_reference,
                source_file_path=source_display_path,
                output_file_path=final_output_reference,
                rows=len(frame_rows),
                teams=selected_teams,
                save=save,
            )

        total_duration = perf_counter() - total_start
        print(
            "[tracking_engine] DONE opta_match_id="
            f"{opta_match_id} rows={len(frame_rows):,} total_duration={total_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="match_total",
            duration=total_duration,
            opta_match_id=opta_match_id,
            file_path=Path(final_output_reference or source_display_path),
            source_file_path=source_display_path,
            output_file_path=final_output_reference if final_output_reference is not None else None,
            rows=len(frame_rows),
            teams=selected_teams,
            save=save,
        )
        if return_df:
            return frame_rows
        return None

    def _save_processed_match_lazy(
        self,
        filename: str | int,
        *,
        teams: list[Team],
        run_id: str,
        run_mode: str,
        output_path: Path,
    ) -> dict[str, object]:
        total_start = perf_counter()
        match_files = self.storage_manager.resolve_match_files(filename)
        opta_match_id = match_files.opta_match_id

        print(f"[tracking_engine] START opta_match_id={opta_match_id}")

        resolve_start = perf_counter()
        source_path, source_status = resolve_tracking_source(
            match_files.jsonl_path,
            match_files.parquet_path,
            match_files.ndjson_path,
        )
        source_display_path = match_files.display_for(source_path)
        resolve_duration = perf_counter() - resolve_start
        print(
            "[tracking_engine] task=resolve_tracking_source "
            f"opta_match_id={opta_match_id} duration={resolve_duration:.2f}s "
            f"status={source_status}"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="resolve_tracking_source",
            duration=resolve_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            source_status=source_status,
            teams=teams,
            save=True,
        )

        metadata_start = perf_counter()
        metadata = load_match_metadata(self.storage_manager, opta_match_id, self.cfg)
        metadata_duration = perf_counter() - metadata_start
        print(
            "[tracking_engine] task=load_match_metadata "
            f"opta_match_id={opta_match_id} duration={metadata_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="load_match_metadata",
            duration=metadata_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            teams=teams,
            save=True,
        )

        build_start = perf_counter()
        lazy_output = self._build_lazy_match_output(
            source_path,
            opta_match_id,
            teams,
            metadata,
            sort_output=False,
        )
        build_duration = perf_counter() - build_start
        print(
            "[tracking_engine] task=build_lazy_match_output "
            f"opta_match_id={opta_match_id} duration={build_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="build_lazy_match_output",
            duration=build_duration,
            opta_match_id=opta_match_id,
            file_path=source_display_path,
            source_file_path=source_display_path,
            teams=teams,
            save=True,
        )

        save_start = perf_counter()
        self._write_output_lazy(lazy_output, output_path, str(output_path))
        save_duration = perf_counter() - save_start
        row_count = count_parquet_rows(output_path)
        size_mb = output_path.stat().st_size / 1e6
        print(
            "[tracking_engine] task=save_lazy_match "
            f"opta_match_id={opta_match_id} duration={save_duration:.2f}s path={output_path.name}"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="save_lazy_match",
            duration=save_duration,
            opta_match_id=opta_match_id,
            file_path=output_path,
            source_file_path=source_display_path,
            output_file_path=output_path,
            rows=row_count,
            teams=teams,
            save=True,
        )

        total_duration = perf_counter() - total_start
        print(
            "[tracking_engine] DONE opta_match_id="
            f"{opta_match_id} rows={row_count:,} total_duration={total_duration:.2f}s"
        )
        self._log_task(
            run_id=run_id,
            run_mode=run_mode,
            task_scope="match",
            task="match_total",
            duration=total_duration,
            opta_match_id=opta_match_id,
            file_path=output_path,
            source_file_path=source_display_path,
            output_file_path=output_path,
            rows=row_count,
            teams=teams,
            save=True,
        )
        return {
            "opta_match_id": opta_match_id,
            "rows": row_count,
            "size_mb": round(size_mb, 2),
            "duration_s": round(total_duration, 2),
        }

    def _build_lazy_match_output(
        self,
        source_path: Path,
        opta_match_id: int,
        teams: list[Team],
        metadata,
        *,
        sort_output: bool,
    ) -> pl.LazyFrame:
        output = self._build_match_rows_lazy(source_path, opta_match_id, teams)
        output = enrich_tracking_rows(output, metadata, self.model)
        output = add_zone_columns(output, self.cfg)
        output = add_storage_layout_columns(output, self.cfg)
        output = select_output_columns(output, self.model, self.cfg)
        if sort_output:
            return output.sort(list(self.cfg.warehouse_sort_columns))
        return output

    def _build_match_rows_lazy(
        self,
        source_path: Path,
        opta_match_id: int,
        teams: list[Team],
    ) -> pl.LazyFrame:
        raw = self._scan_base_tracking(source_path, teams)
        frames = []
        if "home" in teams:
            frames.append(self._process_team(raw, opta_match_id, "homePlayers", "home"))
        if "away" in teams:
            frames.append(self._process_team(raw, opta_match_id, "awayPlayers", "away"))
        if not frames:
            raise ValueError("At least one team must be selected.")
        return pl.concat(frames)

    def _discover_match_ids(self, matches: Sequence[str | int] | None) -> list[str]:
        if matches is None:
            return self.storage_manager.discover_match_ids()
        seen: set[str] = set()
        ordered: list[str] = []
        for value in matches:
            match_id = extract_match_id(value)
            if match_id not in seen:
                ordered.append(match_id)
                seen.add(match_id)
        return ordered

    def _build_compaction_batches(self, match_ids: list[str]) -> list[list[str]]:
        batches: list[list[str]] = []
        current_batch: list[str] = []
        current_size_mb = 0.0

        for match_id in match_ids:
            file_size_mb = self.storage_manager.source_file_size_mb(match_id)
            if current_batch and current_size_mb + file_size_mb > self.cfg.target_compacted_file_size_mb:
                batches.append(current_batch)
                current_batch = []
                current_size_mb = 0.0
            current_batch.append(match_id)
            current_size_mb += file_size_mb

        if current_batch:
            batches.append(current_batch)
        return batches

    @staticmethod
    def _scan_base_tracking(
        source_path: Path,
        teams: list[Team],
    ) -> pl.LazyFrame:
        projected_columns: list[pl.Expr] = [
            pl.col("period"),
            pl.col("frameIdx"),
            pl.col("gameClock"),
            pl.col("wallClock"),
            pl.col("live"),
            pl.col("lastTouch"),
            pl.col("ball").struct.field("xyz").list.get(0).alias("ball_x"),
            pl.col("ball").struct.field("xyz").list.get(1).alias("ball_y"),
            pl.col("ball").struct.field("xyz").list.get(2).alias("ball_z"),
            pl.col("ball").struct.field("speed").alias("ball_speed"),
        ]
        if "home" in teams:
            projected_columns.append(pl.col("homePlayers"))
        if "away" in teams:
            projected_columns.append(pl.col("awayPlayers"))
        return scan_tracking(source_path).select(projected_columns)

    def _process_team(
        self,
        lf: pl.LazyFrame,
        opta_match_id: int,
        explode_col: str,
        team: Team,
    ) -> pl.LazyFrame:
        return (
            lf.explode(explode_col)
            .unnest(explode_col)
            .select([
                pl.lit(opta_match_id).alias("opta_match_id"),
                pl.lit(team).alias("team"),
                pl.col("period"),
                pl.col("frameIdx").alias("frame_id"),
                pl.col("gameClock").alias("game_clock"),
                pl.col("wallClock").alias("wall_clock"),
                pl.col("live"),
                pl.col("lastTouch").alias("last_touch"),
                pl.col("optaId").cast(pl.Int64, strict=False).alias("opta_player_id"),
                pl.col("playerId").alias("player_id"),
                pl.col("number").alias("player_number"),
                pl.col("xyz").list.get(0).alias("player_x"),
                pl.col("xyz").list.get(1).alias("player_y"),
                pl.col("xyz").list.get(2).alias("player_z"),
                pl.col("speed").alias("player_speed"),
                pl.col("ball_x"),
                pl.col("ball_y"),
                pl.col("ball_z"),
                pl.col("ball_speed"),
            ])
        )

    def _write_output(self, df: pl.DataFrame, local_path: Path, final_reference: str) -> str:
        write_tracking(df, local_path, self.cfg)
        return self.storage_manager.finalize_output(local_path, final_reference)

    def _write_output_lazy(self, lf: pl.LazyFrame, local_path: Path, final_reference: str) -> str:
        write_tracking_lazy(lf, local_path, self.cfg)
        return self.storage_manager.finalize_output(local_path, final_reference)

    def query_logs(
        self,
        sql: str | None = None,
        log_path: str | Path | None = None,
    ) -> pl.DataFrame:
        return query_performance_logs(Path(log_path) if log_path is not None else self.log_path, sql)

    def _log_task(
        self,
        *,
        run_id: str,
        run_mode: str,
        task_scope: str,
        task: str,
        duration: float,
        opta_match_id: int | None = None,
        file_path: str | Path | None = None,
        source_file_path: str | Path | None = None,
        output_file_path: str | Path | None = None,
        rows: int | None = None,
        status: str = "success",
        source_status: str | None = None,
        batch_index: int | None = None,
        match_count: int | None = None,
        match_ids_csv: str | None = None,
        teams: list[Team] | None = None,
        save: bool | None = None,
    ) -> None:
        append_performance_log(
            self.log_path,
            {
                "logged_at_utc": utc_now_iso(),
                "run_id": run_id,
                "run_mode": run_mode,
                "task_scope": task_scope,
                "task": task,
                "duration_seconds": round(duration, 4),
                "duration_text": f"{duration:.2f}s",
                "status": status,
                "source_status": source_status,
                "opta_match_id": opta_match_id,
                "rows": rows,
                "file_path": str(file_path) if file_path is not None else None,
                "source_file_path": (
                    str(source_file_path) if source_file_path is not None else None
                ),
                "output_file_path": (
                    str(output_file_path) if output_file_path is not None else None
                ),
                "batch_index": batch_index,
                "match_count": match_count,
                "match_ids_csv": match_ids_csv,
                "teams": ",".join(teams) if teams is not None else None,
                "save": save,
            },
        )

    @staticmethod
    def _validate_teams(teams: list[Team] | None) -> list[Team]:
        selected_teams = ["home", "away"] if teams is None else teams
        invalid_teams = sorted(set(selected_teams) - {"home", "away"})
        if invalid_teams:
            raise ValueError(f"Unsupported teams: {invalid_teams}. Use 'home' and/or 'away'.")
        return selected_teams

    @staticmethod
    def _validate_model(model: str) -> str:
        if model not in SUPPORTED_MODELS:
            raise ValueError(f"Unsupported model '{model}'. Use one of: {', '.join(SUPPORTED_MODELS)}.")
        return model

    @staticmethod
    def _is_match_sequence(value: object) -> bool:
        return isinstance(value, (list, tuple, set))


