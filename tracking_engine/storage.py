from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

import yaml

DEFAULT_CONFIG_PATH = Path("config.yaml")
SUPPORTED_STORAGE_PROFILES = ("local", "aws_s3", "azure_blob", "adls")


@dataclass(frozen=True)
class StorageProfile:
    name: str
    read_from: str
    export_to: str
    metadata_from: str
    storage_options: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MatchFiles:
    opta_match_id: int
    jsonl_path: Path
    ndjson_path: Path
    parquet_path: Path
    display_paths: dict[str, str]

    def display_for(self, path: Path) -> str:
        return self.display_paths.get(str(path), str(path))


def extract_match_id(value: str | int) -> str:
    raw = str(value).strip()
    name = Path(raw).name
    for suffix in (".jsonl", ".ndjson", ".parquet", ".json"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    if name.startswith("g") and name[1:].isdigit():
        return name[1:]
    if name.isdigit():
        return name
    raise ValueError(
        "Expected a numeric Opta match identifier or a filename like "
        "'2562179.jsonl' or 'g2562179.parquet'."
    )


def load_storage_profile(
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    storage: str = "local",
    *,
    tracking_dir: str | Path | None = None,
    export_dir: str | Path | None = None,
    metadata_dir: str | Path | None = None,
) -> StorageProfile:
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Storage config not found: {config_path}")

    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if storage not in config:
        raise KeyError(
            f"Storage profile '{storage}' not found in {config_path}. "
            f"Available profiles: {sorted(config)}"
        )

    section = dict(config[storage] or {})
    read_from = str(tracking_dir) if tracking_dir is not None else str(section.get("read_from", "")).strip()
    export_to = str(export_dir) if export_dir is not None else str(section.get("export_to", "")).strip()
    metadata_from = (
        str(metadata_dir)
        if metadata_dir is not None
        else str(section.get("metadata_from") or export_to or read_from).strip()
    )
    raw_storage_options = section.get("storage_options") or {}
    if not isinstance(raw_storage_options, dict):
        raise ValueError(
            f"Storage profile '{storage}' must define storage_options as a mapping in {config_path}."
        )
    storage_options = dict(raw_storage_options)
    if not read_from or not export_to:
        raise ValueError(
            f"Storage profile '{storage}' must define read_from and export_to in {config_path}."
        )
    return StorageProfile(
        name=storage,
        read_from=read_from,
        export_to=export_to,
        metadata_from=metadata_from,
        storage_options=storage_options,
    )


class StorageManager:
    def __init__(self, profile: StorageProfile):
        self.profile = profile
        self._temp_dir = TemporaryDirectory(prefix="tracking_engine_")

    @classmethod
    def from_yaml(
        cls,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        storage: str = "local",
        *,
        tracking_dir: str | Path | None = None,
        export_dir: str | Path | None = None,
        metadata_dir: str | Path | None = None,
    ) -> "StorageManager":
        profile = load_storage_profile(
            config_path=config_path,
            storage=storage,
            tracking_dir=tracking_dir,
            export_dir=export_dir,
            metadata_dir=metadata_dir,
        )
        return cls(profile)

    @property
    def read_from(self) -> str:
        return self.profile.read_from

    @property
    def export_to(self) -> str:
        return self.profile.export_to

    @property
    def metadata_from(self) -> str:
        return self.profile.metadata_from

    @property
    def storage_options(self) -> dict[str, object]:
        return self.profile.storage_options

    @property
    def is_local_read(self) -> bool:
        return not self._is_remote(self.profile.read_from)

    @property
    def is_local_export(self) -> bool:
        return not self._is_remote(self.profile.export_to)

    def default_log_path(self) -> Path:
        if self.is_local_export:
            export_dir = Path(self.profile.export_to)
            export_dir.mkdir(parents=True, exist_ok=True)
            return export_dir / "tracking_engine_performance.jsonl"
        return Path.cwd() / "tracking_engine_performance.jsonl"

    def discover_match_ids(self) -> list[str]:
        if self.is_local_read:
            read_dir = Path(self.profile.read_from)
            if not read_dir.exists():
                raise FileNotFoundError(f"Tracking read_from directory not found: {read_dir}")
            match_ids = {
                extract_match_id(path.name)
                for path in read_dir.iterdir()
                if path.is_file() and self._is_supported_tracking_name(path.name)
            }
            return sorted(match_ids, key=int)

        fs, base_path = self._filesystem(self.profile.read_from)
        match_ids = {
            extract_match_id(Path(entry).name)
            for entry in fs.glob(f"{base_path.rstrip('/') }/*")
            if self._is_supported_tracking_name(Path(entry).name)
        }
        return sorted(match_ids, key=int)

    def resolve_match_files(self, match_ref: str | int) -> MatchFiles:
        match_id = extract_match_id(match_ref)
        opta_match_id = int(match_id)
        display_paths: dict[str, str] = {}

        if self.is_local_read:
            read_dir = Path(self.profile.read_from)
            jsonl_path, jsonl_display = self._resolve_local_candidate(read_dir, match_id, ".jsonl")
            ndjson_path, ndjson_display = self._resolve_local_candidate(read_dir, match_id, ".ndjson")
            parquet_path, parquet_display = self._resolve_local_candidate(read_dir, match_id, ".parquet")
        else:
            jsonl_path, jsonl_display = self._stage_remote_candidate(match_id, ".jsonl")
            ndjson_path, ndjson_display = self._stage_remote_candidate(match_id, ".ndjson")
            parquet_path, parquet_display = self._stage_remote_candidate(match_id, ".parquet")

        for local_path, display in (
            (jsonl_path, jsonl_display),
            (ndjson_path, ndjson_display),
            (parquet_path, parquet_display),
        ):
            display_paths[str(local_path)] = display

        if not jsonl_path.exists() and not ndjson_path.exists() and not parquet_path.exists():
            raise FileNotFoundError(
                f"No tracking file found for opta_match_id={match_id} in {self.profile.read_from}. "
                "Expected .jsonl, .ndjson, or .parquet."
            )

        return MatchFiles(
            opta_match_id=opta_match_id,
            jsonl_path=jsonl_path,
            ndjson_path=ndjson_path,
            parquet_path=parquet_path,
            display_paths=display_paths,
        )

    def source_file_size_mb(self, match_ref: str | int) -> float:
        match_id = extract_match_id(match_ref)
        if not self.is_local_read:
            fs, base_path = self._filesystem(self.profile.read_from)
            for suffix in (".parquet", ".jsonl", ".ndjson"):
                for name in self._candidate_names(match_id, suffix):
                    remote_path = self._remote_join(base_path, name)
                    if fs.exists(remote_path):
                        return fs.size(remote_path) / 1e6

        match_files = self.resolve_match_files(match_ref)
        for path in (match_files.parquet_path, match_files.jsonl_path, match_files.ndjson_path):
            if path.exists():
                return path.stat().st_size / 1e6
        raise FileNotFoundError(f"Unable to find a source file for {match_ref}.")

    def resolve_metadata_path(self, match_ref: str | int) -> Path | None:
        match_id = extract_match_id(match_ref)
        metadata_candidates = [
            f"{match_id}.json",
            f"g{match_id}.json",
            f"{match_id}_SecondSpectrum_Metadata.json",
            f"g{match_id}_SecondSpectrum_Metadata.json",
            f"{match_id}_Metadata.json",
            f"g{match_id}_Metadata.json",
        ]

        if not self._is_remote(self.profile.metadata_from):
            metadata_dir = Path(self.profile.metadata_from)
            for name in metadata_candidates:
                candidate = metadata_dir / name
                if candidate.exists():
                    return candidate
            return None

        fs, base_path = self._filesystem(self.profile.metadata_from)
        for name in metadata_candidates:
            remote_path = self._remote_join(base_path, name)
            if fs.exists(remote_path):
                local_path = self._temp_path("metadata", name)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                fs.get(remote_path, str(local_path))
                return local_path
        return None

    def prepare_output_path(self, filename: str, *, intermediate: bool = False) -> tuple[Path, str]:
        if self.is_local_export or intermediate:
            target_dir = Path(self.profile.export_to) if self.is_local_export else self._temp_path("export")
            target_dir.mkdir(parents=True, exist_ok=True)
            local_path = target_dir / filename
            return local_path, str(local_path)

        temp_path = self._temp_path("export", filename)
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        return temp_path, self._remote_uri(self.profile.export_to, filename)

    def finalize_output(self, local_path: Path, final_reference: str) -> str:
        if self.is_local_export or not self._is_remote(final_reference):
            return str(local_path)

        fs, remote_path = self._filesystem(final_reference)
        parent = str(Path(remote_path).parent).replace("\\", "/")
        if parent not in ("", "."):
            fs.makedirs(parent, exist_ok=True)
        fs.put(str(local_path), remote_path)
        return final_reference

    @staticmethod
    def _is_supported_tracking_name(name: str) -> bool:
        stem = Path(name).stem
        if "_" in stem:
            return False
        try:
            extract_match_id(name)
        except ValueError:
            return False
        return Path(name).suffix in {".jsonl", ".ndjson", ".parquet"}

    @staticmethod
    def _is_remote(value: str) -> bool:
        return "://" in value

    @staticmethod
    def _candidate_names(match_id: str, suffix: str) -> Iterable[str]:
        yield f"{match_id}{suffix}"
        yield f"g{match_id}{suffix}"

    def _resolve_local_candidate(self, directory: Path, match_id: str, suffix: str) -> tuple[Path, str]:
        for name in self._candidate_names(match_id, suffix):
            candidate = directory / name
            if candidate.exists():
                return candidate, str(candidate)
        canonical = directory / f"{match_id}{suffix}"
        return canonical, str(canonical)

    def _stage_remote_candidate(self, match_id: str, suffix: str) -> tuple[Path, str]:
        fs, base_path = self._filesystem(self.profile.read_from)
        for name in self._candidate_names(match_id, suffix):
            remote_path = self._remote_join(base_path, name)
            if fs.exists(remote_path):
                local_path = self._temp_path("read", name)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                fs.get(remote_path, str(local_path))
                return local_path, self._remote_uri(self.profile.read_from, name)
        canonical = self._temp_path("read", f"{match_id}{suffix}")
        return canonical, self._remote_uri(self.profile.read_from, f"{match_id}{suffix}")

    def _temp_path(self, *parts: str) -> Path:
        return Path(self._temp_dir.name, *parts)

    @staticmethod
    def _remote_join(base_path: str, name: str) -> str:
        return f"{base_path.rstrip('/')}/{name}"

    @staticmethod
    def _remote_uri(base_uri: str, name: str) -> str:
        return f"{base_uri.rstrip('/')}/{name}"

    def _filesystem(self, uri: str):
        try:
            import fsspec
        except ImportError as exc:
            raise ImportError(
                "Remote storage support requires 'fsspec'. For S3 install 's3fs'; "
                "for Azure Blob or ADLS install 'adlfs'."
            ) from exc

        return fsspec.core.url_to_fs(uri, **self.profile.storage_options)
