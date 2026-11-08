"""Download and inspect annual Fiskeridirektoratet CSV archives."""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

from common.contracts import RunContext

CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class SourceFile:
    period: str
    url: str
    archive: Path
    sha256: str
    fetched_at: str


@dataclass(frozen=True)
class CsvInfo:
    path: Path
    columns: list[str]
    rows: int
    sha256: str


def download(period: str, directory: Path, ctx: RunContext) -> SourceFile:
    """Stream one annual archive to disk; hash exactly the downloaded bytes."""
    if not re.fullmatch(r"20\d{2}", period):
        raise ValueError(f"Expected an annual catch period YYYY, got {period!r}")
    filename = f"fangstdata_{period}.csv.zip"
    url = ctx.config["source"]["url_template"].format(period=period)
    archive = directory / filename
    digest = hashlib.sha256()
    received = 0
    with urlopen(url, timeout=ctx.config["source"].get("timeout_seconds", 120)) as response:
        expected = response.headers.get("Content-Length")
        with archive.open("xb") as target:
            while chunk := response.read(CHUNK_BYTES):
                target.write(chunk)
                digest.update(chunk)
                received += len(chunk)
    if expected is not None and received != int(expected):
        raise ValueError(f"Incomplete download: expected {expected} bytes, received {received}")
    return SourceFile(
        period, url, archive, digest.hexdigest(), datetime.now(timezone.utc).isoformat()
    )


def extract_and_inspect(source: SourceFile, directory: Path) -> CsvInfo:
    """Preserve CSV bytes, validate archive CRC and check every record's width."""
    filename = f"fangstdata_{source.period}.csv"
    csv_path = directory / filename
    digest = hashlib.sha256()
    with ZipFile(source.archive) as archive:
        members = [entry for entry in archive.infolist() if not entry.is_dir()]
        if len(members) != 1 or members[0].filename != filename:
            raise ValueError(f"Expected one archive member named {filename}")
        # Reading the member to EOF checks its CRC. No archive paths are extracted.
        with archive.open(members[0]) as origin, csv_path.open("xb") as target:
            while chunk := origin.read(CHUNK_BYTES):
                target.write(chunk)
                digest.update(chunk)

    with csv_path.open(encoding="utf-8-sig", errors="strict", newline="") as handle:
        reader = csv.reader(handle, delimiter=";", quotechar='"', strict=True)
        columns = next(reader, [])
        if not columns or any(not name or "\x00" in name for name in columns):
            raise ValueError("CSV needs a nonempty header without NUL characters")
        if len(set(columns)) != len(columns) or any(c.startswith("_source_") for c in columns):
            raise ValueError("CSV contains duplicate or reserved column names")
        if {"_run_id", "_ingested_at"} & set(columns):
            raise ValueError("CSV contains reserved metadata column names")
        rows = 0
        for rows, row in enumerate(reader, 1):
            if len(row) != len(columns):
                raise ValueError(
                    f"CSV record {rows}: expected {len(columns)} fields, got {len(row)}"
                )
            if any("\x00" in value for value in row):
                raise ValueError(f"CSV record {rows}: NUL characters are unsupported")
    return CsvInfo(csv_path, columns, rows, digest.hexdigest())
