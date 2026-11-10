"""Bronze preservation, reruns, revisions and failure isolation."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from bronze import ingest, manifest, source
from common.config import load_config
from common.contracts import RunContext

HEADER = '"Dokumentnummer";"Art";"Rundvekt";"Mottakende fart.type"\n'
BODY = '"0001";"Sild; fersk";"14,50";""\n"0002";"Torsk ";"0";"B\u00e5t"\n'


@pytest.fixture
def setup_source(tmp_path):
    origin = tmp_path / "origin"
    origin.mkdir()
    cfg = load_config("test")
    cfg["storage"]["root"] = str(tmp_path / "storage")
    cfg["source"]["periods"] = ["2023"]
    cfg["source"]["url_template"] = origin.as_uri() + "/fangstdata_{period}.csv.zip"
    cfg["source"]["expected_columns"] = [
        "Dokumentnummer",
        "Art",
        "Rundvekt",
        "Mottakende fart.type",
    ]

    def make_archive(content=HEADER + BODY, period="2023", member=None):
        archive = origin / f"fangstdata_{period}.csv.zip"
        with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
            handle.writestr(member or f"fangstdata_{period}.csv", content.encode("utf-8"))
        return archive

    def context(run_id="test-run"):
        return RunContext(run_id, "test", datetime(2026, 11, 5, tzinfo=timezone.utc), cfg)

    return cfg, make_archive, context


def test_source_preserves_bytes_and_text(setup_source, tmp_path):
    _, make_archive, context = setup_source
    archive = make_archive()
    work = tmp_path / "work"
    work.mkdir()
    fetched = source.download("2023", work, context())
    inspected = source.extract_and_inspect(fetched, work)
    assert fetched.archive.read_bytes() == archive.read_bytes()
    assert fetched.sha256 == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert inspected.path.read_bytes() == (HEADER + BODY).encode("utf-8")
    assert inspected.rows == 2
    assert inspected.columns == ["Dokumentnummer", "Art", "Rundvekt", "Mottakende fart.type"]


@pytest.mark.parametrize(
    "content",
    [
        "",
        "a;a\n1;2\n",
        "a;b\n1\n",
        "a;b\n1;2;3\n",
        "_run_id;a\n1;2\n",
        'a;b\n1;"unterminated',
        "a;b\n1;\x00\n",
    ],
)
def test_invalid_csv_fails_before_spark(setup_source, tmp_path, content):
    _, make_archive, context = setup_source
    make_archive(content)
    work = tmp_path / "work"
    work.mkdir()
    fetched = source.download("2023", work, context())
    with pytest.raises((ValueError, csv.Error)):
        source.extract_and_inspect(fetched, work)


def test_unexpected_zip_member_is_not_extracted(setup_source, tmp_path):
    _, make_archive, context = setup_source
    make_archive(member="../outside.csv")
    work = tmp_path / "work"
    work.mkdir()
    with pytest.raises(ValueError, match="Expected one archive member"):
        source.extract_and_inspect(source.download("2023", work, context()), work)
    assert not (tmp_path / "outside.csv").exists()


def test_annual_period_required(setup_source, tmp_path):
    _, _, context = setup_source
    with pytest.raises(ValueError, match="annual catch period"):
        source.download("2023-01", tmp_path, context())


def test_truncated_download_is_rejected(setup_source, tmp_path, monkeypatch):
    _, _, context = setup_source
    response = io.BytesIO(b"incomplete")
    response.headers = {"Content-Length": "100"}
    monkeypatch.setattr(source, "urlopen", lambda *args, **kwargs: response)
    with pytest.raises(ValueError, match="Incomplete download"):
        source.download("2023", tmp_path, context())


def test_actual_schema_and_values_round_trip(spark, setup_source):
    cfg, make_archive, context = setup_source
    fixture = Path("tests/data/fangstdata_2023_sample.csv").read_text(encoding="utf-8")
    cfg["source"]["expected_columns"] = next(csv.reader(io.StringIO(fixture), delimiter=";"))
    make_archive(fixture)
    result = ingest.run(spark, context("actual-schema"))
    assert result.status == "success", result.message
    frame = spark.read.parquet((Path(result.outputs[0]) / "records").as_posix())
    original = list(csv.DictReader(io.StringIO(fixture), delimiter=";"))
    assert len(original[0]) == 133
    assert len(frame.columns) == 139
    fields = list(original[0])
    expected = sorted(tuple(row[name] for name in fields) for row in original)
    actual = sorted(tuple(row[name] for name in fields) for row in frame.collect())
    assert actual == expected


def test_changed_sample_is_preserved_reported_and_fails_layer(spark, setup_source):
    cfg, make_archive, context = setup_source
    changed_header = HEADER.rstrip("\n") + ';"New field"\n'
    changed_body = BODY.replace("\n", ';"new"\n')
    make_archive(changed_header + changed_body)

    result = ingest.run(spark, context("schema-change"))

    assert result.status == "failed"
    assert result.rows_out == 2
    assert "schema changed" in result.message
    assert "New field" in result.message
    snapshot = Path(result.outputs[0])
    assert (snapshot / "fangstdata_2023.csv").read_text(encoding="utf-8") == (
        changed_header + changed_body
    )
    report = json.loads((Path(cfg["storage"]["root"]) / "manifests/schema-change.json").read_text())
    assert report["files"][0]["schema"]["status"] == "changed"
    assert report["files"][0]["schema"]["added"] == ["New field"]


def test_unchanged_schema_is_in_layer_message(spark, setup_source):
    _, make_archive, context = setup_source
    make_archive()
    result = ingest.run(spark, context("schema-unchanged"))
    assert result.status == "success"
    assert "schema unchanged (4 columns)" in result.message


def test_ingestion_preserves_values_metadata_and_reruns(spark, setup_source):
    cfg, make_archive, context = setup_source
    archive = make_archive()
    result = ingest.run(spark, context("first"))
    assert (result.status, result.rows_in, result.rows_out) == ("success", 2, 2)
    snapshot = Path(result.outputs[0])
    assert (snapshot / archive.name).read_bytes() == archive.read_bytes()
    assert (snapshot / "fangstdata_2023.csv").read_bytes() == (HEADER + BODY).encode("utf-8")
    df = spark.read.parquet((snapshot / "records").as_posix())
    rows = {row["Dokumentnummer"]: row.asDict() for row in df.collect()}
    assert rows["0001"]["Art"] == "Sild; fersk"
    assert rows["0001"]["Rundvekt"] == "14,50"
    assert rows["0001"]["Mottakende fart.type"] == ""
    assert rows["0002"]["Art"] == "Torsk "
    assert rows["0002"]["Mottakende fart.type"] == "B\u00e5t"
    assert rows["0001"]["_source_name"] == "fiskeridir_catch"
    assert rows["0001"]["_source_period"] == "2023"
    assert rows["0001"]["_source_file"] == "fangstdata_2023.csv"
    assert rows["0001"]["_run_id"] == "first"
    assert rows["0001"]["_ingested_at"] is not None
    assert rows["0001"]["_source_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    before = {
        p.relative_to(snapshot): p.stat().st_mtime_ns for p in snapshot.rglob("*") if p.is_file()
    }
    second = ingest.run(spark, context("second"))
    assert second.status == "skipped"
    assert second.rows_out == 0
    assert before == {
        p.relative_to(snapshot): p.stat().st_mtime_ns for p in snapshot.rglob("*") if p.is_file()
    }
    report = json.loads((Path(cfg["storage"]["root"]) / "manifests/second.json").read_text())
    assert report["files"][0]["status"] == "skipped"
    assert report["files"][0]["ingestion_run_id"] == "first"


def test_revision_keeps_previous_snapshot(spark, setup_source):
    _, make_archive, context = setup_source
    make_archive()
    first = ingest.run(spark, context("original"))
    make_archive((HEADER + BODY).replace("14,50", "18,00"))
    revised = ingest.run(spark, context("revised"))
    assert revised.status == first.status == "success"
    assert revised.outputs != first.outputs
    assert all(Path(path).is_dir() for path in first.outputs + revised.outputs)


def test_failure_writes_manifest_without_publishing(spark, setup_source):
    cfg, make_archive, context = setup_source
    make_archive("a;b\n1\n")
    result = ingest.run(spark, context("broken"))
    assert result.status == "failed"
    assert result.outputs == []
    assert not list(manifest.source_path(context()).glob("period=*/source_version=*"))
    report = json.loads((Path(cfg["storage"]["root"]) / "manifests/broken.json").read_text())
    assert report["layers"][0]["status"] == "failed"
    assert report["files"][0]["status"] == "failed"


def test_write_failure_can_be_retried(spark, setup_source, monkeypatch):
    _, make_archive, context = setup_source
    make_archive()
    with monkeypatch.context() as patch:

        def fail(*args):
            raise RuntimeError("simulated metadata failure")

        patch.setattr(ingest.metadata, "add_metadata", fail)
        assert ingest.run(spark, context("failed")).status == "failed"
    assert ingest.run(spark, context("retry")).status == "success"
