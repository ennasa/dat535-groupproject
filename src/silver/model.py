"""Proposed Silver record model from A4 profiling.

This module defines the target contract only. Casting, cleaning, duplicate handling
and writing belong to A6/A7. Person B must approve the proposal before A5 is final.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MODEL_VERSION = "a5-proposal-1"
APPROVAL_STATUS = "approved"

FieldRole = Literal["key", "dimension", "measure", "provenance"]


@dataclass(frozen=True)
class SilverField:
    """One direct Bronze-to-Silver field mapping."""

    source: str
    name: str
    data_type: DataType
    nullable: bool
    role: FieldRole


@dataclass(frozen=True)
class ReferenceJoin:
    """A proposed A7 reference-data join; no reference data are joined in A5."""

    name: str
    silver_key: str
    cardinality: Literal["many-to-one"] = "many-to-one"
    join_type: Literal["left"] = "left"
    unmatched_action: Literal["retain-and-flag"] = "retain-and-flag"


def _field(
    source: str,
    name: str,
    data_type: DataType,
    nullable: bool = False,
    role: FieldRole = "dimension",
) -> SilverField:
    return SilverField(source, name, data_type, nullable, role)


TEXT = StringType()
INTEGER = IntegerType()
LONG = LongType()
DATE = DateType()
TIMESTAMP = TimestampType()
BOOLEAN = BooleanType()
DECIMAL = DecimalType(24, 6)


SILVER_FIELDS: tuple[SilverField, ...] = (
    # Grain and document identity
    _field("Fangstår", "catch_year", INTEGER, role="key"),
    _field("Salgslag (kode)", "sales_org_code", TEXT, role="key"),
    _field("Dokumenttype (kode)", "document_type_code", TEXT, role="key"),
    _field("Dokumentnummer", "document_number", TEXT, role="key"),
    _field("Linjenummer", "line_number", INTEGER, role="key"),
    _field("Salgslag", "sales_org_name", TEXT),
    _field("Dokumenttype", "document_type_name", TEXT),
    _field("Dokument versjonsnummer", "document_version_number", INTEGER),
    _field("Dokument salgsdato", "document_sale_date", DATE),
    _field("Dokument versjonstidspunkt", "document_version_at_utc", TIMESTAMP),
    _field("Oppdateringstidspunkt", "source_updated_date", DATE),
    # Landing geography and time
    _field("Mottaksstasjon", "landing_station", TEXT, nullable=True),
    _field("Landingskommune (kode)", "landing_municipality_code", TEXT, nullable=True),
    _field("Landingskommune", "landing_municipality_name", TEXT, nullable=True),
    _field("Landingsfylke (kode)", "landing_county_code", TEXT, nullable=True),
    _field("Landingsfylke", "landing_county_name", TEXT, nullable=True),
    _field("Landingsnasjon (kode)", "landing_country_code", TEXT),
    _field("Landingsnasjon", "landing_country_name", TEXT),
    _field("Landingsdato", "landing_date", DATE),
    # Spark has no time-only type. Keep the source time without inventing a time zone.
    _field("Landingsklokkeslett", "landing_time_local", TEXT),
    # Code 13 means delivered in the following year, so this is not an integer month.
    _field("Landingsmåned (kode)", "landing_month_code", TEXT),
    _field("Dellanding (signal)", "is_partial_landing", BOOLEAN),
    # Vessel
    _field("Fartøy ID", "vessel_id", TEXT, nullable=True),
    _field("Registreringsmerke (seddel)", "vessel_registration", TEXT),
    _field("Fartøynavn", "vessel_name", TEXT, nullable=True),
    _field("Fartøytype (kode)", "vessel_type_code", TEXT),
    _field("Fartøytype", "vessel_type_name", TEXT),
    _field("Fartøynasjonalitet (kode)", "vessel_country_code", TEXT),
    _field("Fartøynasjonalitet", "vessel_country_name", TEXT),
    _field("Største lengde", "vessel_length_m", DECIMAL, nullable=True),
    # Catch, gear and area
    _field("Siste fangstdato", "last_catch_date", DATE),
    _field("Kvotetype (kode)", "quota_type_code", TEXT),
    _field("Kvotetype", "quota_type_name", TEXT),
    _field("Redskap (kode)", "gear_code", TEXT),
    _field("Redskap", "gear_name", TEXT),
    _field("Redskap - gruppe (kode)", "gear_group_code", TEXT),
    _field("Redskap - gruppe", "gear_group_name", TEXT),
    _field("Fangstfelt (kode)", "catch_field_code", TEXT),
    _field("Hovedområde (kode)", "main_area_code", TEXT),
    _field("Hovedområde", "main_area_name", TEXT),
    _field("Lokasjon (kode)", "location_code", TEXT),
    _field("Sone (kode)", "zone_code", TEXT),
    _field("Sone", "zone_name", TEXT),
    # Species
    _field("Art - FDIR (kode)", "species_fdir_code", TEXT),
    _field("Art - FDIR", "species_fdir_name", TEXT),
    _field("Art (kode)", "species_code", TEXT),
    _field("Art", "species_name", TEXT),
    _field("Art - gruppe (kode)", "species_group_code", TEXT),
    _field("Art - gruppe", "species_group_name", TEXT),
    _field("Art - hovedgruppe (kode)", "species_main_group_code", TEXT),
    _field("Art - hovedgruppe", "species_main_group_name", TEXT),
    _field("Art FAO (kode)", "species_fao_code", TEXT, nullable=True),
    _field("Art FAO", "species_fao_name", TEXT, nullable=True),
    # Product classifications
    _field("Produkttilstand (kode)", "product_state_code", TEXT),
    _field("Produkttilstand", "product_state_name", TEXT),
    _field("Konserveringsmåte (kode)", "preservation_method_code", TEXT),
    _field("Konserveringsmåte", "preservation_method_name", TEXT),
    _field("Landingsmåte (kode)", "landing_method_code", TEXT),
    _field("Landingsmåte", "landing_method_name", TEXT),
    _field("Kvalitet (kode)", "quality_code", TEXT),
    _field("Kvalitet", "quality_name", TEXT),
    _field("Anvendelse (kode)", "use_code", TEXT, nullable=True),
    _field("Anvendelse", "use_name", TEXT, nullable=True),
    # Quantities and value. Weight unit is kilograms in the source documentation.
    _field("Antall stykk", "piece_count", LONG, nullable=True, role="measure"),
    _field("Bruttovekt", "gross_weight_kg", DECIMAL, role="measure"),
    _field("Produktvekt", "product_weight_kg", DECIMAL, role="measure"),
    _field("Rundvekt", "round_weight_kg", DECIMAL, nullable=True, role="measure"),
    _field("Fangstverdi", "catch_value_nok", DECIMAL, nullable=True, role="measure"),
    # Bronze provenance retained through Silver
    _field("_source_name", "source_name", TEXT, role="provenance"),
    _field("_source_file", "source_file", TEXT, role="provenance"),
    _field("_source_period", "source_period", TEXT, role="provenance"),
    _field("_source_sha256", "source_sha256", TEXT, role="provenance"),
    _field("_ingested_at", "ingested_at", TIMESTAMP, role="provenance"),
    _field("_run_id", "ingestion_run_id", TEXT, role="provenance"),
)

KEY_FIELDS: tuple[str, ...] = (
    "catch_year",
    "sales_org_code",
    "document_type_code",
    "document_number",
    "line_number",
)

# Resolve a future duplicate key by source business version, never by arrival order.
DUPLICATE_ORDER: tuple[str, ...] = (
    "document_version_number",
    "document_version_at_utc",
    "source_updated_date",
)
DUPLICATE_POLICY = (
    "Remove exact duplicates; for a repeated key keep the greatest business-version "
    "tuple; quarantine tied rows that disagree outside the key."
)

REFERENCE_JOINS: tuple[ReferenceJoin, ...] = (
    ReferenceJoin("species_fdir", "species_fdir_code"),
    ReferenceJoin("species_fao", "species_fao_code"),
    ReferenceJoin("landing_municipality", "landing_municipality_code"),
    ReferenceJoin("landing_country", "landing_country_code"),
    ReferenceJoin("gear", "gear_code"),
)

# Revised annual source snapshots rebuild only that catch-year partition after checks.
HISTORY_POLICY = (
    "Use one approved completed Bronze snapshot per catch year. Preserve its SHA-256 "
    "in every Silver row; replace the year partition atomically when a revision is approved."
)


def silver_schema() -> StructType:
    """Return the ordered target schema for Silver output."""
    return StructType(
        [StructField(field.name, field.data_type, field.nullable) for field in SILVER_FIELDS]
    )


def source_to_target() -> dict[str, str]:
    """Return the direct field-name mapping consumed by A6 cleaning."""
    return {field.source: field.name for field in SILVER_FIELDS}


def _validate_model() -> None:
    names = [field.name for field in SILVER_FIELDS]
    sources = [field.source for field in SILVER_FIELDS]
    if len(names) != len(set(names)):
        raise ValueError("Silver target field names must be unique")
    if len(sources) != len(set(sources)):
        raise ValueError("Each Bronze field may map to at most one Silver field")

    by_name = {field.name: field for field in SILVER_FIELDS}
    if any(name not in by_name or by_name[name].nullable for name in KEY_FIELDS):
        raise ValueError("Every key field must exist and be non-nullable")
    if any(name not in by_name for name in DUPLICATE_ORDER):
        raise ValueError("Duplicate ordering fields must exist in the Silver model")
    if any(join.silver_key not in by_name for join in REFERENCE_JOINS):
        raise ValueError("Reference join keys must exist in the Silver model")


_validate_model()
