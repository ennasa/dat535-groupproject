# Fisheries seasonality Gold mart (A9)

## Consumer and question

The consumer is an analyst exploring seasonal and regional landing patterns. The
question is:

> How do landed fish quantity and species composition vary by Norwegian region and
> month?

The demonstration notebook is
[`notebooks/03_gold_consumer.ipynb`](../notebooks/03_gold_consumer.ipynb).

## Grain and tables

The mart contains:

| Table | Grain |
|---|---|
| `dim_month` | One actual landing year-month |
| `dim_landing_region` | One observed country/county code and label combination |
| `dim_species` | One observed consolidated species classification combination |
| `fact_monthly_species_region` | One month × landing region × species |

Dimension identifiers are deterministic SHA-256 values where the natural code is not
enough to preserve changing labels. The fact retains only dimension identifiers and
aggregated measures.

## Measures

`total_round_weight_kg` is the primary quantity because the source defines round weight
as live-weight equivalent. Product and gross weight are not added to it.

The fact also provides:

- product weight and catch-value totals;
- product-line and distinct-document counts;
- missing and zero round-weight counts;
- total region/month round weight;
- each species' round-weight share within its region and month.

A null round weight does not become zero. A zero remains zero until the shared quality
policy establishes its meaning. Species share is null where the region/month total is
not positive.

## Geography and coverage

Foreign landings are retained and identified by country. Missing county uses the
explicit `UNKNOWN` / `Unknown / not supplied` member; it is not assigned to a region.
The main consumer view filters `landing_country_code = NOR` and actual landing year
2023. Off-year landings remain visible in coverage metrics.

## Candidate validation and publication

`src/gold/build.py` creates an immutable candidate from exactly one completed Silver
snapshot per configured period. It reconciles the sum of Gold `line_count` with Silver
row count and rereads every written Parquet table before completing the candidate.
Repeated builds skip an existing candidate.

Normal execution rejects Silver unless its model is approved and its quality mode is
`rules-engine`. The local A9/A10 preview explicitly overrides that gate, writes only to
ignored `data/preview`, and is labelled unvalidated. In the normal pipeline,
`src/gold/validate.py` checks the stored candidate against Silver (exact totals, unique
grain, no orphan rows, species totals and shares), and `src/gold/publish.py` moves the
pointer `_published.json` only if every check passes. The consumer notebook reads only the
published version.

## Local preview evidence

The full preview reconciled 1,079,929 Silver lines into 5,496 fact rows, 22 month
members, 22 region members and 158 species members. Compact results are stored in
[`evidence/a9-a10-preview.json`](evidence/a9-a10-preview.json), with the validation
record in [`evidence/a9-a10-validation.json`](evidence/a9-a10-validation.json).
