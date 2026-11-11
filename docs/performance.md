# Silver performance evidence (A8)

Status: **local WSL comparison and VM repetition complete** (see "VM repetition" below).
Spark UI screenshots were not captured.

## Method

All measurements used the same 2023 Bronze snapshot, 1,079,929 input rows, PySpark
3.5.3, Java 17, `local[*]` and eight shuffle partitions. The isolated benchmark wrote
to ignored `data/performance/<label>` paths and enabled an uncompressed Spark event log.
Quality rules and external references are not yet available, so these runs measured the
same clean → derived-fields → reconciled Parquet path in explicit unvalidated benchmark
mode. All runs produced exactly 1,079,929 output rows.

Metrics are parsed from `SparkListenerTaskEnd` records by
`scripts/summarize_spark_eventlog.py`. The compact event summaries are committed; raw
event logs and Parquet output remain ignored local artifacts.

## Results

| Measurement | Baseline | First attempt (rejected) | Final local |
|---|---:|---:|---:|
| Wall duration | 299.392 s | 441.147 s | **197.808 s** |
| Spark application duration | 271.816 s | 422.708 s | **182.358 s** |
| Shuffle read / write | 451.6 / 451.6 MB | 12.3 / 12.3 MB | **299.1 / 299.1 MB** |
| Memory spill | 2.642 GB | 0 | **0** |
| Disk spill | 648.5 MB | 0 | **0** |
| Worst task max / median | 161.9× | 360.6× | 209.7× during one-time repartition |
| Largest relevant task | 160.795 s | 198.828 s | **103.991 s** |
| Rows in / out | 1,079,929 / 1,079,929 | same | same |

The final local version is **33.9% faster** by wall time than the baseline. It reduces
shuffle volume by **33.8%** and removes all measured memory and disk spill.

## Evidence-based change

The baseline always applied `dropDuplicates` across every non-provenance field. A4 had
already shown that the five-field business key was unique. The event log showed that the
wide aggregation caused 451.6 MB shuffle, 3.29 GB combined spill and one 160.8-second
task.

The first attempt checked duplicate keys using only the five narrow key fields and ran
the wide exact deduplication only when candidates existed. It removed spill, but keeping
the source's uneven 12 partitions made counting, enrichment and writing wait repeatedly
for the same large partition. At 441.1 seconds, this change was rejected as slower.

The retained implementation keeps the narrow candidate check and the full exact-dedup
fallback. When the key is unique, it performs one hash repartition on the business key
using the configured shuffle partition count. Later actions then consume eight balanced
partitions. No early filter was introduced because Silver must retain every accepted
record, and no broadcast join was introduced because reference data are not configured.

## Reproduce on the VM

Run both strategies on the same quiet VM and source snapshot:

```bash
PYTHONPATH=src python scripts/benchmark_silver.py \
  --label baseline-vm --dedup-strategy baseline

PYTHONPATH=src python scripts/benchmark_silver.py \
  --label optimized-vm --dedup-strategy optimized
```

Then summarise each event-log directory with `scripts/summarize_spark_eventlog.py` and
capture the corresponding Jobs and Stages pages from Spark History Server. VM numbers
and screenshots, not the local WSL timings, are the final report evidence.

## VM repetition (11 November 2026)

Both strategies were rerun on the project VM (4 cores, 7.8 GB RAM, PySpark 3.5.0,
Java 8, Python 3.11, `local[*]`, 8 shuffle partitions, default 1 GB driver memory) on the
same 2023 Bronze snapshot. Unlike the local runs, these runs include the Silver quality
rules and the quality report. One run per strategy.

| Measure | Baseline | Optimized | Change |
|---|---:|---:|---:|
| Wall duration | 123.606 s | 99.832 s | −19.2% |
| Spark application duration | 120.919 s | 97.676 s | −19.2% |
| Executor CPU time | 200.6 s | 165.8 s | −17.4% |
| Shuffle read / write | 1,545.7 / 1,545.7 MB | 301.8 / 301.8 MB | −80.5% |
| Memory spill | 6,333.4 MB | 2,701.1 MB | −57.4% |
| Disk spill | 1,617.8 MB | 632.6 MB | −60.9% |
| Rows in / out | 1,079,929 / 1,079,929 | same | |

The two largest baseline shuffle stages (474.5 MB each) come from the full-row
deduplication and are absent in the optimized run. The gain is smaller than locally
because the quality rules and report cost the same in both runs. Both runs still spill,
mainly because of the default 1 GB driver memory; one optimized stage is skewed (largest
task 38.6 s vs. median 0.17 s). Evidence: `evidence/a8-baseline-vm.json`,
`evidence/a8-optimized-vm.json`.

## Evidence files

- [`a8-local-baseline.json`](evidence/a8-local-baseline.json)
- [`a8-local-optimized-first.json`](evidence/a8-local-optimized-first.json)
- [`a8-local-optimized.json`](evidence/a8-local-optimized.json)
- [`a8-local-comparison.json`](evidence/a8-local-comparison.json)
