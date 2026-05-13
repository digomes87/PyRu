# Storage Layout

## Partition scheme

Feature rows are stored as Parquet files partitioned by symbol, date, and hour:

```
{base_path}/
  symbol=BTCUSDT/
    date=20240115/
      hour=14/
        part-0000.parquet
        part-0001.parquet
```

### Partition column derivation

| Partition column | Type   | Derived from              | Example       |
|------------------|--------|---------------------------|---------------|
| `symbol`         | string | `Trade.symbol`            | `BTCUSDT`     |
| `date`           | int32  | `ts // 86_400_000_000_000` (ns→day epoch) | `19737` |
| `hour`           | int8   | `(ts % 86_400_000_000_000) // 3_600_000_000_000` | `14` |

The `date` and `hour` values are stored as integers (not strings) so that range predicates can use native integer comparison rather than string lexicographic ordering.

## File format

- Compression: **Snappy** (balanced speed vs size; LZ4 for write-heavy, Zstd for archive)
- Row group size: 128 MiB (default)
- Page size: 1 MiB
- Dictionary encoding: enabled for `symbol` and `side` columns

## Cross-language compatibility

Both the Python (pyarrow) and Rust (arrow-rs/parquet) writers must produce files that are readable by both. The conformance test reads a file written by one implementation with the other and asserts identical record counts and column values within float tolerance.

## Query patterns the layout is optimized for

1. **Symbol + time range** — `WHERE symbol = 'X' AND date BETWEEN d1 AND d2` — hits at most `(d2-d1+1) * 24` directories
2. **Single hour point lookup** — `WHERE symbol = 'X' AND date = d AND hour = h` — hits exactly one directory
3. **Full symbol scan** — reads all hours, leverages column projection pushdown to skip unused columns

## Delta Lake

The optional Delta Lake layer wraps the Parquet files with a transaction log (`_delta_log/`) enabling:
- ACID writes (concurrent writers safe)
- Time travel (read feature values as of a past transaction)
- Schema evolution
- Vacuum (compaction of small files)

Use Delta when running the serving layer in production. Use raw Parquet for archival exports and ad-hoc analysis.
