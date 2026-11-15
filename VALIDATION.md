# Validation of the continuation

Validated 2026-11-15 using Python 3.12, Java and local PySpark 4.0.1.

Executed successfully:
- Two unittest methods covering typed reads, null repairs, invalid list-reference removal and all nine SQL queries (Q1–Q8 and open).
- Every query produced a non-empty result on the designed synthetic fixture. These are NOT IMDb results.
- Gzip validation accepts a valid fixture and rejects corruption; fingerprints change when file bytes change.
- Original clean_values script executes for three tables, writes Parquet, reads every column back and checks metrics.
- A second clean run skips all three validated tables; changing one source causes only that table to be recomputed.
- Spark enrichment selection, cached-text joins, explicit missing statuses and coverage outputs execute on the synthetic fixture.
- All packaged Python files compile. Compose YAML parses with the original service names.
- REPORT.md is byte-for-byte identical to the supplied archive.
- Packaged ZIP CRC verification passed.

Not validated here:
- Full IMDb data or the physical USB filesystem (not supplied).
- Docker/PowerShell execution on the user's Windows machine.
- The user's Spark 4.2.0 image and the availability of its matching Kafka connector.
- Live Wikipedia/Wikidata requests, a real Kafka stream or a 15–30 minute observation window.

No full-data result is claimed or fabricated. Remaining analytical limitations (language proxy and title-based candidate matches) are explicit in RUN_GUIDE.md.

Reproduce local tests with Java and PySpark 4.x:

```bash
SPARK_LOCAL_IP=127.0.0.1 python tests/test_pipeline.py
SPARK_LOCAL_IP=127.0.0.1 python tests/check_resume.py
SPARK_LOCAL_IP=127.0.0.1 python tests/check_enrichment.py
```

The synthetic fixtures are temporary and removed by the tests.
