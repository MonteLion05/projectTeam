# Changes from the uploaded projectTeam-code.zip

This update extends the supplied project, preserving the original report and quality checks.

## Modified files

- PROJECT.md
- README.md
- compose.yaml
- scripts/clean_values.py
- scripts/convert_to_parquet.py
- scripts/download_imdb.py
- scripts/rebuild_episode.py
- scripts/test_usb.py
- scripts/verify_local.py
- src/loaders.py
- src/spark_session.py
- streaming/wikistream_producer.py

## Added files

- .env.example
- .gitignore
- CONTINUE_AR.md
- RUN_GUIDE.md
- jobs/analyze.py
- jobs/finish_enrichment.py
- jobs/select_enrichment.py
- scripts/collect_wikipedia.py
- scripts/run_pipeline.ps1
- src/core.py
- src/integrity.py
- streaming/wikistream_consumer.py
- tests/check_enrichment.py
- tests/check_resume.py
- tests/test_pipeline.py

## Byte-for-byte unchanged supplied files

- REPORT.md
- jobs/.gitkeep
- notebooks/.gitkeep
- requirements.txt
- scripts/__init__.py
- scripts/check_casts.py
- scripts/check_duplicates.py
- scripts/check_relations.py
- scripts/check_typed.py
- scripts/check_values.py
- scripts/profile_data.py
- src/__init__.py
- src/quality.py
- src/schemas.py
- streaming/admin.py

Generated validation notes and this change list are additional files. No full IMDb data are included.
