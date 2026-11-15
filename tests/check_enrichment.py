"""Offline Spark selection/enrichment smoke test with explicit synthetic text."""
import json
import runpy
from test_pipeline import PipelineTests, ROOT
from pyspark.sql import SparkSession

PipelineTests.setUpClass()
try:
    runpy.run_path(str(ROOT/'jobs/select_enrichment.py'),run_name='__main__')
    cache=PipelineTests.data/'results/enrichment/cache';cache.mkdir(parents=True)
    (cache/'tt0.json').write_text(json.dumps({'imdb_id':'tt0','kind':'film','text':'Synthetic test only.',
        'status':'ok','source_url':'https://example.invalid/synthetic','wikipedia_title':'Fixture'}))
    runpy.run_path(str(ROOT/'jobs/finish_enrichment.py'),run_name='__main__')
    spark=SparkSession.builder.master('local[1]').appName('assert-enrichment').getOrCreate()
    films=spark.read.parquet(str(PipelineTests.data/'results/enrichment/films/parquet'))
    assert films.filter("tconst='tt0' AND status='ok'").count()>0
    assert films.filter("status='not_collected'").count()>0
    print('PASS: enrichment selection, cached-text join, missing statuses and coverage (synthetic only).')
    spark.stop()
finally:
    PipelineTests.temp.cleanup()
