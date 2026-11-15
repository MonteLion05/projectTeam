"""Spark schema enforcement, text cleaning, joins and coverage metrics."""
from pathlib import Path
from pyspark.sql import functions as F
from src.core import session,RESULTS,output
s=session('finish-enrichment')
try:
    schema='imdb_id string, kind string, retrieved_at string, text string, source_url string, wikipedia_title string, status string, error string'
    files=list((Path(RESULTS)/'enrichment/cache').glob('*.json'))
    data=s.read.schema(schema).json([str(x) for x in files]) if files else s.createDataFrame([],schema)
    data=data.withColumn('text',F.trim(F.regexp_replace(F.regexp_replace('text',r'\[\d+\]',''),r'\s+',' ')))
    data=data.withColumn('text',F.when(F.length('text')>0,F.col('text')))
    for label,key in [('films','tconst'),('people','nconst')]:
        selection=s.read.parquet(f'{RESULTS}/enrichment/selection_{label}/parquet')
        joined=selection.join(data,selection[key]==data.imdb_id,'left').drop('imdb_id')
        joined=joined.withColumn('status',F.coalesce('status',F.lit('not_collected')))
        output(joined,f'enrichment/{label}')
        output(joined.groupBy('status').count(),f'enrichment/{label}_status')
        output(joined.agg(F.count('*').alias('selected_rows'),F.count('text').alias('rows_with_text'),
            (F.count('text')/F.count('*')).alias('text_coverage')),f'enrichment/{label}_coverage')
    pages=data.filter(F.col('source_url').isNotNull()).select('imdb_id','wikipedia_title','source_url').distinct()
    output(pages,'enrichment/page_map')
finally:s.stop()
