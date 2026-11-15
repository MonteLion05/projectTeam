"""Match live events to IMDb; persist evidence and watermarked aggregates."""
import argparse,json,time
from pathlib import Path
from pyspark.sql import functions as F
from src.core import session,clean,RESULTS
p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=1800);a=p.parse_args()
s=session('wiki-imdb-stream',kafka=True)
queries=[];directory=Path(RESULTS)/'streaming';directory.mkdir(parents=True,exist_ok=True)
try:
    # Broad title candidates: matching is evidence of relevance, not verified entity identity.
    candidates=clean(s,'title.basics').selectExpr('tconst AS imdb_id','primaryTitle AS page_title').unionByName(
        clean(s,'name.basics').selectExpr('nconst AS imdb_id','primaryName AS page_title'))
    candidates=candidates.withColumn('match_key',F.lower(F.trim('page_title'))).select('imdb_id','match_key').distinct()
    events=s.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:19092').option('subscribe','wikistreams').option('startingOffsets','latest').load()
    schema='title string, server_name string, type string, namespace int, timestamp long, meta struct<id:string,dt:string>'
    parsed=events.select(F.from_json(F.col('value').cast('string'),schema).alias('e')).select('e.*')
    parsed=parsed.filter((F.col('namespace')==0)&F.col('type').isin('edit','new')).withColumn('event_time',F.to_timestamp(F.from_unixtime('timestamp')))
    parsed=parsed.filter(F.col('event_time').isNotNull()).withColumn('event_id',F.col('meta.id')).filter(F.col('event_id').isNotNull())
    parsed=parsed.withColumn('match_key',F.lower(F.trim(F.regexp_replace('title',r' \([^)]*\)$',''))))
    matched=parsed.join(candidates,'match_key','inner').withColumn('match_basis',F.lit('normalized_title_candidate'))
    # This deduplicates per IMDb candidate and bounds state using event time.
    bounded=matched.withWatermark('event_time','10 minutes').dropDuplicatesWithinWatermark(['event_id','imdb_id'])
    queries.append(bounded.writeStream.format('parquet').option('path',str(directory/'matches')).option(
        'checkpointLocation',str(directory/'checkpoints/matches')).trigger(processingTime='20 seconds').start())
    aggregate=matched.withWatermark('event_time','10 minutes').groupBy(F.window('event_time','1 minute'),'server_name').count()
    # Update-mode batches preserve evolving counts, not only finalized windows.
    def save_batch(df,batch):
        df.write.mode('overwrite').json(str(directory/'window_updates'/f'batch_{batch}'))
    queries.append(aggregate.writeStream.outputMode('update').foreachBatch(save_batch).option(
        'checkpointLocation',str(directory/'checkpoints/windows')).trigger(processingTime='20 seconds').start())
    deadline=time.monotonic()+a.seconds;seen=set()
    with (directory/'progress.jsonl').open('a') as f:
        while time.monotonic()<deadline:
            for q in queries:
                if q.exception():raise RuntimeError(str(q.exception()))
                for progress in q.recentProgress:
                    key=(str(q.id),progress['batchId'])
                    if key not in seen:f.write(json.dumps(progress)+'\n');f.flush();seen.add(key)
            time.sleep(2)
finally:
    for q in queries:q.stop()
    s.stop()
