"""Run the project's SQL on validated downloads with reusable disk materialization."""
import json,os,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'current/projectTeam'))
os.environ['DATA_DIR']=str(ROOT)
os.environ['RESULTS_DIR']=str(ROOT/'results')
os.environ['INPUT_FORMAT']='parquet'
from pyspark.sql import SparkSession,functions as F
from pyspark import StorageLevel
from src.schemas import TABLE_COLUMNS
from src.loaders import read_tsv
from src import core
from jobs.analyze import QUERIES
spark=(SparkSession.builder.master('local[2]').appName('imdb-verified-eight-questions')
 .config('spark.sql.shuffle.partitions','32').config('spark.ui.enabled','false')
 .config('spark.sql.autoBroadcastJoinThreshold','-1').config('spark.sql.adaptive.autoBroadcastJoinThreshold','-1')
 .config('spark.local.dir',str(ROOT/'spark-tmp')).getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
status={}
def log(x):print(time.strftime('%H:%M:%S'),x,flush=True)
try:
 for table in TABLE_COLUMNS:
  dest=ROOT/'parquet/raw'/table
  if not (dest/'_SUCCESS').exists():
   log('READ TSV AND WRITE PARQUET '+table)
   read_tsv(spark,table).write.mode('overwrite').parquet(str(dest))
 for table in TABLE_COLUMNS:
  dest=ROOT/'parquet/base'/table
  if not (dest/'_SUCCESS').exists():
   log('CLEAN VALUES AND UNIQUE KEYS '+table)
   core.base_clean(spark,table).write.mode('overwrite').parquet(str(dest))
 core.base_clean=lambda s,t:s.read.parquet(str(ROOT/'parquet/base'/t))
 for table in TABLE_COLUMNS:
  dest=ROOT/'parquet/clean'/table
  if not (dest/'_SUCCESS').exists():
   log('CLEAN REFERENCES '+table)
   core.clean(spark,table).write.mode('overwrite').parquet(str(dest))
  spark.read.parquet(str(dest)).createOrReplaceTempView(table.replace('.','_'))
 spark.sql("CREATE OR REPLACE TEMP VIEW films AS SELECT b.*,r.averageRating,r.numVotes FROM title_basics b LEFT JOIN title_ratings r USING(tconst) WHERE titleType='movie' AND runtimeMinutes>=60")
 spark.sql('CREATE OR REPLACE TEMP VIEW fg AS SELECT *,explode(genres) AS genre FROM films')
 for name,query in QUERIES.items():
  log('RUN '+name)
  start=time.time();df=spark.sql(query.format(min_votes=1000)).persist(StorageLevel.DISK_ONLY)
  n=df.count();dest=ROOT/'results'/name
  df.write.mode('overwrite').parquet(str(dest/'parquet'))
  for field in df.schema.fields:
   if field.dataType.typeName() in ('array','struct','map'):df=df.withColumn(field.name,F.to_json(field.name))
  df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(dest/'csv'))
  status[name]={'rows':n,'elapsed_seconds':round(time.time()-start,2),'sql':query.format(min_votes=1000)}
  (ROOT/'results/status.json').write_text(json.dumps(status,indent=2))
  log('SAVED '+name+' rows='+str(n));spark.catalog.clearCache()
 coverage=spark.sql("SELECT count(*) AS films,sum(CASE WHEN l.tconst IS NOT NULL THEN 1 ELSE 0 END) AS films_with_language_proxy FROM films f LEFT JOIN (SELECT DISTINCT titleId AS tconst FROM title_akas WHERE isOriginalTitle=true AND language IS NOT NULL AND language NOT IN ('und','zxx','mul')) l USING(tconst)").first().asDict()
 (ROOT/'results/q6_language_coverage.json').write_text(json.dumps(coverage,indent=2))
 log('ALL COMPLETE')
finally:spark.stop()
