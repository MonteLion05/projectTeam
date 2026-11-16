import os,sys,json,time
from pathlib import Path
root=Path(__file__).resolve().parent
sys.path.insert(0,str(root.parent/'current/projectTeam'))
from pyspark.sql import SparkSession,functions as F
from pyspark import StorageLevel
from jobs.analyze import QUERIES
s=(SparkSession.builder.master('local[1]').appName('verify-result-labels').config('spark.sql.shuffle.partitions','32').config('spark.sql.autoBroadcastJoinThreshold','-1').config('spark.sql.adaptive.autoBroadcastJoinThreshold','-1').config('spark.ui.enabled','false').getOrCreate())
s.sparkContext.setLogLevel('ERROR')
try:
 counts={}
 for p in sorted((root/'parquet/clean').iterdir()):
  if not p.is_dir():continue
  df=s.read.parquet(str(p));df.createOrReplaceTempView(p.name.replace('.','_'));counts[p.name]=df.count()
 (root/'results/clean_row_counts.json').write_text(json.dumps(counts,indent=2))
 s.sql("CREATE OR REPLACE TEMP VIEW films AS SELECT b.*,r.averageRating,r.numVotes FROM title_basics b LEFT JOIN title_ratings r USING(tconst) WHERE titleType='movie' AND runtimeMinutes>=60")
 old=s.read.parquet(str(root/'results/q8/parquet'))
 old.select('nconst').distinct().createOrReplaceTempView('selected_people')
 selected=F.broadcast(old.select('nconst').distinct())
 film_ids=F.broadcast(s.table('films').select('tconst'))
 actors=(s.table('title_principals').filter(F.col('category').isin('actor','actress'))
     .select('tconst','nconst').join(film_ids,'tconst','left_semi').join(selected,'nconst','left_semi')
     .select('nconst').distinct().withColumn('role',F.lit('actor')))
 branches=[actors]
 for column,label in [('directors','director'),('writers','writer')]:
  branch=(s.table('title_crew').join(film_ids,'tconst','left_semi')
      .select(F.explode(column).alias('nconst')).join(selected,'nconst','left_semi')
      .distinct().withColumn('role',F.lit(label)))
  branches.append(branch)
 roles=branches[0].unionByName(branches[1]).unionByName(branches[2]).groupBy('nconst').agg(
     F.sort_array(F.collect_set('role')).alias('career_role_names'))

 enriched=old.join(roles,'nconst').persist(StorageLevel.DISK_ONLY)
 assert enriched.count()==old.count()
 assert enriched.filter(F.size('career_role_names')!=F.col('career_roles')).count()==0
 dest=root/'results/q8_enriched'
 enriched.write.mode('overwrite').parquet(str(dest/'parquet'))
 enriched.withColumn('career_role_names',F.to_json('career_role_names')).coalesce(1).write.mode('overwrite').option('header',True).csv(str(dest/'csv'))
 enriched.unpersist()
 q4=s.read.parquet(str(root/'results/q4/parquet'))
 q4=q4.join(s.table('title_basics').selectExpr('tconst AS excellent_film','primaryTitle AS excellent_film_title'),'excellent_film')
 q4.coalesce(1).write.mode('overwrite').option('header',True).csv(str(root/'results/q4_with_titles'))
 print('VALIDATED Q8 role labels and row preservation; saved Q4 film titles and clean row counts.',flush=True)
finally:s.stop()
(root/'results/q8').rename(root/'results/q8_original')
(root/'results/q8_enriched').rename(root/'results/q8')
status=json.loads((root/'results/status.json').read_text());status['q8']['sql']=QUERIES['q8'].format(min_votes=1000);status['q8']['supplement']='Role sets calculated from the same cleaned snapshot and joined to initial Q8; row count and role-count consistency verified.'
(root/'results/status.json').write_text(json.dumps(status,indent=2))
