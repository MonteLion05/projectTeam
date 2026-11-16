from pathlib import Path
from pyspark.sql import SparkSession,functions as F
root=Path(__file__).resolve().parent
s=SparkSession.builder.master('local[1]').appName('export-standard-csv').config('spark.ui.enabled','false').getOrCreate()
s.sparkContext.setLogLevel('ERROR')
try:
 for name in ['q1','q2','q3','q4','q5','q6','q7','q8','open']:
  df=s.read.parquet(str(root/'results'/name/'parquet'))
  for field in df.schema.fields:
   if field.dataType.typeName() in ('array','struct','map'):df=df.withColumn(field.name,F.to_json(field.name))
  df.coalesce(1).write.mode('overwrite').option('header',True).option('escape','"').option('quoteAll',True).csv(str(root/'results'/name/'csv'))
  print('EXPORTED',name,flush=True)
 df=s.read.parquet(str(root/'results/q4/parquet')).join(s.read.parquet(str(root/'parquet/clean/title.basics')).selectExpr('tconst AS excellent_film','primaryTitle AS excellent_film_title'),'excellent_film')
 df.coalesce(1).write.mode('overwrite').option('header',True).option('escape','"').option('quoteAll',True).csv(str(root/'results/q4_with_titles'))
finally:s.stop()
