"""Shared Spark loading, conservative repairs and output utilities."""
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from src.schemas import TABLE_COLUMNS

ROOT = os.environ.get('DATA_DIR', '/home/jovyan/data')
RESULTS = os.environ.get('RESULTS_DIR', '/home/jovyan/results')
KEYS = {t: ['tconst'] for t in TABLE_COLUMNS}
KEYS.update({'name.basics':['nconst'], 'title.akas':['titleId','ordering'],
             'title.principals':['tconst','ordering']})
LISTS = {'name.basics':['primaryProfession','knownForTitles'],
         'title.basics':['genres'], 'title.crew':['directors','writers']}
MIN_VOTES = int(os.environ.get('MIN_VOTES', '1000'))

def session(name, kafka=False):
    from src.spark_session import get_spark
    return get_spark(name, kafka=kafka)

def raw(s, table):
    from src.loaders import read_raw
    return read_raw(s, table)

def present(c):
    return c.isNotNull() & (F.trim(c)!='') & (F.trim(c)!='\\N')

def typed(s, table):
    df = raw(s,table)
    for c,t in TABLE_COLUMNS[table]:
        df = df.withColumn(c,F.when(present(F.col(c)),F.col(c)).try_cast(t))
    return df

def rules(table):
    from scripts.clean_values import value_rules
    return value_rules(table)

def base_clean(s, table):
    df=typed(s,table)
    flags=F.array(*[F.when(cond,F.lit(label)) for _,cond,label,_ in rules(table)]).cast('array<string>')
    df=df.withColumn('dq_flags',F.filter(flags,lambda x:x.isNotNull()))
    for c,_,label,repair in rules(table):
        if repair: df=df.withColumn(c,F.when(~F.array_contains('dq_flags',label),F.col(c)))
    for c in LISTS.get(table,[]):
        df=df.withColumn(c,F.transform(F.split(c,','),lambda x:F.trim(x)))
    for c in KEYS[table]: df=df.filter(F.col(c).isNotNull())
    # Reject all ambiguous duplicate keys, rather than select an arbitrary record.
    unique=df.groupBy(*KEYS[table]).count().filter('count = 1').drop('count')
    return df.join(unique,KEYS[table],'left_semi')

RELATIONS=[('title.akas','titleId','title.basics','tconst'),
 ('title.crew','tconst','title.basics','tconst'),
 ('title.episode','tconst','title.basics','tconst'),
 ('title.episode','parentTconst','title.basics','tconst'),
 ('title.ratings','tconst','title.basics','tconst'),
 ('title.principals','tconst','title.basics','tconst'),
 ('title.principals','nconst','name.basics','nconst')]
ARRAY_REFS=[('title.crew','directors','name.basics','nconst'),
 ('title.crew','writers','name.basics','nconst'),
 ('name.basics','knownForTitles','title.basics','tconst')]

def clean(s,table):
    """Lazy clean view; no second full copy is required on disk."""
    df=base_clean(s,table)
    for child,c,parent,key in RELATIONS:
        if child==table:
            p=base_clean(s,parent).select(F.col(key).alias(c))
            df=df.join(p,c,'left_semi')
    # Keep valid members of list references; raw values remain in the raw layer.
    for child,c,parent,key in ARRAY_REFS:
        if child!=table: continue
        rowkey=KEYS[table][0]
        refs=df.select(rowkey,F.posexplode(c).alias('position','reference'))
        valid=refs.join(base_clean(s,parent).select(F.col(key).alias('reference')),
                        'reference','left_semi')
        grouped=valid.groupBy(rowkey).agg(F.sort_array(F.collect_list(
            F.struct('position','reference'))).alias('_refs'))
        grouped=grouped.withColumn('_valid',F.transform('_refs',lambda x:x.reference)).drop('_refs')
        df=df.join(grouped,rowkey,'left').withColumn(c,
            F.when(F.col(c).isNull(),F.lit(None).cast('array<string>'))
            .otherwise(F.coalesce('_valid',F.array().cast('array<string>')))).drop('_valid')
    return df

def output(df,name):
    """Parquet preserves arrays; JSON remains readable on GitHub for small outputs."""
    df.write.mode('overwrite').parquet(f'{RESULTS}/{name}/parquet')
    df.write.mode('overwrite').json(f'{RESULTS}/{name}/json')
    print('SAVED',name,flush=True)

def views(s):
    for table in TABLE_COLUMNS:
        clean(s,table).createOrReplaceTempView(table.replace('.','_'))
    s.sql('''CREATE OR REPLACE TEMP VIEW films AS
        SELECT b.*, r.averageRating, r.numVotes FROM title_basics b
        LEFT JOIN title_ratings r USING(tconst)
        WHERE titleType='movie' AND runtimeMinutes>=60''')
    s.sql('''CREATE OR REPLACE TEMP VIEW fg AS
        SELECT *, explode(genres) AS genre FROM films''')
