"""Small synthetic integration checks, never IMDb result evidence."""
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.data = Path(cls.temp.name)
        os.environ['DATA_DIR'] = str(cls.data)
        os.environ['RESULTS_DIR'] = str(cls.data/'results')
        os.environ['INPUT_FORMAT'] = 'tsv'
        os.environ['SHUFFLE_PARTITIONS'] = '2'
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master('local[1]').appName('synthetic-tests').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
        from src.schemas import TABLE_COLUMNS
        rows = {table: [] for table in TABLE_COLUMNS}
        rows['name.basics'] = [ ['nm1','Actor A','1980',r'\N','actor','tt0,tt_missing'], ['nm2','Director B','1980','1970','director','tt0'], ['nm3','Director C','1980',r'\N','director','tt1'] ]
        for i in range(18):
            ident = f'tt{i}'
            rows['title.basics'].append([ident,'movie',f'Movie {i}',f'Movie {i}','0',str(1980+i*2),r'\N',('300' if i==17 else '90'),'Drama,Comedy'])
            rating = '8.5' if i < 3 else '5.0'
            rows['title.ratings'].append([ident,rating,'20000'])
            rows['title.crew'].append([ident,('nm3' if i in (1,2) else 'nm2'),'nm2'])
            rows['title.principals'].append([ident,'1','nm1','actor',r'\N','[]'])
            rows['title.akas'].append([ident,'1',f'Movie {i}','US',['en','fr','ar'][i%3],r'\N',r'\N','1'])
        rows['title.basics'].append(['series','tvSeries','Series','Series','0','2000',r'\N','30','Drama'])
        rows['title.basics'].append(['bad','movie','Invalid','Invalid','0','2000',r'\N','0','Drama'])
        for season in range(1,4):
            for number in range(1,11):
                ident=f'ep{season}_{number}'
                rows['title.basics'].append([ident,'tvEpisode','Episode','Episode','0','2000',r'\N','30','Drama'])
                rows['title.episode'].append([ident,'series',str(season),str(number)])
                rows['title.ratings'].append([ident,str(10-season),'2000'])
        (cls.data/'raw').mkdir()
        for table, records in rows.items():
            with gzip.open(cls.data/'raw'/f'{table}.tsv.gz','wt',encoding='utf-8') as f:
                f.write('\t'.join(c for c,_ in TABLE_COLUMNS[table])+'\n')
                for row in records: f.write('\t'.join(row)+'\n')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_01_clean_and_all_sql(self):
        from src.core import clean
        from src.schemas import TABLE_COLUMNS
        from jobs.analyze import QUERIES
        for table in TABLE_COLUMNS:
            frame = clean(self.spark,table).cache()
            frame.count()
            frame.createOrReplaceTempView(table.replace('.','_'))
        self.assertIsNone(self.spark.sql("SELECT deathYear FROM name_basics WHERE nconst='nm2'").first()[0])
        self.assertIsNone(self.spark.sql("SELECT runtimeMinutes FROM title_basics WHERE tconst='bad'").first()[0])
        self.assertEqual(self.spark.sql("SELECT knownForTitles FROM name_basics WHERE nconst='nm1'").first()[0],['tt0'])
        self.spark.sql("CREATE OR REPLACE TEMP VIEW films AS SELECT b.*,r.averageRating,r.numVotes FROM title_basics b LEFT JOIN title_ratings r USING(tconst) WHERE titleType='movie' AND runtimeMinutes>=60")
        self.spark.sql('CREATE OR REPLACE TEMP VIEW fg AS SELECT *,explode(genres) AS genre FROM films')
        for name,query in QUERIES.items():
            with self.subTest(question=name):
                result=self.spark.sql(query.format(min_votes=1000)).collect()
                print('SYNTHETIC SQL PASS',name,len(result),flush=True)
                if name in ('q1','q2','q3','q4','q5','q6','q7','q8','open'): self.assertTrue(result)

    def test_02_archive_checks(self):
        from scripts.download_imdb import validate
        good=self.data/'raw/title.episode.tsv.gz'
        self.assertEqual(len(validate(good)),64)
        broken=self.data/'broken.gz';broken.write_bytes(b'bad gzip')
        with self.assertRaises(Exception): validate(broken)
        from src.integrity import fingerprint
        before=fingerprint(broken);broken.write_bytes(b'changed')
        self.assertNotEqual(before,fingerprint(broken))

if __name__ == '__main__': unittest.main(verbosity=2)
