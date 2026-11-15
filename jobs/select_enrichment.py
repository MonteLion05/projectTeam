"""Select top films and at least three available principals per film."""
from src.core import session,views,output,RESULTS
s=session('select-enrichment')
try:
    views(s)
    films=s.sql('''WITH ranked AS (SELECT *,row_number() OVER(PARTITION BY genre,startYear
    ORDER BY coalesce(numVotes,0) DESC,averageRating DESC,tconst) AS rank
    FROM fg WHERE startYear BETWEEN 1980 AND 2025) SELECT tconst,primaryTitle,startYear,genre,rank,numVotes
    FROM ranked WHERE rank<=10''')
    output(films,'enrichment/selection_films'); films.createOrReplaceTempView('selection')
    people=s.sql('''WITH ranked AS (SELECT p.*,row_number() OVER(PARTITION BY p.tconst ORDER BY ordering,nconst) AS person_rank
    FROM title_principals p JOIN (SELECT DISTINCT tconst FROM selection) f USING(tconst))
    SELECT p.tconst,p.nconst,n.primaryName,p.category,p.person_rank FROM ranked p JOIN name_basics n USING(nconst)
    WHERE person_rank<=3''')
    output(people,'enrichment/selection_people')
    targets=films.selectExpr('tconst AS imdb_id',"'film' AS kind").distinct().unionByName(
        people.selectExpr('nconst AS imdb_id',"'person' AS kind").distinct())
    targets.write.mode('overwrite').json(f'{RESULTS}/enrichment/targets')
    output(films.select('tconst').distinct().join(people.groupBy('tconst').count(),'tconst','left').fillna(0,['count']),
        'enrichment/principal_coverage')
finally:s.stop()
