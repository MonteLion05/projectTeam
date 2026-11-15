"""Run one or all IMDb exploration questions, writing actual outputs only."""
import argparse
from src.core import session, views, output, MIN_VOTES

QUERIES = {
'q1': '''WITH stats AS (SELECT genre,count(*) AS genre_films,avg(runtimeMinutes) AS mean_minutes,
 stddev_pop(runtimeMinutes) AS sd FROM fg GROUP BY genre)
 SELECT f.tconst,f.primaryTitle,f.genre,f.runtimeMinutes,s.genre_films,s.mean_minutes,
 (f.runtimeMinutes-s.mean_minutes)/s.sd AS z_score
 FROM fg f JOIN stats s USING(genre) WHERE s.sd>0 AND abs(f.runtimeMinutes-s.mean_minutes)>2*s.sd''',
'q2': '''WITH scores AS (SELECT startYear AS year,genre,sum(coalesce(numVotes,0)) AS votes,count(*) AS film_count
 FROM fg WHERE startYear BETWEEN 1980 AND 2025 GROUP BY startYear,genre), ranks AS
 (SELECT *,row_number() OVER(PARTITION BY year ORDER BY votes DESC,genre) AS rank FROM scores)
 SELECT * FROM ranks WHERE rank<=3 ORDER BY year,rank''',
'q3': '''WITH scores AS (SELECT startYear AS year,genre,sum(coalesce(numVotes,0)) AS votes FROM fg
 WHERE startYear BETWEEN 1980 AND 2025 GROUP BY startYear,genre), ranks AS
 (SELECT *,row_number() OVER(PARTITION BY year ORDER BY votes DESC,genre) AS rank FROM scores),
 buckets AS (SELECT f.*,CASE WHEN r.rank<=3 THEN 'top3' ELSE 'other' END AS bucket
 FROM fg f JOIN ranks r ON f.startYear=r.year AND f.genre=r.genre), agg AS
 (SELECT startYear AS year,bucket,count(*) AS genre_memberships,count(DISTINCT tconst) AS distinct_films,
 sum(coalesce(numVotes,0)) AS votes,avg(averageRating) AS mean_rating,avg(runtimeMinutes) AS mean_minutes
 FROM buckets GROUP BY startYear,bucket)
 SELECT *,votes / nullif(sum(votes) OVER(PARTITION BY year),0) AS vote_share FROM agg ORDER BY year,bucket''',
'q4': '''WITH directors AS (SELECT tconst,explode(directors) AS nconst FROM title_crew),
 works AS (SELECT d.nconst,f.* FROM directors d JOIN films f USING(tconst) WHERE numVotes>={min_votes}),
 stats AS (SELECT nconst,sum(CASE WHEN averageRating>=8 THEN 1 ELSE 0 END) AS excellent,
 sum(CASE WHEN averageRating<8 THEN 1 ELSE 0 END) AS other_count,
 avg(CASE WHEN averageRating<8 THEN averageRating END) AS other_mean,
 max(CASE WHEN averageRating>=8 THEN tconst END) AS excellent_film FROM works GROUP BY nconst)
 SELECT s.*,n.primaryName FROM stats s JOIN name_basics n USING(nconst)
 WHERE excellent=1 AND other_count>=2 AND other_mean<=6''',
'q5': '''WITH eps AS (SELECT e.*,r.averageRating FROM title_episode e LEFT JOIN title_ratings r USING(tconst)
 JOIN title_basics b ON e.parentTconst=b.tconst WHERE b.titleType='tvSeries' AND seasonNumber>=1),
 seasons AS (SELECT parentTconst,seasonNumber,count(*) AS episodes FROM eps GROUP BY parentTconst,seasonNumber),
 eligible AS (SELECT parentTconst,count(*) AS seasons,max(seasonNumber) AS last_season FROM seasons
 GROUP BY parentTconst HAVING count(*)>=3 AND min(episodes)>=10),
 extrema AS (SELECT parentTconst,min(averageRating) AS worst,max(averageRating) AS best FROM eps GROUP BY parentTconst),
 hits AS (SELECT e.parentTconst,max(CASE WHEN e.averageRating=x.worst AND e.seasonNumber=g.last_season THEN 1 ELSE 0 END) AS worst_last,
 max(CASE WHEN e.averageRating=x.best AND e.seasonNumber<=2 THEN 1 ELSE 0 END) AS best_early
 FROM eps e JOIN extrema x USING(parentTconst) JOIN eligible g USING(parentTconst) GROUP BY e.parentTconst)
 SELECT g.*,b.primaryTitle,x.worst,x.best FROM eligible g JOIN hits h USING(parentTconst)
 JOIN extrema x USING(parentTconst) JOIN title_basics b ON g.parentTconst=b.tconst WHERE worst_last=1 AND best_early=1''',
'q6': '''WITH languages AS (SELECT DISTINCT titleId AS tconst,language FROM title_akas
 WHERE isOriginalTitle=true AND language IS NOT NULL AND language NOT IN ('und','zxx','mul')),
 success AS (SELECT * FROM films WHERE averageRating>=7 AND numVotes>=10000),
 actors AS (SELECT DISTINCT tconst,nconst FROM title_principals WHERE category IN ('actor','actress')),
 credits AS (SELECT DISTINCT a.nconst,s.tconst,l.language FROM actors a JOIN success s USING(tconst) JOIN languages l USING(tconst)),
 stats AS (SELECT nconst,count(DISTINCT language) AS language_count,sort_array(collect_set(language)) AS languages,
 count(DISTINCT tconst) AS successful_films FROM credits GROUP BY nconst)
 SELECT x.*,n.primaryName,language_count>=3 AS at_least_three_languages,
 'original-title language proxy; not verified spoken language' AS language_basis
 FROM stats x JOIN name_basics n USING(nconst) WHERE language_count>=2''',
'q7': '''WITH actors AS (SELECT DISTINCT tconst,nconst FROM title_principals WHERE category IN ('actor','actress')),
 works AS (SELECT a.nconst,f.* FROM actors a JOIN films f USING(tconst) WHERE startYear IS NOT NULL),
 careers AS (SELECT nconst,min(startYear) AS first_year,max(startYear) AS last_year,count(*) AS films
 FROM works GROUP BY nconst HAVING max(startYear)-min(startYear)>=20 AND count(*)>=15),
 scored AS (SELECT w.*,c.first_year,c.last_year,c.films,
 CASE WHEN startYear<=first_year+(last_year-first_year)/3.0 THEN 'early' ELSE 'later' END AS phase
 FROM works w JOIN careers c USING(nconst) WHERE numVotes>={min_votes} AND averageRating IS NOT NULL),
 stats AS (SELECT nconst,max(first_year) AS first_year,max(last_year) AS last_year,max(films) AS films,
 avg(CASE WHEN phase='early' THEN averageRating END) AS early_mean,
 avg(CASE WHEN phase='later' THEN averageRating END) AS later_mean,
 max(CASE WHEN phase='early' THEN averageRating END) AS early_max,
 max(CASE WHEN phase='later' THEN averageRating END) AS later_max FROM scored GROUP BY nconst)
 SELECT x.*,n.primaryName FROM stats x JOIN name_basics n USING(nconst)
 WHERE early_mean>later_mean AND early_max>later_max''',
'q8': '''WITH roles AS (
 SELECT tconst,nconst,'actor' AS role FROM title_principals WHERE category IN ('actor','actress')
 UNION SELECT tconst,explode(directors) AS nconst,'director' AS role FROM title_crew
 UNION SELECT tconst,explode(writers) AS nconst,'writer' AS role FROM title_crew),
 movie_roles AS (SELECT r.tconst,r.nconst,count(DISTINCT role) AS roles_on_film FROM roles r
 JOIN films f USING(tconst) GROUP BY r.tconst,r.nconst),
 versatile AS (SELECT r.nconst,count(DISTINCT role) AS career_roles FROM roles r JOIN films f USING(tconst)
 GROUP BY nconst HAVING count(DISTINCT role)>=2),
 stats AS (SELECT m.nconst,v.career_roles,CASE WHEN roles_on_film>=2 THEN 'multiple' ELSE 'single' END AS role_mode,
 count(*) AS films,avg(f.averageRating) AS mean_rating,avg(f.numVotes) AS mean_votes
 FROM movie_roles m JOIN versatile v USING(nconst) JOIN films f USING(tconst)
 WHERE f.numVotes>={min_votes} GROUP BY m.nconst,v.career_roles,CASE WHEN roles_on_film>=2 THEN 'multiple' ELSE 'single' END)
 SELECT x.*,n.primaryName FROM stats x JOIN name_basics n USING(nconst)''',
'open': '''WITH dirs AS (SELECT tconst,explode(directors) AS nconst FROM title_crew),
 pairs AS (SELECT DISTINCT a.tconst,a.nconst AS actor,d.nconst AS director FROM title_principals a
 JOIN dirs d USING(tconst) WHERE a.category IN ('actor','actress')),
 numbered AS (SELECT p.*,f.primaryTitle,f.startYear,f.averageRating,f.numVotes,
 row_number() OVER(PARTITION BY actor,director ORDER BY startYear,tconst) AS collaboration_number
 FROM pairs p JOIN films f USING(tconst) WHERE startYear IS NOT NULL),
 stats AS (SELECT actor,director,count(*) AS collaborations,
 max(CASE WHEN collaboration_number=1 AND numVotes>={min_votes} THEN averageRating END) AS first_rating,
 avg(CASE WHEN collaboration_number>1 AND numVotes>={min_votes} THEN averageRating END) AS later_mean
 FROM numbered GROUP BY actor,director)
 SELECT x.*,a.primaryName AS actor_name,d.primaryName AS director_name,later_mean-first_rating AS rating_difference
 FROM stats x JOIN name_basics a ON x.actor=a.nconst JOIN name_basics d ON x.director=d.nconst
 WHERE collaborations>=2 AND first_rating IS NOT NULL AND later_mean IS NOT NULL'''
}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--question',choices=list(QUERIES)+['all'],default='all'); a=p.parse_args()
    s=session('exploration')
    try:
        views(s)
        for name in (QUERIES if a.question=='all' else [a.question]):
            output(s.sql(QUERIES[name].format(min_votes=MIN_VOTES)),f'exploration/{name}')
            if name=='q6':
                output(s.sql('''SELECT count(*) AS films, sum(CASE WHEN l.tconst IS NOT NULL THEN 1 ELSE 0 END) AS films_with_language_proxy
                FROM films f LEFT JOIN (SELECT DISTINCT titleId AS tconst FROM title_akas WHERE isOriginalTitle=true
                AND language IS NOT NULL AND language NOT IN ('und','zxx','mul')) l USING(tconst)'''),'exploration/q6_coverage')
    finally: s.stop()
if __name__=='__main__': main()
