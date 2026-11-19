# Big Data Processing — Report — Group gr-04

This report presents the available IMDb .

Contributors: Lionel MONTEIRO WORA and Ahmed El Monaser.

## Evidence and execution record

The tables below are transcribed from saved outputs of the fresh-snapshot batch run. 

| Section | Evidence status |
|---|---|
| Preparation | Cleaning executed; complete current-snapshot column-quality reports still need to be added |
| Q1–Q5, Q7–Q8 | Full query outputs available |
| Q6 | Query executed; film-language coverage for the chosen proxy is zero; substantive question remains unanswered |
| Enrichment | No completed full online collection/coverage evidence supplied |
| Streaming | No completed live test and matching-event metrics supplied |
| Open question | Full query output available |
| Methodological explanations and conclusions | Included below for the available implementation and outputs |

## 1. Data preparation

### 1.1 Recorded row counts

Raw counts are line counts excluding the TSV header. Clean counts are Spark table counts after the pipeline's transformations. These counts do not replace column-level quality measurements.

| Table | Raw rows | Clean rows |
| --- | --- | --- |
| name.basics | 15818137 | 15818137 |
| title.akas | 60445719 | 60445708 |
| title.basics | 12965577 | 12965577 |
| title.crew | 12965577 | 12965577 |
| title.episode | 10036861 | 10036855 |
| title.principals | 102774786 | 102767310 |
| title.ratings | 1732558 | 1732558 |

### 1.2 Null, missing and empty values per column

The complete current-snapshot column profile is not included in the supplied evidence. Separate null, literal `\N`, empty and combined-missing counts and percentages are still required.

### 1.3 Five columns with the highest null percentage

The requested null-only ranking is not available. The existing `top_five_missing` script ranks combined missing values, which is a different metric.

### 1.4 Other quality issues and handling

The pipeline handles casts, duplicate keys, value rules and orphan references. Detailed current-snapshot issue counts and audit reports are not supplied, so their frequencies cannot be reported here.

### 1.5 Methodological Explanation

The pipeline separates loading, value cleaning and reference checks. Reading the TSV fields as strings first avoids losing invalid values during automatic type inference. The data are then converted to Parquet for repeated Spark queries. Missing markers are normalised, types are converted safely, and invalid keys or references are filtered.

An alternative is to query the TSV files directly. This uses less intermediate storage but repeats parsing and makes restarts slower. Parquet requires more disk space during preparation but supports reuse. Dropping invalid references makes joins more consistent, although it can remove records that contain some useful information. The row counts show the resulting table sizes; they do not measure every repair or replace the missing column-quality reports.

## 2. Data exploration

### Recorded query configuration

These are code parameters, not a methodological justification:

| Setting | Recorded value |
|---|---|
| Common film filter | `titleType = movie`, `runtimeMinutes >= 60` |
| Q1 threshold | Absolute deviation greater than 2 population standard deviations within genre |
| Q2/Q3 popularity and years | Sum of `numVotes`; 1980–2025 |
| Q4 eligible films | At least 1,000 votes; exactly one rated ≥8; at least two others averaging ≤6 |
| Q5 season criteria | At least 3 observed positive-numbered seasons, each ≥10 episodes; tied extrema accepted |
| Q6 success | Rating ≥7 and votes ≥10,000; original-title-language proxy |
| Q7 career/early period | At least 20 years and 15 recorded films; early first third; rating comparisons use films with ≥1,000 votes |
| Q8 roles | Actor/director/writer; at least 2 career roles; film comparisons use ≥1,000 votes |

The full SQL is in the linked execution record. There is no global release-year cutoff for every question.

### 2.1 Q1 — Duration outliers by genre

**Full output: 12,619 film–genre rows, 8,940 distinct films, 27 genres.**

[Complete output: q1.csv](tables/q1/page-001.md)

Excerpt: five largest absolute recorded z-scores.

| tconst | primaryTitle | genre | runtimeMinutes | mean_minutes | z_score |
| --- | --- | --- | --- | --- | --- |
| tt8273150 | Logistics | Documentary | 51420 | 88.73883849973895 | 193.98197229454684 |
| tt5136218 | London EC1 | Comedy | 5460 | 95.20311957653593 | 192.99082337102075 |
| tt3854496 | Ambiancé | Documentary | 43200 | 88.73883849973895 | 162.91841032118325 |
| tt5068890 | Hunger! | Drama | 6000 | 99.01700763176852 | 162.24278900717528 |
| tt1806963 | Matrjoschka | Drama | 5700 | 99.01700763176852 | 153.9945299010058 |

### Q1 interpretation

Q1 identifies durations that differ from the genre mean by more than two population standard deviations. The output contains 12,619 film–genre rows but only 8,940 distinct films because a film can belong to several genres. The longest entries strongly affect the distribution. A large z-score is a reason to inspect a record, not proof that its duration is wrong. A median and interquartile-range rule would be less sensitive to extreme values.

### 2.2 Q2 — Top three genres by year

**Full output: 138 rows across 46 years.**

[Complete output: q2.csv](tables/q2/page-001.md)

Excerpt: year 2025.

| year | rank | genre | votes | film_count |
| --- | --- | --- | --- | --- |
| 2025 | 1 | Drama | 8650682 | 4947 |
| 2025 | 2 | Action | 7707298 | 1237 |
| 2025 | 3 | Comedy | 5903306 | 2442 |

### Q2 interpretation

Q2 ranks genres by total votes for films released in each year. The 138 rows represent three genres for each of the 46 years from 1980 to 2025. In the 2025 excerpt, Drama ranks first, followed by Action and Comedy. These are accumulated votes at collection time, not votes cast during 2025. Ranking by average rating would answer a different question about ratings rather than audience voting activity.

### 2.3 Q3 — Top-three genres versus other genres

**Full output: 92 year/group rows.**

[Complete output: q3.csv](tables/q3/page-001.md)

Excerpt: year 2025. These are genre-membership aggregates; distinct-film counts can overlap across groups.

| year | bucket | genre_memberships | distinct_films | votes | mean_rating | mean_minutes | vote_share |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2025 | other | 12712 | 9779 | 34394233 | 6.208343697003946 | 96.34738829452486 | 0.6070764791687814 |
| 2025 | top3 | 8626 | 7356 | 22261286 | 6.206114828069078 | 102.3465105495015 | 0.39292352083121856 |

### Q3 comparison and key takeaways

For 2025, the top-three genre memberships have a mean rating of about 6.21, close to the other memberships. Their mean duration is longer, at about 102.35 minutes compared with 96.35. The combined other genres receive more votes than the top three together. Films with several genres can contribute repeatedly, so vote shares describe genre memberships and are not shares of a unique-film audience.

### 2.4 Q4 — Directors with one highly rated eligible film

**Full output: 47 directors under the recorded eligibility rules.**

[Complete output: q4.csv](tables/q4/page-001.md) · [Complete output: q4_with_titles.csv](tables/q4_with_titles/page-001.md)

Excerpt: Robert Rodriguez and Richard Kelly.

| primaryName | excellent_film_title | excellent_film | excellent | other_count | other_mean |
| --- | --- | --- | --- | --- | --- |
| Robert Rodriguez | Sin City | tt0401792 | 1 | 22 | 5.9409090909090905 |
| Richard Kelly | Donnie Darko | tt0246578 | 1 | 2 | 5.449999999999999 |

### Q4 interpretation

Q4 returns 47 directors with exactly one eligible film rated at least 8 and at least two other eligible films averaging no more than 6. For example, Sin City is the qualifying highly rated film for Robert Rodriguez in this output. The 1,000-vote threshold limits reliance on very small voting samples but excludes less widely rated films. The result describes this selection rule, not the overall quality of a director’s career.

### 2.5 Q5 — Series meeting the season and episode criteria

**Full output: 546 series.**

[Complete output: q5.csv](tables/q5/page-001.md)

Excerpt: five qualifying series with the highest recorded season counts (ties ordered by identifier).

| parentTconst | primaryTitle | seasons | last_season | worst | best |
| --- | --- | --- | --- | --- | --- |
| tt1796154 | Papiny dochki | 20 | 20 | 4.6 | 8.5 |
| tt6953640 | Roadkill | 13 | 13 | 6.8 | 9.5 |
| tt0369179 | Two and a Half Men | 12 | 12 | 3.6 | 8.7 |
| tt0094514 | Murphy Brown | 11 | 11 | 5.9 | 8.1 |
| tt4264964 | Evening Urgant | 11 | 11 | 7.2 | 9.4 |

### Q5 interpretation

Q5 returns 546 series under the implemented season and episode rules. At least three observed positive-numbered seasons are required, with at least ten episodes per season. Tied rating extrema are allowed. The excerpt lists qualifying series with the largest recorded season counts. These are counts in the dataset; missing episodes or season numbers can affect which series qualify.

### 2.6 Q6 — Success across distinct film languages

**Status: incomplete / inconclusive.**

- [Empty query output](tables/q6/page-001.md)
- [Measured language coverage](evidence/verified_results/q6_language_coverage.json)

| Recorded measurement | Value |
|---|---|
| Eligible films in the common film view | 431,386 |
| Films with the selected original-title-language proxy | 0 |
| Returned actor rows under that proxy | 0 |

**This output is not evidence that no multilingual actors exist.** No verified originally-French/English/etc. success list or ≥3-language list is available from this execution.

Next step: obtain verified film-language data, rerun the appropriate analysis, and insert both requested lists and coverage evidence.

### Q6 methodology, limitations and interpretation

The selected original-title-language proxy provides no language values for the 431,386 eligible films. As a result, the empty actor output cannot answer the question. Translation languages in alternative titles would not reliably establish the original or spoken language of a film. The next step is to obtain verified language data, measure its coverage and rerun the success criteria. No multilingual-actor conclusion can be drawn from the current output.

### 2.7 Q7 — Early-career rating peaks

**Full output: 5,375 actors/actresses under the recorded definition.**

[Complete output: q7.csv](tables/q7/page-001.md)

Excerpt: Al Pacino, Robert De Niro and Sylvester Stallone. Recorded career years/counts and rating-eligible subsets are different quantities.

| primaryName | first_year | last_year | films | early_mean | later_mean | early_max | later_max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Robert De Niro | 1968 | 2026 | 101 | 6.783333333333334 | 6.419999999999998 | 9.0 | 8.7 |
| Al Pacino | 1971 | 2026 | 58 | 7.269230769230769 | 6.439024390243904 | 9.2 | 8.3 |
| Sylvester Stallone | 1970 | 2025 | 61 | 6.157894736842106 | 5.757500000000001 | 8.1 | 7.6 |

### Q7 interpretation and definition of popularity

Q7 uses film ratings to compare the early first third of a recorded career with the later period. The selected actors have higher early mean and maximum ratings under this definition. For Al Pacino, the example means are approximately 7.27 early and 6.44 later. This does not directly measure fame or popularity. Career bounds also use recorded years, which can include announced titles. Alternatives include released-title-only career bounds or equal numbers of films in each period.

### 2.8 Q8 — Multi-role professionals

**Full output: 46,330 comparison rows representing 37,670 people.**

[Complete output: q8.csv](tables/q8/page-001.md)

Excerpt: comparison rows for Clint Eastwood, Woody Allen and Ben Affleck, when present in the output.

| primaryName | career_role_names | role_mode | films | mean_rating | mean_votes |
| --- | --- | --- | --- | --- | --- |
| Ben Affleck | actor, director, writer | multiple | 8 | 7.25 | 401851.875 |
| Ben Affleck | actor, director, writer | single | 44 | 6.127272727272726 | 159596.56818181818 |
| Clint Eastwood | actor, director | multiple | 26 | 6.888461538461538 | 135292.53846153847 |
| Clint Eastwood | actor, director | single | 40 | 6.902500000000001 | 131513.925 |
| Woody Allen | actor, director, writer | multiple | 54 | 6.887037037037037 | 68042.11111111111 |
| Woody Allen | actor, director, writer | single | 7 | 5.871428571428572 | 31750.571428571428 |

### Q8 comparison and interpretation

Q8 separates films where a person has one of the selected roles from films where the same person has multiple selected roles. Ben Affleck has a higher mean rating in the multiple-role group in this excerpt, whereas Clint Eastwood’s two means are close. The effect is therefore not uniform. The comparison does not show that taking several roles causes a higher rating: film selection, sample size and other factors differ between groups. The 46,330 rows represent 37,670 people because a person may appear in both groups.

### 2.9 Methodological Explanation — Exploration

The exploration uses Spark SQL to join IMDb identifiers and calculate grouped statistics. Explicit thresholds keep the queries reproducible: films are generally movies of at least 60 minutes, and several comparisons use a minimum of 1,000 votes. These thresholds reduce the influence of small samples but exclude some films.

Total votes are used for genre popularity, while ratings are used for several career and collaboration comparisons. These measures should not be treated as interchangeable. Multi-genre membership, incomplete language coverage and the chosen career boundaries also affect interpretation. Alternative thresholds or robust statistics could be compared in a sensitivity analysis; no such additional analysis is included in the supplied results.

## 3. Data enrichment

### 3.1 Results

Full-scope enrichment has not been completed in the supplied evidence. Synthetic tests establish functionality only.

| Required evidence | Current entry |
|---|---|
| Top 10 feature films per genre/year, 1980–2025 | Not measured or not supplied |
| Film synopses and explicit source URLs | Not measured or not supplied |
| Biographies for at least three principals per film | Not measured or not supplied |
| Missing/ambiguous/error records | Not measured or not supplied |
| Overall text coverage | Not measured or not supplied |
| API collection evidence | Not measured or not supplied |

### 3.2 Methodological Explanation — Enrichment

The implemented enrichment selects films by votes within each genre and year and resolves IMDb identifiers through Wikidata to Wikipedia pages. Identifier-based lookup is preferable to title-only search because different films can share a name. Plot or synopsis sections are preferred, with labelled introduction fallbacks.

Spark then normalises the text and joins it to IMDb records. Explicit missing and error statuses allow coverage to be measured instead of silently dropping unsuccessful lookups. Wikipedia has uneven coverage, and API collection requires retry and rate-limit handling. Another source might improve coverage but would introduce different access and licensing constraints. Only synthetic joins are evidenced here; full collection quality and coverage have not been measured.

## 4. Streaming data processing


### 4.1Late-arriving data

The consumer implementation uses a ten-minute event-time watermark. This allows bounded handling of out-of-order events and limits state retention for relevant stateful operations. Events arriving beyond the applicable watermark may no longer contribute to those operations. The exact observed behaviour must be checked in the live consumer metrics. There are no measured late-row counts in the supplied evidence. A longer delay could accommodate later events but would retain state for longer.

### 4.2 Methodological Explanation — Streaming

The producer sends live Wikipedia edit and new-page events to Kafka. Spark reads the stream and compares normalised page titles with IMDb film and person names. This is a simple candidate-matching approach, but shared names and ambiguous titles can create false matches. Entity identifiers would offer stronger matching where available.

The match sink deduplicates event/IMDb pairs. Window outputs are batch snapshots and must not be summed as independent event totals. Kafka and persisted checkpoints support recovery, at the cost of additional operational setup. Without a completed live observation window, throughput, delivery reliability and the number of valid matches remain unmeasured.

## 5. Open question — First and subsequent actor/director collaborations

### 5.1 Question and relevance

Recorded query topic: film-rating differences between the first and subsequent collaborations of an actor/director pair.

Research question: how do film ratings differ between the first and later collaborations of an actor/director pair? This explores whether repeated collaboration is associated with different ratings. The analysis needs film credits, release years and ratings.

### 5.2 Results

**Full output: 24,653 eligible actor/director pairs.**

[Complete output: open.csv](tables/open/page-001.md)

Excerpt: five pairs with the highest recorded collaboration counts (ties ordered by identifiers).

| actor_name | director_name | collaborations | first_rating | later_mean | rating_difference |
| --- | --- | --- | --- | --- | --- |
| Lina Romay | Jesús Franco | 90 | 4.6 | 4.385714285714285 | -0.2142857142857144 |
| Howard Vernon | Jesús Franco | 32 | 6.1 | 5.199999999999999 | -0.9000000000000004 |
| Joan Sims | Gerald Thomas | 30 | 6.2 | 6.029166666666666 | -0.17083333333333428 |
| Carlo Verdone | Carlo Verdone | 28 | 7.2 | 6.177272727272728 | -1.0227272727272725 |
| Woody Allen | Woody Allen | 27 | 5.8 | 7.076923076923077 | 1.2769230769230768 |

### 5.3 Key takeaways

The output contains 24,653 eligible actor/director pairs. The examples show both increases and decreases after the first collaboration. For Lina Romay and Jesús Franco, the later mean is about 0.21 points lower; for Woody Allen in both roles, it is about 1.28 points higher. These examples do not establish the overall trend across every pair, and they do not show a causal effect of repeated collaboration.

### 5.4 Methodological Explanation — Open question

The query joins actor and director credits by film, orders collaborations over time and compares the first film rating with the mean of later ratings. This gives an interpretable comparison for each eligible pair. A mean can be influenced by extreme ratings, so a median or vote-weighted comparison would be useful alternatives. Same-person actor/director pairs are retained and should be distinguished from collaborations between two different people. Results also depend on credit coverage, rating eligibility and the ordering used when release years tie.

