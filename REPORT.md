# Report - Group gr-04

<!--
Reminder: AI tools are strictly prohibited for writing the explanations and
reasoning in this file. Each owner writes their own sections by hand.
Owners below are the proposed split and can be adjusted.
-->

| Section | Owner |
|---|---|
| 1. Data preparation | Ahmed El Monaser |
| 2. Data exploration (Q1-Q4) | Ahmed El Monaser |
| 2. Data exploration (Q5-Q6) | Ahmed Amine ZAHRAOUI |
| 2. Data exploration (Q7-Q8) | Lionel MONTEIRO WORA |
| 3. Data enrichment | Ahmed Amine ZAHRAOUI |
| 4. Streaming | Lionel MONTEIRO WORA |
| 5. Open question | Lionel MONTEIRO WORA |

## 1. Data preparation

### 1.1 Null, missing and empty values per column

<!-- TODO: one table per IMDb table, % of nulls / `\N` / empty values for each column -->

### 1.2 Five columns with the highest percentage of nulls

<!-- TODO: ranked table across the whole dataset -->

### 1.3 Other data quality issues

<!-- TODO: issues found (e.g. deathYear < birthYear, foreign keys without parent), with counts -->

### 1.4 Handling of problematic values

<!-- TODO: what was implemented for each issue, rows affected, location of the cleaned tables -->

### 1.5 Methodological Explanation

<!-- TODO: methodology and why it was chosen; alternatives considered and trade-offs -->

## 2. Data exploration

### 2.1 Movies whose length differs notably from the genre average

<!-- TODO: results table; definition of "notably different" -->

### 2.2 Top three genres per year (1980-2025)

<!-- TODO: results table; definition of "popular" -->

### 2.3 Top three genres compared with the other genres

<!-- TODO: comparison and key takeaways -->

### 2.4 Directors with exactly one movie >= 8.0 and other movies averaging <= 6.0

<!-- TODO: results table -->

### 2.5 TV series (>= 3 seasons of >= 10 episodes, worst episode in last season, best in first two)

<!-- TODO: results table -->

### 2.6 Actors successful in several languages

<!-- TODO: actors with success in several languages, and in >= 3 languages; definition of "success" -->

### 2.7 Actors whose popularity peaked early in their career

<!-- TODO: 20+ years of career and >= 15 movies; definition of "peaked early" -->

### 2.8 Multi-talented film professionals

<!-- TODO: combinations of writer/director/actor; performance when holding several roles on the same project -->

### 2.9 Methodological Explanation

<!-- TODO: methodology and why it was chosen; alternatives considered and trade-offs -->

## 3. Data enrichment

### 3.1 Results

<!-- TODO: sample of the enriched data (synopsis, principals' biographies, source_url, notes for missing information), coverage statistics -->

### 3.2 Methodological Explanation

<!-- TODO: source chosen and why, rate limiting, lineage, Spark processing, scalability to the full dataset; alternatives and trade-offs -->

## 4. Streaming

### 4.1 Results

<!-- TODO: filtering criteria, test window (15-30 min), events captured and processed, streaming metrics -->

### 4.2 Late arriving data

<!-- TODO: chosen method to deal with late data -->

### 4.3 Methodological Explanation

<!-- TODO: methodology and why it was chosen; alternatives considered and trade-offs -->

## 5. Open question

### 5.1 Question

<!-- TODO: the question, why it is interesting, additional data required (if any) -->

### 5.2 Results

<!-- TODO: output tables or summary metrics, key takeaways -->

### 5.3 Methodological Explanation

<!-- TODO: methodology and why it was chosen; alternatives considered -->
