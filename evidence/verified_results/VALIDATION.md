# Actual execution validation

All seven fresh downloads passed complete gzip decoding and SHA256 recording.
The main Spark run completed Q1–Q8 and the additional open query with exit code 0.
CSV records were counted and matched status.json for every query.
Q6 returned no rows and the original-title language coverage was 0 / 431386 films: it is inconclusive, not a completed factual multilingual-actor answer.
Q8's role labels were added from the same cleaned snapshot after the main run. An initial supplementary JVM disconnected; a retry with early filtering and small broadcast joins completed successfully. Row counts were unchanged and size(career_role_names) matched career_roles for every row.
Q4 film titles were joined to the same cleaned title table; the primary query results remain in q4.csv.
The evidence logs distinguish the failed optional attempt from the successful main run and successful retry.
The ZIP does not include raw or intermediate IMDb datasets, or a team report.

CSV files were exported with doubled-quote escaping and checked with a standard CSV parser: all rows have the expected columns, record counts match, and every Q8 JSON role list parses with the expected size.
