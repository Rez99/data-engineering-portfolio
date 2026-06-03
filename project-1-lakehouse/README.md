|Stage|Architecture|Stack|Storage Cost|Memory Cost|Compute Cost|Notes|
|---|---|---|---|---|---|---|
|1|File Analytics|CSV + Pandas|🟠 Medium|🔴 High|🟢 Low|Simple and flexible for exploratory analysis, but limited by available memory.|
|2|OLTP Database|Postgres|🔴 High|🟢 Low|🔴 High|Optimized for transactions and updates, not large analytical scans.|
|3|OLAP Database|DuckDB|🟢 Low|🟢 Low|🟢 Low|Columnar OLAP systems dramatically reduce storage and query costs for analytics.|
|4|Lakehoue Architecture|Iceberg + Polaris + DuckDB|🟢 Low|🟢 Low|🟢 Low|Lakehouses decouple storage, metadata, and compute while retaining warehouse capabilities.|