# DuckPGQ graph

This section describes how to build and query a graph of the social network data in [DuckDB](https://duckdb.org/) using the [DuckPGQ](https://duckpgq.org/) community extension. DuckPGQ adds SQL/PGQ (the graph query extension of the SQL:2023 standard) to DuckDB, so the graph is declared as a *property graph* over regular DuckDB tables and queried with `GRAPH_TABLE (... MATCH ...)` clauses embedded in SQL.

> [!NOTE]
> All timing numbers shown below are on an M5 Pro Macbook Pro with 48 GB of RAM, using DuckDB 1.5.6 and the DuckPGQ community extension (`61f5d082`).

## Setup

DuckDB is an embedded database, so there is no server to run. The `duckdb` Python package is part of the project dependencies (`uv sync`), and the DuckPGQ extension is installed from the DuckDB community extension repository the first time the scripts run:

```sql
INSTALL duckpgq FROM community;
LOAD duckpgq;
```

### Using a locally built DuckPGQ

To benchmark a locally built extension (for example from DuckPGQ's `main` branch), point the `DUCKPGQ_EXTENSION_PATH` environment variable at the `duckpgq.duckdb_extension` binary. The scripts then `LOAD` it directly (with `allow_unsigned_extensions` enabled) instead of installing the community build. The installed `duckdb` Python package must match the DuckDB version the extension was built against.

```sh
DUCKPGQ_EXTENSION_PATH=/path/to/duckpgq.duckdb_extension uv run query.py
```

## Build graph

The script `build_graph.py` reads the Parquet node/edge files under `data/output` into DuckDB tables and declares a property graph named `social_network` on top of them. The edge files use `from`/`to` as column names, which are reserved words in SQL, so they are renamed to `src`/`dst` during ingestion.

```sh
uv run build_graph.py
```

The property graph definition looks like this:

```sql
CREATE PROPERTY GRAPH social_network
VERTEX TABLES (Person, City, State, Country, Interest)
EDGE TABLES (
    Follows     SOURCE KEY (src) REFERENCES Person (id) DESTINATION KEY (dst) REFERENCES Person (id)   LABEL Follows,
    LivesIn     SOURCE KEY (src) REFERENCES Person (id) DESTINATION KEY (dst) REFERENCES City (id)     LABEL LivesIn,
    HasInterest SOURCE KEY (src) REFERENCES Person (id) DESTINATION KEY (dst) REFERENCES Interest (id) LABEL HasInterest,
    CityIn      SOURCE KEY (src) REFERENCES City (id)   DESTINATION KEY (dst) REFERENCES State (id)    LABEL CityIn,
    StateIn     SOURCE KEY (src) REFERENCES State (id)  DESTINATION KEY (dst) REFERENCES Country (id)  LABEL StateIn
);
```

## Ingestion performance

The numbers shown below are for when we ingest 100K person nodes, ~10K location nodes and ~2.4M edges into the graph. DuckDB reads the Parquet files directly, so ingestion is very fast.

```bash
# Graph has 100K nodes and ~2.4M edges
$ uv run build_graph.py
Nodes loaded in 0.0380s
Edges loaded in 0.0261s
Created property graph 'social_network'
Successfully loaded nodes and edges into DuckDB
```

## Query graph

The script `query.py` contains a suite of queries that can be run to benchmark various aspects of the DB's performance. The queries are the same as the Cypher queries used for the other systems, translated to SQL/PGQ.

```sh
uv run query.py
```

A few notes on the translation:

* The pattern inside `GRAPH_TABLE` returns a table via the `COLUMNS (...)` clause. Aggregations, `ORDER BY` and `LIMIT` are expressed in the surrounding SQL.
* DuckPGQ currently requires every edge pattern to bind a variable, so `-[:Follows]->` is written as `-[f:Follows]->`.
* Prepared-statement parameters (`$param`) are not yet supported inside `GRAPH_TABLE` clauses, so query parameters are passed as DuckDB session variables (`SET VARIABLE` + `getvariable()`). The values are constant-folded during planning, so filters are pushed down into the scans as if they were literals.
* Queries 3 and 4 use the explicit two-hop `City -> State -> Country` path instead of the variable-length `-[*1..2]->` pattern used in the Cypher versions.

### Results
```

Query 1:
 
        SELECT personID, name, count(*) AS numFollowers
        FROM GRAPH_TABLE (social_network
            MATCH (follower:Person)-[f:Follows]->(person:Person)
            COLUMNS (person.id AS personID, person.name AS name)
        )
        GROUP BY personID, name
        ORDER BY numFollowers DESC LIMIT 3;
    
Top 3 most-followed persons:
shape: (3, 3)
┌──────────┬─────────────────┬──────────────┐
│ personID ┆ name            ┆ numFollowers │
│ ---      ┆ ---             ┆ ---          │
│ i64      ┆ str             ┆ i64          │
╞══════════╪═════════════════╪══════════════╡
│ 85723    ┆ Katherine Ewing ┆ 4998         │
│ 68753    ┆ Devon Pineda    ┆ 4985         │
│ 54696    ┆ Dakota Lawrence ┆ 4976         │
└──────────┴─────────────────┴──────────────┘

Query 2:
 
        WITH most_followed AS (
            SELECT personID, count(*) AS numFollowers
            FROM GRAPH_TABLE (social_network
                MATCH (follower:Person)-[f:Follows]->(person:Person)
                COLUMNS (person.id AS personID)
            )
            GROUP BY personID
            ORDER BY numFollowers DESC LIMIT 1
        )
        SELECT name, numFollowers, city, state, country
        FROM GRAPH_TABLE (social_network
            MATCH (person:Person)-[l:LivesIn]->(city:City)
            COLUMNS (
                person.id AS personID,
                person.name AS name,
                city.city AS city,
                city.state AS state,
                city.country AS country
            )
        )
        JOIN most_followed USING (personID);
    
City in which most-followed person lives:
shape: (1, 5)
┌─────────────────┬──────────────┬────────┬───────┬───────────────┐
│ name            ┆ numFollowers ┆ city   ┆ state ┆ country       │
│ ---             ┆ ---          ┆ ---    ┆ ---   ┆ ---           │
│ str             ┆ i64          ┆ str    ┆ str   ┆ str           │
╞═════════════════╪══════════════╪════════╪═══════╪═══════════════╡
│ Katherine Ewing ┆ 4998         ┆ Austin ┆ Texas ┆ United States │
└─────────────────┴──────────────┴────────┴───────┴───────────────┘

Query 3:
 
        SELECT city, avg(age) AS averageAge
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(c:City)-[ci:CityIn]->(s:State)-[si:StateIn]->(co:Country)
            WHERE co.country = getvariable('country')
            COLUMNS (c.city AS city, p.age AS age)
        )
        GROUP BY city
        ORDER BY averageAge LIMIT 5;
    
Cities with lowest average age in United States:
shape: (5, 2)
┌─────────────┬────────────┐
│ city        ┆ averageAge │
│ ---         ┆ ---        │
│ str         ┆ f64        │
╞═════════════╪════════════╡
│ Seattle     ┆ 40.386619  │
│ Dallas      ┆ 40.438012  │
│ Kansas City ┆ 40.492504  │
│ Miami       ┆ 40.499395  │
│ Austin      ┆ 40.509249  │
└─────────────┴────────────┘

Query 4:
 
        SELECT countries, count(*) AS personCounts
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(ci:City)-[cin:CityIn]->(s:State)-[sin:StateIn]->(country:Country)
            WHERE p.age >= getvariable('age_lower') AND p.age <= getvariable('age_upper')
            COLUMNS (country.country AS countries)
        )
        GROUP BY countries
        ORDER BY personCounts DESC LIMIT 3;
    
Persons between ages 30-40 in each country:
shape: (3, 2)
┌────────────────┬──────────────┐
│ countries      ┆ personCounts │
│ ---            ┆ ---          │
│ str            ┆ i64          │
╞════════════════╪══════════════╡
│ United States  ┆ 30741        │
│ Canada         ┆ 2976         │
│ United Kingdom ┆ 1864         │
└────────────────┴──────────────┘

Query 5:
 
        SELECT count(*) AS numPersons
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[h:HasInterest]->(i:Interest), (p:Person)-[l:LivesIn]->(c:City)
            WHERE lower(i.interest) = lower(getvariable('interest'))
              AND lower(p.gender) = lower(getvariable('gender'))
              AND c.city = getvariable('city') AND c.country = getvariable('country')
            COLUMNS (p.id AS personID)
        );
    
Number of male users in London, United Kingdom who have an interest in fine dining:
shape: (1, 1)
┌────────────┐
│ numPersons │
│ ---        │
│ i64        │
╞════════════╡
│ 48         │
└────────────┘

Query 6:
 
        SELECT count(*) AS numPersons, city, country
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[h:HasInterest]->(i:Interest), (p:Person)-[l:LivesIn]->(c:City)
            WHERE lower(i.interest) = lower(getvariable('interest'))
              AND lower(p.gender) = lower(getvariable('gender'))
            COLUMNS (c.city AS city, c.country AS country)
        )
        GROUP BY city, country
        ORDER BY numPersons DESC LIMIT 5;
    
City with the most female users who have an interest in tennis:
shape: (5, 3)
┌────────────┬─────────────┬────────────────┐
│ numPersons ┆ city        ┆ country        │
│ ---        ┆ ---         ┆ ---            │
│ i64        ┆ str         ┆ str            │
╞════════════╪═════════════╪════════════════╡
│ 77         ┆ Birmingham  ┆ United Kingdom │
│ 67         ┆ Charlotte   ┆ United States  │
│ 67         ┆ Kansas City ┆ United States  │
│ 65         ┆ Portland    ┆ United States  │
│ 65         ┆ Montreal    ┆ Canada         │
└────────────┴─────────────┴────────────────┘

Query 7:
 
        SELECT count(*) AS numPersons, state, country
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(c:City)-[ci:CityIn]->(s:State), (p:Person)-[h:HasInterest]->(i:Interest)
            WHERE p.age >= getvariable('age_lower') AND p.age <= getvariable('age_upper')
              AND s.country = getvariable('country')
              AND lower(i.interest) = lower(getvariable('interest'))
            COLUMNS (s.state AS state, s.country AS country)
        )
        GROUP BY state, country
        ORDER BY numPersons DESC LIMIT 1;
    

        State in United States with the most users between ages 23-30 who have an interest in photography:
shape: (1, 3)
┌────────────┬────────────┬───────────────┐
│ numPersons ┆ state      ┆ country       │
│ ---        ┆ ---        ┆ ---           │
│ i64        ┆ str        ┆ str           │
╞════════════╪════════════╪═══════════════╡
│ 117        ┆ California ┆ United States │
└────────────┴────────────┴───────────────┘
        

Query 8:
 
        SELECT count(*) AS numPaths
        FROM GRAPH_TABLE (social_network
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            COLUMNS (a.id AS personID)
        );
    

        Number of second-degree paths:
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 58431994 │
└──────────┘
        

Query 9:
 
        SELECT count(*) AS numPaths
        FROM GRAPH_TABLE (social_network
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            WHERE b.age < getvariable('age_1') AND c.age > getvariable('age_2')
            COLUMNS (a.id AS personID)
        );
    

        Number of paths through persons below 50 to persons above 25:
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 45362926 │
└──────────┘
        
Queries completed in 0.4373s
```

## Query performance

```sh
$ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
============================= test session starts ==============================
platform darwin -- Python 3.13.15, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/dwolde/git/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items

benchmark_query.py .........                                             [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1       8.4900 (4.38)       9.5504 (3.07)       8.7872 (4.23)     0.3471 (5.12)       8.6702 (4.24)     0.1613 (2.31)          2;2  113.8015 (0.24)         12           1
test_benchmark_query2       5.1435 (2.65)      14.2600 (4.58)       5.5352 (2.66)     0.8976 (13.24)      5.3945 (2.64)     0.2012 (2.88)          4;9  180.6604 (0.38)        169           1
test_benchmark_query3       3.7844 (1.95)      10.6868 (3.43)       4.1087 (1.98)     0.5173 (7.63)       4.0424 (1.97)     0.1803 (2.59)          7;8  243.3850 (0.51)        195           1
test_benchmark_query4       2.4524 (1.26)       3.1136 (1.0)        2.6342 (1.27)     0.0855 (1.26)       2.6298 (1.28)     0.0980 (1.41)         80;9  379.6156 (0.79)        347           1
test_benchmark_query5       1.9388 (1.0)        5.2637 (1.69)       2.0794 (1.0)      0.2015 (2.97)       2.0471 (1.0)      0.0938 (1.35)        16;25  480.8996 (1.0)         382           1
test_benchmark_query6       2.6896 (1.39)       3.3700 (1.08)       2.8458 (1.37)     0.0678 (1.0)        2.8419 (1.39)     0.0697 (1.0)         74;10  351.3978 (0.73)        323           1
test_benchmark_query7       2.5558 (1.32)       3.1488 (1.01)       2.7080 (1.30)     0.0759 (1.12)       2.6984 (1.32)     0.0747 (1.07)        76;17  369.2701 (0.77)        339           1
test_benchmark_query8     165.9533 (85.59)    166.8996 (53.60)    166.4807 (80.06)    0.3350 (4.94)     166.4379 (81.30)    0.4123 (5.91)          2;0    6.0067 (0.01)          6           1
test_benchmark_query9     157.6740 (81.32)    159.8213 (51.33)    158.5235 (76.23)    0.7168 (10.57)    158.4049 (77.38)    0.8899 (12.76)         2;0    6.3082 (0.01)          7           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================== 9 passed in 9.54s ===============================
```
