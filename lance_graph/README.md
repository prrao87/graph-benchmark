# Lance Graph

This section describes how to build and query a graph of the social network data
using Lance Graph. The graph is stored as Lance datasets on disk, and the Cypher
engine interprets those datasets via a GraphConfig mapping.

> [!NOTE]
> All timing numbers shown below are on an M5 MacBook Pro (10-core CPU: 4 performance + 6 efficiency cores) with 24 GB of RAM, running macOS 26.6.2.

## Setup

Lance Graph runs locally and reads Lance datasets directly from disk.

```sh
uv add lance-graph lance pyarrow polars
```

## Build graph

The script `build_graph.py` converts the Parquet node/edge datasets under
`data/output` into Lance datasets under `lance_graph/graph_lance`.

```sh
uv run build_graph.py
```

## Ingestion performance

## Query graph

The script `query.py` runs the same suite of Cypher queries as Ladybug and prints
the results as Polars DataFrames. Parameterized predicates now use Lance Graph's
native `$param` support via `CypherQuery.with_parameter(...)`.

```sh
uv run query.py
```

### Results

```
Query 1:
 
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.id AS personid, person.name AS name, count(follower.id) AS numfollowers
        ORDER BY numfollowers DESC LIMIT 3
    
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
 
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
        RETURN person.name AS name, count(follower.id) as numfollowers, city.city AS city, city.state AS state, city.country AS country
        ORDER BY numfollowers DESC LIMIT 1
    
shape: (1, 5)
┌─────────────────┬──────────────┬────────┬───────┬───────────────┐
│ name            ┆ numFollowers ┆ city   ┆ state ┆ country       │
│ ---             ┆ ---          ┆ ---    ┆ ---   ┆ ---           │
│ str             ┆ i64          ┆ str    ┆ str   ┆ str           │
╞═════════════════╪══════════════╪════════╪═══════╪═══════════════╡
│ Katherine Ewing ┆ 4998         ┆ Austin ┆ Texas ┆ United States │
└─────────────────┴──────────────┴────────┴───────┴───────────────┘

Query 3:
 
        MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageage
        ORDER BY averageage LIMIT 5
    
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
 
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personcounts
        ORDER BY personcounts DESC LIMIT 3
    
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
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE i.interest = $interest
        AND p.gender = $gender
        AND c.city = $city AND c.country = $country
        RETURN count(p) AS numpersons
    
shape: (1, 1)
┌────────────┐
│ numPersons │
│ ---        │
│ i64        │
╞════════════╡
│ 48         │
└────────────┘

Query 6:
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE i.interest = $interest
        AND p.gender = $gender
        RETURN count(p.id) AS numpersons, c.city AS city, c.country AS country
        ORDER BY numpersons DESC LIMIT 5
    
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
 
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        AND i.interest = $interest
        RETURN count(p.id) AS numpersons, s.state AS state, s.country AS country
        ORDER BY numpersons DESC LIMIT 1
    
shape: (1, 3)
┌────────────┬────────────┬───────────────┐
│ numPersons ┆ state      ┆ country       │
│ ---        ┆ ---        ┆ ---           │
│ i64        ┆ str        ┆ str           │
╞════════════╪════════════╪═══════════════╡
│ 117        ┆ California ┆ United States │
└────────────┴────────────┴───────────────┘

Query 8:
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numpaths
    
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 58431994 │
└──────────┘

Query 9:
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numpaths
    
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 45362926 │
└──────────┘
Queries completed in 0.4812s
```

## Query performance

```sh
$ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
============================= test session starts ==============================
platform darwin -- Python 3.13.14, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items

benchmark_query.py .........                                             [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1      21.1973 (5.46)      23.6905 (4.94)      21.9959 (5.37)     0.5128 (7.33)      21.8871 (5.45)     0.6370 (7.43)          8;1   45.4630 (0.19)         33           1
test_benchmark_query2      40.6932 (10.47)     50.4283 (10.51)     44.9270 (10.96)    3.0540 (43.63)     43.9210 (10.93)    5.4490 (63.55)         8;0   22.2583 (0.09)         24           1
test_benchmark_query3       5.6832 (1.46)       6.7475 (1.41)       5.9275 (1.45)     0.1494 (2.13)       5.9150 (1.47)     0.1754 (2.05)         46;3  168.7055 (0.69)        163           1
test_benchmark_query4       3.9742 (1.02)       5.4312 (1.13)       4.1581 (1.01)     0.1227 (1.75)       4.1428 (1.03)     0.0866 (1.01)        29;13  240.4923 (0.99)        219           1
test_benchmark_query5       3.8850 (1.0)       15.9477 (3.32)       4.0980 (1.0)      0.8168 (11.67)      4.0188 (1.0)      0.0857 (1.0)           3;8  244.0193 (1.0)         236           1
test_benchmark_query6       4.4770 (1.15)       4.7996 (1.0)        4.6217 (1.13)     0.0700 (1.0)        4.6223 (1.15)     0.0901 (1.05)         77;1  216.3720 (0.89)        210           1
test_benchmark_query7       5.3456 (1.38)       5.8515 (1.22)       5.5312 (1.35)     0.0831 (1.19)       5.5225 (1.37)     0.0996 (1.16)         52;3  180.7939 (0.74)        177           1
test_benchmark_query8     114.1072 (29.37)    116.5610 (24.29)    115.2548 (28.12)    0.8447 (12.07)    115.1952 (28.66)    1.3518 (15.76)         3;0    8.6764 (0.04)          9           1
test_benchmark_query9      91.0889 (23.45)     96.1538 (20.03)     93.5529 (22.83)    1.4669 (20.96)     93.8689 (23.36)    1.6869 (19.67)         4;0   10.6891 (0.04)         11           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================== 9 passed in 11.37s ==============================
```
