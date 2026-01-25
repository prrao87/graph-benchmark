# Lance Graph

This section describes how to build and query a graph of the social network data
using Lance Graph. The graph is stored as Lance datasets on disk, and the Cypher
engine interprets those datasets via a GraphConfig mapping.

> [!NOTE]
> All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

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
the results as Polars DataFrames.

```sh
uv run query.py
```

### Results

```
Query 1:
 
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.id AS personid, person.name AS name, count(follower.id) AS numfollowers
        ORDER BY numfollowers DESC LIMIT 3
    
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
 
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
        RETURN person.name AS name, count(follower.id) as numfollowers, city.city AS city, city.state AS state, city.country AS country
        ORDER BY numfollowers DESC LIMIT 1
    
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
 
        MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageage
        ORDER BY averageage LIMIT 5
    
Cities with lowest average age in United States:
shape: (5, 2)
┌─────────────┬────────────┐
│ city        ┆ averageAge │
│ ---         ┆ ---        │
│ str         ┆ f64        │
╞═════════════╪════════════╡
│ Seattle     ┆ 39.655417  │
│ Dallas      ┆ 39.720371  │
│ Austin      ┆ 39.771676  │
│ Kansas City ┆ 39.771793  │
│ Miami       ┆ 39.773881  │
└─────────────┴────────────┘

Query 4:
 
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personcounts
        ORDER BY personcounts DESC LIMIT 3
    
Persons between ages 30-40 in each country:
shape: (3, 2)
┌────────────────┬──────────────┐
│ countries      ┆ personCounts │
│ ---            ┆ ---          │
│ str            ┆ i64          │
╞════════════════╪══════════════╡
│ United States  ┆ 30714        │
│ Canada         ┆ 2986         │
│ United Kingdom ┆ 1842         │
└────────────────┴──────────────┘

Query 5:
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        AND c.city = $city AND c.country = $country
        RETURN count(p) AS numpersons
    
Number of male users in London, United Kingdom who have an interest in fine dining:
shape: (1, 1)
┌────────────┐
│ numPersons │
│ ---        │
│ i64        │
╞════════════╡
│ 4368       │
└────────────┘

Query 6:
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        RETURN count(p.id) AS numpersons, c.city AS city, c.country AS country
        ORDER BY numpersons DESC LIMIT 5
    
City with the most female users who have an interest in tennis:
shape: (5, 3)
┌────────────┬──────────────┬───────────────┐
│ numPersons ┆ city         ┆ country       │
│ ---        ┆ ---          ┆ ---           │
│ i64        ┆ str          ┆ str           │
╞════════════╪══════════════╪═══════════════╡
│ 4579       ┆ Dallas       ┆ United States │
│ 4569       ┆ Philadelphia ┆ United States │
│ 4545       ┆ New York     ┆ United States │
│ 4506       ┆ Portland     ┆ United States │
│ 4506       ┆ Sacramento   ┆ United States │
└────────────┴──────────────┴───────────────┘

Query 7:
 
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        AND lower(i.interest) = lower($interest)
        RETURN count(p.id) AS numpersons, s.state AS state, s.country AS country
        ORDER BY numpersons DESC LIMIT 1
    

        State in United States with the most users between ages 23-30 who have an interest in photography:
shape: (1, 3)
┌────────────┬──────────┬───────────────┐
│ numPersons ┆ state    ┆ country       │
│ ---        ┆ ---      ┆ ---           │
│ i64        ┆ str      ┆ str           │
╞════════════╪══════════╪═══════════════╡
│ 5074       ┆ New York ┆ United States │
└────────────┴──────────┴───────────────┘
        

Query 8:
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numpaths
    

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
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numpaths
    

        Number of paths through persons below 50 to persons above 25:
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 45344112 │
└──────────┘
```

## Query performance

```
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
============================== test session starts ==============================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                               

benchmark_query.py .........                                              [100%]


-------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median                IQR            Outliers       OPS            Rounds  Iterations
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1      26.9350 (4.12)      31.9142 (4.23)      29.1461 (4.15)     1.5569 (8.52)      29.0399 (4.16)      1.9710 (8.05)          2;0   34.3099 (0.24)          8           1
test_benchmark_query2      60.0053 (9.18)      92.5294 (12.26)     71.2966 (10.16)    8.9412 (48.96)     72.3725 (10.37)    14.9065 (60.91)         6;0   14.0259 (0.10)         17           1
test_benchmark_query3       9.0629 (1.39)      10.2294 (1.35)       9.6246 (1.37)     0.2704 (1.48)       9.5786 (1.37)      0.3521 (1.44)         16;0  103.9005 (0.73)         59           1
test_benchmark_query4       6.5372 (1.0)        7.5498 (1.0)        7.0198 (1.0)      0.1826 (1.0)        6.9807 (1.0)       0.2448 (1.0)          27;2  142.4546 (1.0)         110           1
test_benchmark_query5       7.6128 (1.16)       8.8082 (1.17)       8.0441 (1.15)     0.2708 (1.48)       8.0123 (1.15)      0.4007 (1.64)         33;0  124.3146 (0.87)        101           1
test_benchmark_query6      10.0817 (1.54)      11.7397 (1.55)      10.7463 (1.53)     0.3963 (2.17)      10.6860 (1.53)      0.5477 (2.24)         30;0   93.0550 (0.65)         89           1
test_benchmark_query7       8.9319 (1.37)       9.8633 (1.31)       9.3121 (1.33)     0.1926 (1.05)       9.2992 (1.33)      0.2957 (1.21)         37;0  107.3873 (0.75)        102           1
test_benchmark_query8     169.3261 (25.90)    189.0392 (25.04)    173.0360 (24.65)    7.8601 (43.04)    169.8045 (24.32)     1.3944 (5.70)          1;1    5.7791 (0.04)          6           1
test_benchmark_query9     146.1878 (22.36)    153.4628 (20.33)    150.5118 (21.44)    2.3057 (12.62)    151.1071 (21.65)     2.1810 (8.91)          2;1    6.6440 (0.05)          7           1
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================== 9 passed in 10.78s ===============================
```