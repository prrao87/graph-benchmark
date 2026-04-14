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
│ Seattle     ┆ 39.65897   │
│ Dallas      ┆ 39.72201   │
│ Austin      ┆ 39.772832  │
│ Kansas City ┆ 39.774014  │
│ Miami       ┆ 39.775695  │
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
│ United States  ┆ 30713        │
│ Canada         ┆ 2989         │
│ United Kingdom ┆ 1843         │
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
│ 67         ┆ Kansas City ┆ United States  │
│ 67         ┆ Charlotte   ┆ United States  │
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
│ 130        ┆ California ┆ United States │
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
│ 45343391 │
└──────────┘
Queries completed in 0.3451s
```

## Query performance

```sh
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
===================================== test session starts =====================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                                             

benchmark_query.py .........                                                            [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1      19.5093 (4.65)      27.9158 (2.48)      21.5448 (4.52)     1.9446 (2.78)      20.9937 (4.59)     0.8922 (2.73)          4;4   46.4149 (0.22)         33           1
test_benchmark_query2      39.2101 (9.35)      59.7954 (5.32)      44.3800 (9.31)     4.8681 (6.97)      43.8797 (9.59)     6.7535 (20.67)         3;1   22.5327 (0.11)         20           1
test_benchmark_query3       6.2803 (1.50)      15.9057 (1.42)       6.9717 (1.46)     1.3594 (1.95)       6.6377 (1.45)     0.3909 (1.20)          7;8  143.4367 (0.68)        127           1
test_benchmark_query4       4.4251 (1.05)      12.8335 (1.14)       4.9221 (1.03)     0.8815 (1.26)       4.7288 (1.03)     0.3268 (1.0)           7;7  203.1656 (0.97)        205           1
test_benchmark_query5       4.1957 (1.0)       12.1846 (1.08)       4.7647 (1.0)      0.9034 (1.29)       4.5744 (1.0)      0.3350 (1.03)         9;11  209.8755 (1.0)         213           1
test_benchmark_query6       4.7522 (1.13)      11.2365 (1.0)        5.4362 (1.14)     0.6988 (1.0)        5.3026 (1.16)     0.4323 (1.32)          8;6  183.9508 (0.88)        171           1
test_benchmark_query7       5.9705 (1.42)      12.3711 (1.10)       6.6792 (1.40)     1.1412 (1.63)       6.3740 (1.39)     0.3711 (1.14)         9;10  149.7188 (0.71)        155           1
test_benchmark_query8     122.2570 (29.14)    133.6635 (11.90)    126.2321 (26.49)    3.7176 (5.32)     125.4626 (27.43)    3.3949 (10.39)         2;1    7.9219 (0.04)          7           1
test_benchmark_query9     109.8777 (26.19)    117.4411 (10.45)    111.6710 (23.44)    2.5000 (3.58)     110.8712 (24.24)    1.9847 (6.07)          1;1    8.9549 (0.04)          8           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
===================================== 9 passed in 10.54s ======================================
```
