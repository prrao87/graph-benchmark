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
    
Persons between ages 30-40 in each country:
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
    
Number of male users in London, United Kingdom who have an interest in Fine Dining:
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
    
City with the most female users who have an interest in Tennis:
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
    

        State in United States with the most users between ages 23-30 who have an interest in Photography:
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
│ 45343391 │
└──────────┘
        
Queries completed in 0.4013s
```

## Query performance

```
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
===================================================== test session starts =====================================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                                                                             

benchmark_query.py .........                                                                                            [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1      17.8280 (7.11)      22.3267 (6.97)      19.2239 (7.09)     1.1378 (11.92)     18.8716 (7.01)     1.4191 (13.31)        10;2   52.0185 (0.14)         40           1
test_benchmark_query2      36.5063 (14.55)     49.6617 (15.50)     42.4511 (15.65)    3.0251 (31.70)     43.0239 (15.98)    4.1510 (38.92)         8;0   23.5565 (0.06)         23           1
test_benchmark_query3       4.3384 (1.73)       5.4002 (1.69)       4.7274 (1.74)     0.1784 (1.87)       4.6877 (1.74)     0.2119 (1.99)         59;5  211.5314 (0.57)        192           1
test_benchmark_query4       2.6735 (1.07)       3.2958 (1.03)       2.9193 (1.08)     0.1369 (1.43)       2.8810 (1.07)     0.1762 (1.65)        106;3  342.5430 (0.93)        306           1
test_benchmark_query5       2.5086 (1.0)        3.2041 (1.0)        2.7126 (1.0)      0.1083 (1.14)       2.6931 (1.0)      0.1213 (1.14)        90;12  368.6499 (1.0)         340           1
test_benchmark_query6       3.0972 (1.23)       3.6935 (1.15)       3.3093 (1.22)     0.1043 (1.09)       3.2954 (1.22)     0.1384 (1.30)         78;6  302.1804 (0.82)        288           1
test_benchmark_query7       4.1542 (1.66)       5.0323 (1.57)       4.3776 (1.61)     0.0954 (1.0)        4.3705 (1.62)     0.1067 (1.0)          53;7  228.4367 (0.62)        223           1
test_benchmark_query8     121.3912 (48.39)    126.2406 (39.40)    123.4097 (45.49)    1.6706 (17.51)    123.3598 (45.81)    2.8436 (26.66)         3;0    8.1031 (0.02)          9           1
test_benchmark_query9     109.2869 (43.56)    113.9959 (35.58)    110.7751 (40.84)    1.4477 (15.17)    110.6885 (41.10)    1.3790 (12.93)         2;1    9.0273 (0.02)          9           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
===================================================== 9 passed in 10.63s ======================================================
```