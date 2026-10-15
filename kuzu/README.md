# Kùzu graph

This section describes how to build and query a graph of the social network data in Kuzu. It uses Kùzu's [client API](https://github.com/kuzudb/kuzu) to perform the ingestion and querying.

> [!NOTE]
> All timing numbers shown below are on an M5 MacBook Pro (10-core CPU: 4 performance + 6 efficiency cores) with 24 GB of RAM, running macOS 26.6.2.

## Setup

Because Kùzu is an embedded graph database, the database is tightly coupled with the application layer -- there is no server to set up and run. Simply install the Kuzu Python library (`uv add kuzu`) and you're good to go!

> [!NOTE]
> The Kuzu project has officially been archived, and it's now succeeded by a fork, [Ladybug](https://github.com/LadybugDB/ladybug).

## Build graph

The script `build_graph.py` contains the necessary methods to connect to the Kuzu DB and ingest the data from the CSV files, in batches for large amounts of data.

```sh
uv run build_graph.py --batch_size 50000
```

## Visualize graph

The provided `docker-compose.yml` allows you to run [Kùzu Explorer](https://github.com/kuzudb/explorer), an open source visualization
tool for Kuzu. To run the Kùzu Explorer, install Docker and run the following command:

```sh
docker compose up
```

This allows you to access to visualize the graph on the browser at `http://localhost:8000`.

## Ingestion performance

The numbers shown below are for when we ingest 100K person nodes, ~10K location nodes and ~2.4M edges into the graph.

As expected, the nodes load much faster than the edges, since there are many more edges than nodes. The run times for ingesting nodes and edges are output to the console.

```bash
# Graph has 100K nodes and ~2.4M edges
$ uv run build_graph.py
Nodes loaded in 0.3s
Edges loaded in 0.5s
Successfully loaded nodes and edges into Kuzu
```

## Query graph

The script `query.py` contains a suite of queries that can be run to benchmark various aspects of the DB's performance.

```sh
python query.py
```

### Results
```
Query 1:
 
        MATCH (follower:Person)-[:Follows]->(person:Person)
        RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
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
 
        MATCH (follower:Person)-[:Follows]->(person:Person)
        WITH person, count(follower.id) as numFollowers
        ORDER BY numFollowers DESC LIMIT 1
        MATCH (person) -[:LivesIn]-> (city:City)
        RETURN person.name AS name, numFollowers, city.city AS city, city.state AS state, city.country AS country;
    
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
 
        MATCH (p:Person) -[:LivesIn]-> (c:City) -[*1..2]-> (co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageAge
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
 
        MATCH (p:Person)-[:LivesIn]->(ci:City)-[*1..2]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personCounts
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
 
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        WITH p, i
        MATCH (p)-[:LivesIn]->(c:City)
        WHERE c.city = $city AND c.country = $country
        RETURN count(p) AS numPersons
    
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
 
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        WITH p, i
        MATCH (p)-[:LivesIn]->(c:City)
        RETURN count(p.id) AS numPersons, c.city AS city, c.country AS country
        ORDER BY numPersons DESC LIMIT 5
    
City with the most female users who have an interest in tennis:
shape: (5, 3)
┌────────────┬─────────────┬────────────────┐
│ numPersons ┆ city        ┆ country        │
│ ---        ┆ ---         ┆ ---            │
│ i64        ┆ str         ┆ str            │
╞════════════╪═════════════╪════════════════╡
│ 77         ┆ Birmingham  ┆ United Kingdom │
│ 67         ┆ Kansas City ┆ United States  │
│ 67         ┆ Charlotte   ┆ United States  │
│ 65         ┆ Montreal    ┆ Canada         │
│ 65         ┆ Portland    ┆ United States  │
└────────────┴─────────────┴────────────────┘

Query 7:
 
        MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        WITH p, s
        MATCH (p)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
        ORDER BY numPersons DESC LIMIT 1
    

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
 
        MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
        RETURN count(*) AS numPaths
    

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
 
        MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numPaths
    

        Number of paths through persons below 50 to persons above 25:
shape: (1, 1)
┌──────────┐
│ numPaths │
│ ---      │
│ i64      │
╞══════════╡
│ 45362926 │
└──────────┘
        
Queries completed in 1.0597s
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


-------------------------------------------------------------------------------------- benchmark: 9 tests --------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean             StdDev              Median                IQR            Outliers       OPS            Rounds  Iterations
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1     131.9140 (24.15)    161.9295 (25.97)    145.7476 (25.98)    10.7732 (102.35)   145.4369 (26.02)    10.6604 (103.23)        2;0    6.8612 (0.04)          5           1
test_benchmark_query2     204.4252 (37.43)    260.7197 (41.81)    228.8301 (40.79)    27.8654 (264.74)   215.6950 (38.58)    52.3880 (507.29)        2;0    4.3701 (0.02)          5           1
test_benchmark_query3       5.6705 (1.04)       6.5975 (1.06)       5.9783 (1.07)      0.1484 (1.41)       5.9653 (1.07)      0.1721 (1.67)         30;1  167.2705 (0.94)         99           1
test_benchmark_query4       8.3214 (1.52)       8.9987 (1.44)       8.6199 (1.54)      0.1452 (1.38)       8.6060 (1.54)      0.2139 (2.07)         30;0  116.0104 (0.65)         88           1
test_benchmark_query5       9.5784 (1.75)      10.4193 (1.67)       9.8395 (1.75)      0.1247 (1.18)       9.8161 (1.76)      0.1460 (1.41)         18;3  101.6307 (0.57)         77           1
test_benchmark_query6      25.2552 (4.62)      28.9555 (4.64)      25.9429 (4.62)      0.7389 (7.02)      25.7499 (4.61)      0.5821 (5.64)          4;3   38.5462 (0.22)         36           1
test_benchmark_query7       9.7063 (1.78)      13.0538 (2.09)      10.2294 (1.82)      0.6877 (6.53)       9.9751 (1.78)      0.3350 (3.24)          9;9   97.7577 (0.55)         82           1
test_benchmark_query8       5.4614 (1.0)        6.2363 (1.0)        5.6104 (1.0)       0.1053 (1.0)        5.5903 (1.0)       0.1033 (1.0)          22;5  178.2413 (1.0)         143           1
test_benchmark_query9      77.7965 (14.24)     97.8622 (15.69)     87.8314 (15.66)     7.7831 (73.95)     88.6432 (15.86)    12.3961 (120.04)        4;0   11.3854 (0.06)         10           1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================== 9 passed in 10.29s ==============================
```
