# Ladybug graph

This section describes how to build and query a graph of the social network data in Ladybug. It uses Ladybug's [client API](https://github.com/kuzudb/ladybug) to perform the ingestion and querying.

> [!NOTE]
> All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

## Setup

Because Ladybug is an embedded graph database, the database is tightly coupled with the application layer -- there is no server to set up and run. Simply install the Ladybug Python library (`uv add ladybug`) and you're good to go!

## Build graph

The script `build_graph.py` contains the necessary methods to connect to the Ladybug DB and ingest the data from the CSV files, in batches for large amounts of data.

```sh
uv run build_graph.py --batch_size 50000
```

## Visualize graph

The provided `docker-compose.yml` allows you to run [Ladybug Explorer](https://github.com/ladybugdb/explorer), an open source visualization
tool for Ladybug. To run the Ladybug Explorer, install Docker and run the following command:

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
Successfully loaded nodes and edges into Ladybug
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
│ Seattle     ┆ 39.65897   │
│ Dallas      ┆ 39.72201   │
│ Austin      ┆ 39.772832  │
│ Kansas City ┆ 39.774014  │
│ Miami       ┆ 39.775695  │
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
│ United States  ┆ 30713        │
│ Canada         ┆ 2989         │
│ United Kingdom ┆ 1843         │
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
│ 67         ┆ Charlotte   ┆ United States  │
│ 67         ┆ Kansas City ┆ United States  │
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
│ 130        ┆ California ┆ United States │
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
│ 45343391 │
└──────────┘
        
Queries completed in 0.7173s
```

## Query performance

```sh
$ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
============================== test session starts ===============================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                                

benchmark_query.py .........                                               [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1     132.6206 (21.67)    137.1441 (19.16)    134.7551 (20.83)    2.1891 (11.22)    134.0781 (20.85)    4.1973 (17.74)         2;0    7.4209 (0.05)          5           1
test_benchmark_query2     210.8141 (34.45)    217.2234 (30.35)    215.4862 (33.30)    2.6334 (13.50)    216.4809 (33.66)    1.7537 (7.41)          1;1    4.6407 (0.03)          5           1
test_benchmark_query3       6.1202 (1.0)        7.1569 (1.0)        6.4705 (1.0)      0.2145 (1.10)       6.4309 (1.0)      0.2924 (1.24)         30;1  154.5479 (1.0)          91           1
test_benchmark_query4       9.2897 (1.52)      10.3100 (1.44)       9.7589 (1.51)     0.2594 (1.33)       9.7479 (1.52)     0.3608 (1.53)         28;0  102.4706 (0.66)         81           1
test_benchmark_query5      10.3990 (1.70)      11.2540 (1.57)      10.7115 (1.66)     0.1951 (1.0)       10.6800 (1.66)     0.2504 (1.06)         22;1   93.3580 (0.60)         73           1
test_benchmark_query6      26.3234 (4.30)      28.1802 (3.94)      27.1495 (4.20)     0.3699 (1.90)      27.1552 (4.22)     0.3121 (1.32)         10;3   36.8331 (0.24)         35           1
test_benchmark_query7      11.0310 (1.80)      12.3012 (1.72)      11.4979 (1.78)     0.3254 (1.67)      11.4121 (1.77)     0.4327 (1.83)         18;0   86.9725 (0.56)         68           1
test_benchmark_query8       6.2190 (1.02)      12.1982 (1.70)       6.6075 (1.02)     0.5740 (2.94)       6.5393 (1.02)     0.2366 (1.0)           3;3  151.3435 (0.98)        133           1
test_benchmark_query9      86.5393 (14.14)     89.9211 (12.56)     87.6867 (13.55)    1.1659 (5.98)      87.2312 (13.56)    1.0531 (4.45)          2;2   11.4042 (0.07)         11           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
=============================== 9 passed in 9.93s ================================
```
