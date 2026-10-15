# Neo4j graph

This section describes how to build and query a graph of the social network data in Neo4j. It uses the official `neo4j` [Python client](https://github.com/neo4j/neo4j-python-driver) to perform the ingestion and querying.

## Run Neo4j in a Docker container

Because Neo4j uses a client-server architecture, for development purposes, it makes sense to use Docker to orchestrate the setup and teardown of the DB. This is done easily via `docker-compose` as follows.

### Create a `.env` file with DB credentials

The necessary authentication to the database is specified via the variables in `.env.example`. Copy this example file, rename it to `.env` and update the `NEO4J_PASSWORD` field with the desired DB password.

Then, run the Docker container in detached mode as follows.

```sh
docker compose up -d
```

Once development and querying are finished, the container can be stopped as follows.

```sh
docker compose down
```

## Build graph

> [!NOTE]
> All timing numbers shown below are on an M5 MacBook Pro (10-core CPU: 4 performance + 6 efficiency cores) with 24 GB of RAM, running macOS 26.6.2.

The script `build_graph.py` contains the necessary methods to connect to the Neo4j DB and ingest the data from the CSV files, in batches for large amounts of data.

```sh
python build_graph.py
```

## Visualize graph

You can visualize the graph in the Neo4j browser by a) downloading the Neo4j Desktop tool, or b) in the browser via `http://localhost:7474`.

## Ingestion performance

The numbers shown below are for when we ingest 100K person nodes, ~10K location nodes and ~2.
4M edges into the graph. Note the following points:

- **The goal is to perform the entire task in Python**, so we don't want to use other means like `apoc` ot `LOAD CSV` to ingest the data (which may be faster, but would require additional glue code, which defeats the purpose of this exercise)
- The [async API](https://neo4j.com/docs/api/python-driver/current/async_api.html) of the Neo4j Python client is used, which is observed on this dataset to perform ~40% faster than the sync API
- The person nodes and person-person follower edges are **ingested in batches**, which is part of the [best practices](https://neo4j.com/docs/python-manual/current/performance/) when passing data to Neo4j via Python -- this is because the number of persons and followers can get very large, causing the number of edges to nonlinearly increase with the size of the dataset.
- The batch size is set to 500K, which may seem large at first glance, but for the given data, the nodes and edges, even after `UNWIND`ing in Cypher, are small enough to fit in batch memory per transaction -- the memory requirements may be different on more complex datasets

```sh
# Graph has 100K nodes and ~2.4M edges
$ uv run build_graph.py -b 500000

Nodes loaded in 3.6s
Edges loaded in 44.4s
```

As expected, the nodes load much faster than the edges, since there are many more edges than nodes. In addition, the nodes in Neo4j are indexed (via uniqueness constraints), following which the edges are created based on a match on existing nodes, allowing us to achieve this performance.

## Query graph

The script `query.py` contains a suite of queries that can be run to benchmark various aspects of the DB's performance.

```sh
python query.py
```

#### Output

```
Query 1:
 
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.personID AS personID, person.name AS name, count(follower) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3
    
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
 
        MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
        WITH person, count(follower) as followers
        ORDER BY followers DESC LIMIT 1
        MATCH (person) -[:LIVES_IN]-> (city:City)
        RETURN person.name AS name, followers AS numFollowers, city.city AS city, city.state AS state, city.country AS country
    
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
 
        MATCH (p:Person) -[:LIVES_IN]-> (c:City) -[*1..2]-> (co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageAge
        ORDER BY averageAge LIMIT 5
    
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
 
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[*1..2]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personCounts
        ORDER BY personCounts DESC LIMIT 3
    
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
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        WITH p, i
        MATCH (p)-[:LIVES_IN]->(c:City)
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
 
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        WITH p, i
        MATCH (p)-[:LIVES_IN]->(c:City)
        RETURN count(p) AS numPersons, c.city AS city, c.country AS country
        ORDER BY numPersons DESC LIMIT 5
    
Cities with the most female users who have an interest in tennis:
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
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        WITH p, s
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        RETURN count(p) AS numPersons, s.state AS state, s.country AS country
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
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
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
 
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
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
        
Neo4j query script completed in 10.471270s
```

### Query performance benchmark

The benchmark is run using `pytest-benchmark` package as follows.

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


--------------------------------------------------------------------------------- benchmark: 9 tests ---------------------------------------------------------------------------------
Name (time in s)             Min               Max              Mean            StdDev            Median               IQR            Outliers       OPS            Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1     1.8341 (260.99)   1.9861 (172.92)   1.9191 (234.23)   0.0765 (108.01)   1.9534 (241.01)   0.1460 (187.78)        2;0    0.5211 (0.00)          5           1
test_benchmark_query2     0.4701 (66.90)    0.5382 (46.86)    0.4961 (60.55)    0.0293 (41.39)    0.4876 (60.16)    0.0486 (62.50)         1;0    2.0156 (0.02)          5           1
test_benchmark_query3     0.0363 (5.17)     0.0534 (4.64)     0.0402 (4.91)     0.0051 (7.23)     0.0379 (4.67)     0.0044 (5.65)          4;4   24.8629 (0.20)         24           1
test_benchmark_query4     0.0370 (5.26)     0.0603 (5.25)     0.0458 (5.59)     0.0093 (13.20)    0.0409 (5.04)     0.0189 (24.28)         6;0   21.8340 (0.18)         16           1
test_benchmark_query5     0.0070 (1.0)      0.0115 (1.0)      0.0082 (1.0)      0.0007 (1.0)      0.0081 (1.0)      0.0009 (1.13)         24;2  122.0529 (1.0)          98           1
test_benchmark_query6     0.0185 (2.63)     0.0263 (2.29)     0.0198 (2.41)     0.0014 (1.92)     0.0194 (2.40)     0.0008 (1.0)           3;3   50.6057 (0.41)         48           1
test_benchmark_query7     0.1007 (14.33)    0.1231 (10.72)    0.1065 (13.00)    0.0069 (9.78)     0.1039 (12.81)    0.0057 (7.36)          1;1    9.3874 (0.08)         10           1
test_benchmark_query8     3.2891 (468.03)   3.5777 (311.50)   3.4075 (415.89)   0.1143 (161.40)   3.3658 (415.27)   0.1638 (210.62)        2;0    0.2935 (0.00)          5           1
test_benchmark_query9     4.5452 (646.78)   6.0814 (529.48)   4.9030 (598.42)   0.6664 (941.12)   4.5601 (562.62)   0.5617 (722.26)        1;1    0.2040 (0.00)          5           1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
========================= 9 passed in 80.15s (0:01:20) =========================
```