# Kùzu graph

This section describes how to build and query a graph of the social network data in Kuzu. It uses Kùzu's [client API](https://github.com/kuzudb/kuzu) to perform the ingestion and querying.

> [!NOTE]
> All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

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
Nodes loaded in 0.2s
Edges loaded in 0.4s
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
│ Seattle     ┆ 39.655417  │
│ Dallas      ┆ 39.720371  │
│ Austin      ┆ 39.771676  │
│ Kansas City ┆ 39.771793  │
│ Miami       ┆ 39.773881  │
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
│ United States  ┆ 30714        │
│ Canada         ┆ 2986         │
│ United Kingdom ┆ 1842         │
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
│ 45344112 │
└──────────┘
```

## Query performance

```sh
$ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
==================================== test session starts =====================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                                            

benchmark_query.py .........                                                           [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1     134.2170 (22.09)    150.3787 (20.73)    140.2733 (21.84)    6.1561 (34.46)    139.6149 (21.87)    6.7672 (34.75)         1;0    7.1289 (0.05)          5           1
test_benchmark_query2     209.8766 (34.54)    215.2829 (29.67)    212.8170 (33.13)    1.9515 (10.93)    212.9799 (33.36)    2.0487 (10.52)         2;0    4.6989 (0.03)          5           1
test_benchmark_query3       6.1705 (1.02)       7.2551 (1.0)        6.4233 (1.0)      0.1786 (1.0)        6.3836 (1.0)      0.2194 (1.13)         19;3  155.6834 (1.0)          90           1
test_benchmark_query4       9.3286 (1.54)      10.2077 (1.41)       9.7014 (1.51)     0.2136 (1.20)       9.6634 (1.51)     0.2813 (1.44)         27;0  103.0781 (0.66)         82           1
test_benchmark_query5      10.3470 (1.70)      22.2005 (3.06)      11.0527 (1.72)     1.3787 (7.72)      10.8359 (1.70)     0.4497 (2.31)          1;1   90.4757 (0.58)         71           1
test_benchmark_query6      25.7989 (4.25)      27.7034 (3.82)      26.7576 (4.17)     0.3603 (2.02)      26.7365 (4.19)     0.4611 (2.37)          7;2   37.3726 (0.24)         35           1
test_benchmark_query7      10.7259 (1.77)      11.7925 (1.63)      11.0627 (1.72)     0.1957 (1.10)      11.0394 (1.73)     0.1947 (1.0)          17;4   90.3935 (0.58)         76           1
test_benchmark_query8       6.0766 (1.0)        7.4262 (1.02)       6.4241 (1.00)     0.2028 (1.14)       6.4043 (1.00)     0.2545 (1.31)         32;4  155.6633 (1.00)        134           1
test_benchmark_query9      84.4289 (13.89)     87.0802 (12.00)     85.8985 (13.37)    0.7950 (4.45)      85.8460 (13.45)    1.2107 (6.22)          3;0   11.6416 (0.07)         12           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
===================================== 9 passed in 10.01s =====================================
```
