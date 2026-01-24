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
================================================= test session starts ==================================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 9 items                                                                                                      

benchmark_query.py .........                                                                                     [100%]


------------------------------------------------------------------------------------- benchmark: 9 tests -------------------------------------------------------------------------------------
Name (time in ms)              Min                 Max                Mean            StdDev              Median               IQR            Outliers       OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1     139.4702 (22.84)    144.6998 (20.62)    142.1490 (21.71)    2.0914 (12.50)    142.2837 (21.78)    3.3739 (15.51)         2;0    7.0349 (0.05)          5           1
test_benchmark_query2     213.0444 (34.89)    221.3121 (31.54)    216.7693 (33.10)    3.2853 (19.64)    215.3557 (32.96)    4.7682 (21.92)         2;0    4.6132 (0.03)          5           1
test_benchmark_query3       6.1055 (1.0)       17.5115 (2.50)       6.8731 (1.05)     1.2539 (7.50)       6.6400 (1.02)     0.3293 (1.51)          3;4  145.4946 (0.95)         98           1
test_benchmark_query4       9.3057 (1.52)      10.7007 (1.52)       9.7992 (1.50)     0.2356 (1.41)       9.7715 (1.50)     0.2932 (1.35)         19;2  102.0491 (0.67)         77           1
test_benchmark_query5      10.4345 (1.71)      11.8668 (1.69)      10.9959 (1.68)     0.3246 (1.94)      10.9722 (1.68)     0.4415 (2.03)         18;1   90.9429 (0.60)         63           1
test_benchmark_query6      26.8415 (4.40)      28.9885 (4.13)      27.6479 (4.22)     0.5568 (3.33)      27.4565 (4.20)     0.7251 (3.33)          9;0   36.1691 (0.24)         34           1
test_benchmark_query7      10.9891 (1.80)      21.1236 (3.01)      11.6543 (1.78)     1.3354 (7.98)      11.4510 (1.75)     0.3378 (1.55)          2;2   85.8049 (0.56)         68           1
test_benchmark_query8       6.2053 (1.02)       7.0173 (1.0)        6.5490 (1.0)      0.1673 (1.0)        6.5340 (1.0)      0.2175 (1.0)          50;1  152.6960 (1.0)         142           1
test_benchmark_query9      85.1712 (13.95)     89.4144 (12.74)     86.5448 (13.22)    1.3750 (8.22)      85.8915 (13.15)    2.0326 (9.34)          2;0   11.5547 (0.08)         11           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
================================================== 9 passed in 10.00s ==================================================
```
