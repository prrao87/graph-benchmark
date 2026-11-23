"""
Run a series of queries on the Relate database (embedded, in-process)
"""
import time
from pathlib import Path

import polars as pl
import relate
from relate import Connection

DB_PATH = Path(__file__).resolve().parent / "social_network.rl"


def run_query1(conn: Connection) -> None:
    "Who are the top 3 most-followed persons in the network?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.personID AS personID, person.name AS name, count(follower) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3
    """
    print(f"\nQuery 1:\n {query}")
    response = conn.execute(query)
    result = pl.from_dicts(response.fetchall())
    print(f"Top 3 most-followed persons:\n{result}")
    return result


def run_query2(conn: Connection) -> None:
    "In which city does the most-followed person in the network live?"
    query = """
        MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
        WITH person, count(follower) as followers
        ORDER BY followers DESC LIMIT 1
        MATCH (person) -[:LIVES_IN]-> (city:City)
        RETURN person.name AS name, followers AS numFollowers, city.city AS city, city.state AS state, city.country AS country
    """
    print(f"\nQuery 2:\n {query}")
    response = conn.execute(query)
    result = pl.from_dicts(response.fetchall())
    print(f"City in which most-followed person lives:\n{result}")
    return result


def run_query3(conn: Connection, country: str) -> None:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        MATCH (p:Person) -[:LIVES_IN]-> (c:City) -[*1..2]-> (co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageAge
        ORDER BY averageAge LIMIT 5
    """
    print(f"\nQuery 3:\n {query}")
    response = conn.execute(query, {"country": country})
    result = pl.from_dicts(response.fetchall())
    print(f"Cities with lowest average age in {country}:\n{result}")
    return result


def run_query4(conn: Connection, age_lower: int, age_upper: int) -> None:
    "How many persons between a certain age range are in each country?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[*1..2]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personCounts
        ORDER BY personCounts DESC LIMIT 3
    """
    print(f"\nQuery 4:\n {query}")
    response = conn.execute(query, {"age_lower": age_lower, "age_upper": age_upper})
    result = pl.from_dicts(response.fetchall())
    print(f"Persons between ages {age_lower}-{age_upper} in each country:\n{result}")
    return result


def run_query5(conn: Connection, gender: str, city: str, country: str, interest: str) -> None:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        WITH p, i
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE c.city = $city AND c.country = $country
        RETURN count(p) AS numPersons
    """
    print(f"\nQuery 5:\n {query}")
    response = conn.execute(query, {"gender": gender, "city": city, "country": country, "interest": interest})
    result = pl.from_dicts(response.fetchall())
    print(
        f"Number of {gender} users in {city}, {country} who have an interest in {interest}:\n{result}"
    )
    return result


def run_query6(conn: Connection, gender: str, interest: str) -> None:
    "Which city has the maximum number of people of a particular gender that share a particular interest"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        WITH p, i
        MATCH (p)-[:LIVES_IN]->(c:City)
        RETURN count(p) AS numPersons, c.city AS city, c.country AS country
        ORDER BY numPersons DESC LIMIT 5
    """
    print(f"\nQuery 6:\n {query}")
    response = conn.execute(query, {"gender": gender, "interest": interest})
    result = pl.from_dicts(response.fetchall())
    print(f"Cities with the most {gender} users who have an interest in {interest}:\n{result}")
    return result


def run_query7(
    conn: Connection, country: str, age_lower: int, age_upper: int, interest: str
) -> None:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        WITH p, s
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        RETURN count(p) AS numPersons, s.state AS state, s.country AS country
        ORDER BY numPersons DESC LIMIT 1
    """
    print(f"\nQuery 7:\n {query}")
    response = conn.execute(query, {"country": country, "age_lower": age_lower, "age_upper": age_upper, "interest": interest})
    result = pl.from_dicts(response.fetchall())
    print(
        f"""
        State in {country} with the most users between ages {age_lower}-{age_upper} who have an interest in {interest}:\n{result}
        """
    )
    return result


def run_query8(conn: Connection) -> None:
    "How many second-degree paths exist in the graph?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numPaths
    """

    print(f"\nQuery 8:\n {query}")
    response = conn.execute(query)
    result = pl.from_dicts(response.fetchall())
    print(
        f"""
        Number of second-degree paths:\n{result}
        """
    )
    return result


def run_query9(conn: Connection, age_1: int, age_2: int) -> None:
    "How many paths exist in the graph through persons below a certain age to persons above a certain age?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numPaths
    """

    print(f"\nQuery 9:\n {query}")
    response = conn.execute(query, {"age_1": age_1, "age_2": age_2})
    result = pl.from_dicts(response.fetchall())
    print(
        f"""
        Number of paths through persons below {age_1} to persons above {age_2}:\n{result}
        """
    )
    return result


def main() -> None:
    conn = relate.connect(str(DB_PATH), read_only=True)
    start = time.perf_counter()
    # fmt: off
    _ = run_query1(conn)
    _ = run_query2(conn)
    _ = run_query3(conn, country="United States")
    _ = run_query4(conn, age_lower=30, age_upper=40)
    _ = run_query5(conn, gender="male", city="London", country="United Kingdom", interest="fine dining")
    _ = run_query6(conn, gender="female", interest="tennis")
    _ = run_query7(conn, country="United States", age_lower=23, age_upper=30, interest="photography")
    _ = run_query8(conn)
    _ = run_query9(conn, age_1=50, age_2=25)
    # fmt: on
    elapsed = time.perf_counter() - start
    print(f"Relate query script completed in {elapsed:.6f}s")


if __name__ == "__main__":
    main()
