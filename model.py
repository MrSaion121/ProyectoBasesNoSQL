#!/usr/bin/env python3
import logging

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_FLIGHTS_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_by_year (
        airline TEXT,
        from TEXT,
        to text,
        day TEXT,
        month TEXT,
        year TEXT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY ((year), wait, to)
    ) WITH CLUSTERING ORDER BY (wait DESC, to ASC)
"""

CREATE_FLIGHTS_BY_MONTH_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_by_month (
        airline TEXT,
        from TEXT,
        to text,
        day TEXT,
        month TEXT,
        year TEXT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY ((year), wait, month, from)
    ) WITH CLUSTERING ORDER BY (wait ASC, month ASC, from ASC)
"""

CREATE_CLIENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS client (
        age INT,
        gender TEXT,
        reason TEXT,
        stay TEXT,
        transit TEXT,
        PRIMARY KEY ((age), transit)
    ) WITH CLUSTERING ORDER BY (transit DESC)
"""


SELECT_FLIGHTS_YEAR = """
    SELECT airline, from, to, day, month, year, connection, wait
    FROM flights_by_year
    WHERE year > ? AND year < ? AND wait > 0
"""

SELECT_FLIGHTS_MONTH = """
    SELECT airline, from, to, day, month, year, connection, wait
    FROM flights_by_month
    WHERE year > ? AND year < ? AND wait = 0
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHTS_TABLE)
    session.execute(CREATE_CLIENTS_TABLE)
    session.execute(CREATE_FLIGHTS_BY_MONTH_TABLE)

class Wait:
    def __init__(self):
        self.count = 0
        self.wait = 0

def get_flights_by_year(session, yearMax):
    yearMin = yearMax - 5
    log.info(f"Retrieving flights from {yearMin} to {yearMax} ")
    stmt = session.prepare(SELECT_FLIGHTS_YEAR)
    rows = session.execute(stmt, [yearMin, yearMax])
    data = {}
    print("Raw Data: \n")
    for row in rows:
        print(f"=== Airline: {row.airline} ===")
        print(f"- Location: {row.to}")
        print(f"- Wait: {row.wait}")
        if row.to not in data:
            data[row.to] = Wait()
        data[row.to].count += 1
        data[row.to].wait += row.wait
    print("\nAirports with the longest average wait time:")
    sortedData = sorted(data.items(), key=lambda x: x[1].wait/x[1].count, reverse=True)
    for i, (location, info) in enumerate(sortedData[:3]):
        avg_wait = info.wait/info.count
        print(f"=== Number: {i + 1} ===")
        print(f"- Location: {location} -")
        print(f"- Average Wait Time: {avg_wait} -")


    