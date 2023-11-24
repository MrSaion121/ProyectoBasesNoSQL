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
        from_ TEXT,
        to TEXT,
        day INT,
        month INT,
        year INT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY ((year), wait, from_)
    ) WITH CLUSTERING ORDER BY (wait DESC, from_ ASC)
"""

CREATE_FLIGHTS_BY_MONTH_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_by_month (
        airline TEXT,
        from_ TEXT,
        to TEXT,
        day INT,
        month INT,
        year INT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY ((year), wait, month, to)
    ) WITH CLUSTERING ORDER BY (wait ASC, month ASC, to ASC)
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
    SELECT airline, from_, to, day, month, year, connection, wait
    FROM flights_by_year
    WHERE year > ? AND year < ? AND wait > 0
"""

SELECT_FLIGHTS_MONTH = """
    SELECT airline, from_, to, day, month, year, connection, wait
    FROM flights_by_month
    WHERE year = ? AND wait = 0
"""

SELECT_CLIENTS = """
    SELECT age, gender, reason, stay, transit
    FROM client
    WHERE age > 18
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
    fy_stmt = session.prepare(SELECT_FLIGHTS_YEAR)
    rows = session.execute(fy_stmt, [yearMin, yearMax])
    data = {}
    print("Raw Data: \n")
    for row in rows:
        print(f"=== Airline: {row.airline} ===")
        print(f"- Location: {row.from_}")
        print(f"- Wait: {row.wait}")
        if row.from_ not in data:
            data[row.from_] = Wait()
        data[row.from_].count += 1
        data[row.from_].wait += row.wait
    print("\nAirports with the longest average wait time:")
    sortedData = sorted(data.items(), key=lambda x: x[1].wait/x[1].count, reverse=True)
    for i, (location, info) in enumerate(sortedData[:3]):
        avg_wait = info.wait/info.count
        print(f"=== Number: {i + 1} ===")
        print(f"- Location: {location} -")
        print(f"- Average Wait Time: {avg_wait} -")

class Airport:
    def __init__(self):
        self.months = {}
    
    def add_month(self, month):
        if month not in self.months:
            self.months[month] = 1
        else:
            self.months[month] += 1
    
    def get_months(self):
        return self.months


class Transit:
    def __init__(self):
        self.count = 0

def get_flights_by_month(session, year):
    log.info(f"Retrieving flights from the year {year} ")
    fm_stmt = session.prepare(SELECT_FLIGHTS_MONTH)
    rows = session.execute(fm_stmt, [year])
    data = {}
    print("Raw Data: \n")
    for row in rows:
        print(f"=== Airline: {row.airline} ===")
        print(f"- Location: {row.to}")
        print(f"- Month: {row.month}")
        if row.to not in data:
            data[row.to] = Airport()
        data[row.to].add_month(row.month)
    clients = session.execute(SELECT_CLIENTS)
    transits = {}
    for client in clients:
        if client.transit not in transits:
            transits[client.transit] = Transit()
        transits[client.transit].count += 1
    print("\nMonths where airports receive the most flights:")
    sortedData = sorted(data.months.items(), key=lambda x: x[1], reverse=True)
    for location, airport in sortedData:
        month, count = next(iter(airport.months.items()), (None, None))
        print(f"=== Airport: {location} ===")
        print(f"- Month: {month} -")
        print(f"- Count: {count} -")

    print("\nMost common transit methods:")
    sortedTransits = sorted(transits.items(), key=lambda x: x[1].count, reverse=True)
    for i, (transit, info) in enumerate(sortedTransits[:3]):
        print(f"=== Number: {i + 1} ===")
        print(f"- Transit: {transit} -")
        print(f"- Count: {info.count} -")


    