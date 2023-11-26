#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('flights.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'flights')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Get the airports with the longest average wait time in a five year range",
        2: "Get the months with the most flights in a year",
        3: "Get the count of times a client gives a certain stay in a given age range",
        4: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            yearMax = int(input('Enter the year limit you wish to check: '))
            model.get_flights_by_year(session, yearMax)
        if option == 2:
            year = int(input('Enter the year you wish to check: '))
            model.get_flights_by_month(session, year)
        if option == 3:
            stay = input('Enter the stay you want to check: ')
            ageMin = int(input('Enter the minimum age of the clients you wish to check: '))
            ageMax = int(input('Enter the maxium age of the clients you wish to check: '))
            model.get_clients_by_stay(session, stay, ageMin, ageMax)
            pass
        if option == 4:
            exit(0)


if __name__ == '__main__':
    main()