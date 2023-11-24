#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Get the airports with the longest average wait time in a five year range",
        2: "Get the months with the most flights in a year, as well as the most use transit method",
        3: "Show trade history",
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
            model.flights_by_year(session, yearMax)
        if option == 2:
            year = int(input('Enter the year you wish to check: '))
            model.flights_by_months(session, year)
        if option == 3:
            # print_trade_history_menu()
            # tv_option = int(input('Enter your trade view choice: '))
            pass
        if option == 4:
            exit(0)


if __name__ == '__main__':
    main()