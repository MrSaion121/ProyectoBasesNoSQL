#!/usr/bin/env python

"""
Generador de datos para proyecto de Bases de Datos No Relacionales
ITESO 
"""
import argparse
import csv
import datetime

from random import choice, randint, randrange


airlines = ["American Airlines", "Delta Airlines", "Alaska", "Aeromexico", "Volaris"]
airports = ["PDX", "GDL", "SJC", "LAX", "JFK"]
genders = ["male", "female", "unspecified", "undisclosed"]
reasons = ["On vacation/Pleasure", "Business/Work", "Back Home"]
stays = ["Hotel", "Short-term homestay", "Home", "Friend/Family"]
transits = ["Airport cab", "Car rental", "Mobility as a service", "Public Transportation", "Pickup", "Own car"]
connections = [True, False]


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return rand_date


def generate_dataset_cql(output_file, rows):
    fy_stmt = "INSERT INTO flights_by_year (airline, from_, to_, day, month, year, connection, wait) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {});"
    fm_stmt = "INSERT INTO flights_by_month (airline, from_, to_, day, month, year, connection, wait) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {});"
    cl_stmt = "INSERT INTO client (age, gender, reason, stay, transit) VALUES({}, '{}', '{}', '{}', '{}');"
    with open(output_file, "w") as fd:
        for i in range(rows):
            from_airport = choice(airports)
            to_airport = choice(airports)
            while from_airport == to_airport:
                to_airport = choice(airports)
            date = random_date(datetime.datetime(2013, 1, 1), datetime.datetime(2023, 4, 25))
            reason = choice(reasons)
            stay = choice(stays)
            connection = choice(connections)
            wait = randint(30, 720)
            transit = choice(transits)
            if not connection:
               wait = 0
            else:
                transit = "None"
            if reason == "Back Home":
                stay = "Home"
                connection = False
                wait = 0
                transit = choice(transits)
            
            airline = choice(airlines)
            age = randint(1,90)
            gender = choice(genders)

            fd.write(fy_stmt.format(airline, from_airport, to_airport, date.day, date.month, date.year, connection, wait))
            fd.write('\n')
            fd.write(fm_stmt.format(airline, from_airport, to_airport, date.day, date.month, date.year, connection, wait))
            fd.write('\n')
            fd.write(cl_stmt.format(age, gender, reason, stay, transit))
            fd.write('\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--output",
            help="Specify the output filename of your csv, defaults to: flight_passengers.csv", default="flight_passengers.csv")
    parser.add_argument("-r", "--rows",
            help="Amount of random generated entries for the dataset, defaults to: 100", type=int, default=100)
    parser.add_argument("-db", "--database",
            help="Choose the data base to generate the data of", default="cassandra")

    args = parser.parse_args()
    print(f"Generating {args.rows} for flight passenger dataset")
    if args.database == "cassandra" and args.output == "flight_passengers.cql":
        generate_dataset_cql(args.output, args.rows)
    
    print(f"Completed generating dataset in {args.output}")


