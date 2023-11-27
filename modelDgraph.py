import datetime
import json
import pandas as pd 
import pydgraph
import ast


def set_schema(client):
    schema = """
    type passenger{
        
        age
        gender
        reason
        transit
        stay
    }

    type scale{


            location
            avg_wait 
    }
    
    type flight{

        airline
        flight_number
        Ffrom
        Fto
        date
        connection
    }

    type transport{

        name
        N_uses
    }
    
    age: int .    
    gender: string .
    reason: string .
    transit: string .
    stay: string .


    location: string .
    avg_wait: float .


    
    airline: string .
    flight_number: string .
    Ffrom: string .
    Fto: string .
    date: datetime .
    connection: string .


    name: string .
    N_uses: int .
    
    """
    return client.alter(pydgraph.Operation(schema=schema))


    
def data_parser(file:str):

    '''
        data parser, gets an txt and returns an array of dicts
    '''
    userfile = open(file,'r')
    line = userfile.readlines()
    line = line[0]

    ArrayOfData = ast.literal_eval(line)
  
    userfile.close()
    return(ArrayOfData)

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

def create_data(client):
  

    passengers = data_parser('passengersDgraph.txt')
    scales = data_parser('scalesDgraph.txt')
    transit = data_parser('transitDgraph.txt')
    flight = data_parser('flightDgraph.txt')

    finalData = flight + transit + scales + passengers
    txn = client.txn()
    try:
            p = finalData
            
            response = txn.mutate(set_obj=p)

            # Commit transaction.
            commit_response = txn.commit()
            print(f"Commit Response: {commit_response}")

            print(f"UIDs: {response.uids}")
    finally:
            # Clean up. 
            # Calling this after txn.commit() is a no-op and hence safe.
            txn.discard()
    