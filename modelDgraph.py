import datetime
import json
import pandas as pd 
import pydgraph
import ast

# Modelo que sugiere promociones a clientes por medio de encontrar las razón más común por la que toman un vuelo

def set_schema(client):
    schema = """
    type Passengers{
        age
        gender
        reason
        stay
        transit
        in       
    }
    
    type Flights{
        airline
        flight_number
        Ffrom
        Fto
        day
        month 
        year
        connection
        wait
    }


    age: int @index(int) .    
    gender: string .
    reason: string @index(term) .
    stay: string .
    transit: string .
    in: uid @reverse .

    airline: string .
    flight_number: string .
    Ffrom: string .
    Fto: string .
    day: int .
    month: int .
    year: int .
    connection: string .
    wait: float .
    
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

def upload_data(client):
    data = data_parser('dataDgraph.txt')
    txn = client.txn()
    try:
            p = data
            response = txn.mutate(set_obj=p)

            # Commit transaction.
            commit_response = txn.commit()
            print(f"Commit Response: {commit_response}")

            print(f"UIDs: {response.uids}")
    finally:
            # Clean up. 
            # Calling this after txn.commit() is a no-op and hence safe.
            txn.discard()
    
def get_all_data(client):
    query = """{
        all(func: has(age)) {
            uid
            age
            gender
            reason
            stay
            transit
            in {
                airline
                flight_number
                Ffrom
                Fto
                day
                month 
                year
                connection
                wait
            }
        }
    }"""

    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)
    print(f"All data in the Database:\n{json.dumps(data, indent=2)}")
     
def passengers_by_reason(client, reason):
    query = """query passengers_by_reason($a: string) {
        passengers(func: allofterms(reason, $a)) {
            uid
            age
            gender
            reason
            stay
            transit
            in {
                airline
                flight_number
                Ffrom
                Fto
                day
                month 
                year
                connection
                wait
            }
        }
    }"""

    variables = {'$a': reason}
    res = client.txn(read_only=True).query(query, variables=variables)
    psngr = json.loads(res.json)

    print(f"Number of passengers with the reason {reason} for their flight: {len(psngr['passengers'])}")
    print(f"Data associated with {reason}:\n{json.dumps(psngr, indent=2)}")

def reason_count(client, ageMin, ageMax):
    query = """query reason_count($a: int, $b: int) { 
        Vacation(func: ge(age, $a)) @filter(le(age, $b) and eq(reason, "On vacation/Pleasure")) {
            count(uid)
        }
        Work(func: ge(age, $a)) @filter(le(age, $b) and eq(reason, "Business/Work")) {
            count(uid)
        }
        Home(func: ge(age, $a)) @filter(le(age, $b) and eq(reason, "Back Home")) {
            count(uid)
        }
    }"""
    print(ageMin)
    variables = {'$a': ageMin, '$b': ageMax}
    res = client.txn(read_only=True).query(query, variables=variables)
    psngr = json.loads(res.json)

    print(f"Data associated with ages {ageMin} to {ageMax}:\n{json.dumps(psngr, indent=2)}")
