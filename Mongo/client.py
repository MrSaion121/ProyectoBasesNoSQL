import os
import requests
import ast
API_URL = os.getenv("MONGO_URL","http://localhost:5000")
def get_all(typeO:str):
    if typeO == "transport":
        url =url =  API_URL+'/data'+'/transport'
        params = {
            "data":typeO
        }
        response = requests.get(url)
        if response.ok:
            res = response.json()
            print(res)
        else:
            print(f"Error: {response}")


def post_data(data:dict):
    url =  API_URL+'/data'+'/data_Mongo'
    params = data
    response = requests.post(url, json = params)
    if response.ok:

        print("Data created on the database \n")

        res = response.json()
        
        print(res)
    else:
        print(f"Error: {response}")

def get_data(query:dict):

    response = requests.get(API_URL,params = params)
    if response.ok:
        res = response.json()
        
        for x in res:
            print(x)
    
    else:

        print(f"Error: {response}")
def populate_db(collection:str,data):
    
    url =  API_URL+'/data'+'/populate/?collection='+collection
    print(url)
    params = data
    response = requests.post(url, json = params)
    if response.ok:

        print("Data created on the database \n")

        res = response.json()
        
        print(res)
    else:
        print(f"Error: {response}")


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
      

def main():
    # data = {
    #             "type":"transport",
    #             "_id":"12312312312312312kk11",
    #             "Name":"Pickup",
    #             "N_uses":123  ,
    #         }
    # # post_data(data)
    # get_all("transport")

    data = data_parser("scales.txt")
    print(type(data))
    populate_db("scales",data)
if __name__ == "__main__":
    main()