import os
import requests

API_URL = os.getenv("MONGO_URL","http://localhost:5000")

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
    if response.ok :
        print("Data created on the database \n")

        res = response.json()
        
        for x in res:
            print(x)
    
    else:

        print(f"Error: {response}")


def main():
    data = {
                "type":"transport",
                "_id":"12312312312312312kk11",
                "Name":"Pickup",
                "N_uses":123  ,
            }
    post_data(data)
if __name__ == "__main__":
    main()