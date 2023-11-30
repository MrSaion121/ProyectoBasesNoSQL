import os
import requests
import ast
API_URL = os.getenv("MONGO_URL","http://localhost:5000")
def get_all(typeO:str, query ={}):
    if typeO == "flight":
        url = API_URL+'/data'+'/flight'+'/?query='+str(query)
        params  = {}
        response = requests.get(url,json=params)
        if response.ok:
            res = response.json()

            return res
        else:
            print(f"Error: {response}")

        
    if typeO == "transport":
        url = API_URL+'/data'+'/transport'+'/query?='+str(query)
     
        response = requests.get(url, json = params)
        if response.ok:
            res = response.json()
     
            return res
        else:
            print(f"Error: {response}")

    if typeO == "scales":
        url =  API_URL+'/data'+'/scales'+'/?query='+str(query)
        
        response = requests.get(url)
        if response.ok:
            res = response.json()
           
            return res
        else:
            print(f"Error: {response}")

    if typeO == "passengers":
        url =  API_URL+'/data'+'/passenger'
        params = {}
        response = requests.get(url ,json ={})
        if response.ok:
            res = response.json()
            
            return res
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

def set_calculated_values(collection:str):
    if collection == "transport":
        passengers_data =   get_all("passengers")
        transport = get_all("transport")

        nanC = 0
        publicC = 0
        serviceC = 0
        PickupC = 0
        rentalC = 0
        ownC = 0
        cabC = 0
        print(passengers_data)
        for doc in passengers_data:
            transport = doc["transit"]
            if transport == "nan":
                nanC+=1
            elif transport =="Airport cab":
                cabC+=1
            elif transport == "Car rental":
                rentalC+=1
            elif transport == "Pickup":
                PickupC+=1
            elif transport == "Public Transportation":
                publicC +=1
                
            elif transport =="Own car":
                ownC+=1
            elif transport == "Mobility as a service":
                serviceC+=1
            
        data_to_update = [

            {"name":"nan", "N_uses":nanC },
            {"name":"Airport cab","N_uses":cabC},
            {"name":"Car rental","N_uses":rentalC},
            {"name":"Pickup","N_uses":PickupC},
            {"name":"Public Transportation","N_uses":publicC},
            {"name":"Own car", "N_uses":ownC},
            {"name":"Mobility as a service","N_uses":serviceC}

        ]
        url =  API_URL+'/data'+'/update/?collection='+collection
        
        response = requests.post(url, json=data_to_update)
        if response.ok:
            res = response.json()
            
            for x in res:
                print(x)
        
        else:

            print(f"Error: {response}")
    if collection == "scales":
        url =  API_URL+'/data'+'/update/?collection='+collection
        scales = ('GDL','JFK','PDX','SJC','LAX')

       
        for x in scales:
            
      
            query  = {"Fto":x,"connection":True}
            data = get_all("flight",query) 
            calc = 0
            N = 0
            loc = x
            for x in data:
                N = N + 1
                calc = calc + x["wait"]

            # update values

            updates = {"location":loc,"Nfligths":N,"avg_wait":int(calc/len(data))}
            response = requests.post(url, json=updates)
            if response.ok:
                res = response.json()
            
                for x in res:
                 print(x)
            else:

                print(f"Error: {response}")
            
           
def get_most_used_scales():
    url = API_URL+'/data'+'/best_scales'
    response = requests.get(url)
    if response.ok:
        res = response.json()
            
        for x in res:
            print(x)
    else:

        print(f"Error: {response}")
def get_most_used_wait_scales():
    url = API_URL+'/data'+'/best_scales_wait'
    response = requests.get(url)
    if response.ok:
        res = response.json()
            
        for x in res:
            print(x)
    else:

        print(f"Error: {response}")
def get_most_used_transports():
    url = API_URL+'/data'+'/best_transports'
    response = requests.get(url)
    if response.ok:
        res = response.json()
            
        for x in res:
            print(x)
    else:

        print(f"Error: {response}")
    
          
def main():
    # data = {
    #             "type":"transport",
    #             "_id":"12312312312312312kk11",
    #             "Name":"Pickup",
    #             "N_uses":123  ,
    #         }
    # # post_data(data)
    # get_all("transport")
#
    # data = data_parser("scales.txt")
    # flight_data = data_parser("flight.txt")
    # passenger_data = data_parser("passengers.txt")
    # transit_data = data_parser("transit.txt")

    # populate_db("transport", transit_data)
    # populate_db("passengers",passenger_data)
    # populate_db("flight",flight_data )
    # populate_db("scales",data)

    # set_calculated_values("transport")
    # query  = {"connection":True}
    # get_all("flight",query)
    # set_calculated_values("scales")
    print("Best scales by Nfligths")
    get_most_used_scales()


    print("Best scales by wait time")
    get_most_used_wait_scales()
    
    print("Most used transports")
    get_most_used_transports()
if __name__ == "__main__":
    main()