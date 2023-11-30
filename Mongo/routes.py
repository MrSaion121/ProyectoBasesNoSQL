from fastapi import APIRouter, Body, Request, Response, HTTPException, status
from fastapi.encoders import jsonable_encoder
from typing import List
from enum import Enum
from model import passenger, transport, flight, scale
import ast
models ={
    'passenger:':passenger,
    'transport':transport,
    'scale':scale,
    'flight': flight
}
    

router = APIRouter()
@router.post("/populate", response_description ="post data into the database", status_code = status.HTTP_201_CREATED)
def populate_collection(request:Request,collection:str,data = Body(...)):
    
    request.app.database[collection].delete_many({})
    doc = jsonable_encoder(data)
    
    created = request.app.database[collection].insert_many(doc)
   
@router.post("/data_Mongo", response_description ="post data into the database", status_code = status.HTTP_201_CREATED)
def create_data(request:Request, data = Body(...)):
    # Model selection

    type_of = data["type"]
    # model = models[type_of]
    data.pop("type")

    doc = jsonable_encoder(data)
    new_doc = request.app.database[type_of].insert_one(doc)
    created = request.app.database[type_of].find_one(
        {"_id": new_doc.inserted_id}
    )
    return created 

@router.get("/transport",response_description ="Get all data form a collection")
def get_all_transports(request:Request):
   
    transports = request.app.database["transport"].find({})
    ret = []
    for doc in transports:
        
        new_doc = {
            "Name":doc["Name"],
            "N_uses":doc["N_uses"]
        }
        ret.append(new_doc)

    return ret
@router.get("/passenger",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_passengers(request:Request):
    passengers = request.app.database["passengers"].find({})
    ret = []
    for doc in passengers:
        new_doc = {
            "age":doc["age"],
            "gender":doc["gender"],
            "reason":doc["reason"],
            "transit":doc["transit"],
            "stay":doc["stay"]
        }
        ret.append(new_doc)
    return ret
@router.get("/scales",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_scales(request:Request,query):
    
    queryN  = ast.literal_eval(query)
    scales = request.app.database["scales"].find(queryN)
    print(scales)
    ret = []
    for doc in scales:
        doc.pop('_id')
        ret.append(doc)

    return ret

@router.get("/flight",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_flights(request:Request,query):

    queryN  = ast.literal_eval(query)

    flights = request.app.database["flight"].find(queryN)
    ret = []
    for doc in flights:
        new = {
            
        "flight_number":doc["flight_number"],
        "Ffrom":doc["Ffrom"],
        
        "Fto":doc["Fto"],
        "date":doc["date"],
        "connection":doc["connection"],
        "wait":doc["wait"]
        }
        ret.append(new)
    return ret
@router.get("/best_scales",response_description ="Status of the request",status_code = status.HTTP_201_CREATED)
def get_scales(request:Request):
    pipeline = [
        {"$match":{}},{"$sort":{ "Nfligths": -1 }}
        ]
    pipeline = jsonable_encoder(pipeline)
    res = request.app.database["scales"].aggregate(pipeline)
    
    ret = []
    for doc in res:
        new ={
            "location":doc["location"],
            "Nfligths":doc["Nfligths"],
            "avg_wait":doc["avg_wait"]
          }
        ret.append(new)
    return ret
@router.get("/best_scales_wait",response_description ="Status of the request",status_code = status.HTTP_201_CREATED)
def get_scales(request:Request):
    pipeline = [
        {"$match":{}},{"$sort":{ "avg_wait": -1 }}
        ]
    pipeline = jsonable_encoder(pipeline)
    res = request.app.database["scales"].aggregate(pipeline)
    
    ret = []
    for doc in res:
        new ={
            "location":doc["location"],
            "Nfligths":doc["Nfligths"],
            "avg_wait":doc["avg_wait"]
          }
        ret.append(new)
    return ret

@router.get("/best_transports",response_description ="Status of the request",status_code = status.HTTP_201_CREATED)
def get_scales(request:Request):
    pipeline = [
        {"$match":{}},{"$sort":{ "N_uses": -1 }}
        ]
    pipeline = jsonable_encoder(pipeline)
    res = request.app.database["transport"].aggregate(pipeline)
    
    ret = []
    for doc in res:
        new ={
            "Name":doc["Name"],
            "N_uses":doc["N_uses"]
          }
        ret.append(new)
    return ret
@router.post("/update",response_description ="Status of the request",status_code = status.HTTP_201_CREATED)
def update(request:Request, collection:str,data= Body(...)):

    print(collection)
    dataUpdates = jsonable_encoder(data)
    if collection == "transport":
        for x in dataUpdates:
            query = {
                "Name":x.pop("name")
            
            }
            
            update =   { "$set": { "N_uses": x["N_uses"] } }
            
            
            res = request.app.database[collection].update_one(query,update)
            print(res)
    if collection =="scales":
        print(dataUpdates)
        
        
        query = {
                "location":dataUpdates["location"]
            
            }
           
            
        update =   { "$set": { "Nfligths": dataUpdates["Nfligths"],"avg_wait":dataUpdates["avg_wait"] } }
            
            
        res = request.app.database[collection].update_one(query,update)
        print(res)

        
    return {"ok":200}