from fastapi import APIRouter, Body, Request, Response, HTTPException, status
from fastapi.encoders import jsonable_encoder
from typing import List
from enum import Enum
from model import passenger, transport, flight, scale

models ={
    'passenger:':passenger,
    'transport':transport,
    'scale':scale,
    'flight': flight
}
    

router = APIRouter()
@router.post("/populate", response_description ="post data into the database", status_code = status.HTTP_201_CREATED)
def populate_collection(request:Request,collection:str,data = Body(...)):
    
    doc = jsonable_encoder(data)
    print(data)
    created = request.app.database[collection].insert_many(doc)
    print(created)

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

@router.get("/transport",response_description ="Get all data form a collection",response_model =List[transport])
def get_all_transports(request:Request):
   
    # doc = jsonable_encoder(data)
    transports = list(request.app.database["transport"].find({}))
    return transports
@router.get("/passenger",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_passengers(request:Request):
    passengers = list(request.app.database["passenger"].find({}))
    return passengers
@router.get("/scale",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_scales(request:Request):
    scales = list(request.app.database["scale"].find({}))
    return scales 

@router.get("/flight",response_description ="Get all data form a collection",status_code = status.HTTP_201_CREATED)
def get_all_flights(request:Request):
    flights = list(request.app.database["flight"].find({}))
    return flights