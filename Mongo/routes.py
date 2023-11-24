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

@router.post("/data_Mongo", response_description ="post data into the database", status_code = status.HTTP_201_CREATED)
def create_data(request: Request, data = Body(...)):
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