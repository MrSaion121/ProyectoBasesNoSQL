import uuid
from typing import Optional
from pydantic import BaseModel, Field
class scale(BaseModel):
    id: str = Field(default_factory = uuid.uuid4)
    location: str = Field(...)
    Nfligths: int = Field(...)
    avg_wait: float = Field(...)
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example":{
                "_id": "234234234234324234234",
                "location": "Guadalajara, Jalisco, Mexico",
                "Nfligths": 2123,
                "avg_wait": 123.213
            }
        }
class flight(BaseModel):
    id: str = Field(default_factory = uuid.uuid4)
    airline: str = Field(...)
    flight_number: str = Field(...)
    Ffrom: str = Field(...)
    Fto: str  = Field(...)
    date: str = Field(...)
    connection: bool = Field(...)

    class Config:
        populate_by_name = True
        json_schema_extra ={

            "example":{
                "_id": "asdassadasdasdsadadsd",
                "flight_number": "13123123",
                "Ffrom": "Guadalajara, Jalisco, Mexico",
                "Fto": "Oaxaca, Oacaxa, Mexico",
                "date": "2/2/2023",
                "connection": False
            }
        }
class passenger(BaseModel):
    id: str = Field(default_factory = uuid.uuid4)
    age: int = Field(...)
    gender: str = Field(...)
    reason: str = Field(...)
    transit: str = Field(...)

    class Config:
        populate_by_name = True
        json_schema_extra ={

            "example":{
                "_id": "231231231231",
                "age": 22,
                "gender": "male",
                "reason": "On vacation/Pleasure",
                "transit": "Pickup"
            }
        }
        

class transport(BaseModel):
    id: str = Field(default_factory = uuid.uuid4)
    Name: str = Field(...)
    N_uses: int = Field(...)
    class Config:
        populate_by_name = True
        json_schema_extra ={
            "example":{

                "_id":"12312312312312312kk",
                "Name":"Pickup",
                "N_uses":123  
            }
        }