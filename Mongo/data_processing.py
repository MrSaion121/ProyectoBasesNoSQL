import uuid
import pandas as pd
import os 
import random
from model import passenger, transport, flight, scale


df = pd.read_csv('../flight_passengers.csv',sep=',')


transits =df.transit.unique()
flightsData = df[["airline","from","to","day","month","year","connection"]]

scaleData = df[["to","wait","connection"]] #delete where connection is false
scaleData = scaleData[scaleData.connection == True ]
scaleData = scaleData.to.unique()

passenger = df[["age","gender","reason","stay","transit"]]
def flight_data(data):
    fdate = "{day}/{month}/{year}"
    fdata = []
    for index,row in data.iterrows():   

        fn = random.randint(10000,99999)
        date = fdate.format(day =row["day"],month = row["month"],year =row["year"])
        flight = {
                 
                    "flight_number": str(fn),
                    "Ffrom": row["from"],
                    "Fto": row["to"],
                    "date": date,
                    "connection": row["connection"]

        }
        fdata.append(flight)
    f = open('flight.txt','w')
    f.write(str(fdata))
    f.close()
def scale_data(data):
    scaleData = []
  
    for x in data:
        
        scale = {
                
                "location": x,
                "Nfligths": 0,
                "avg_wait": 0
            }
        scaleData.append(scale)
    f = open('scales.txt','w')
    f.write(str(scaleData))
    f.close()
def passenger_data(data):
    
    passengers_data = []
    
    for index,row in data.iterrows():
        psngr = {
     
            "age":row["age"],
            "gender":row["gender"],
            "reason":row["reason"],
            "transit":row["transit"],
            "stay":row["stay"]
        }
   
        passengers_data.append(psngr)
    print(passengers_data[0])
    f = open('passengers.txt','w')
    f.write(str(passengers_data))
    f.close()
def transits_data(data):
    
    finalData = []
    for dt in data:
     
        
        transit = {
             
                "Name":dt,
                "N_uses":0
        }

        finalData.append(transit)
    f = open('transit.txt','w')
    f.write(str(finalData))
    f.close()


transits_data(transits)
passenger_data(passenger)
scale_data(scaleData)
flight_data(flightsData)