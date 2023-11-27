import datetime
import json
import pandas as pd 
import pydgraph
import ast
import random



def create_data():
    df = pd.read_csv('flight_passengers.csv',sep=',')


    transitsData =df.transit.unique()
    flightsData = df[["airline","from","to","day","month","year","connection"]]

    scaleData = df[["to","wait","connection"]] #delete where connection is false
    scaleData = scaleData[scaleData.connection == True ]
    scaleData = scaleData.to.unique()
    
    passengersData = df[["age","gender","reason","stay","transit"]]


    flight_data_file(flightsData)
    scales_data_file(scaleData)
    passengers_data_file(passengersData)
    transit_data_file(transitsData)

def transit_data_file(data):
    
    finalData = []
    for dt in data:
        rn = random.randint(1,50)
        
        transit = {
                "dgraph.type":'transport',
                "uid":'_:T'+str(rn),
                "Name":str(dt),
                "N_uses":0
        }

        finalData.append(transit)
    f = open('transitDgraph.txt','w')
    f.write(str(finalData))
    f.close()

def passengers_data_file(data):
    passengers_data = []
    
    for index,row in data.iterrows():
        psngr = {
            "dgraph.type":'passenger',
            "uid":'_:P'+str(index),
            "age":row["age"],
            "gender":row["gender"],
            "reason":row["reason"],
            "transit":str(row["transit"]),
            "stay":row["stay"]
        }
   
        passengers_data.append(psngr)

    f = open('passengersDgraph.txt','w')
    f.write(str(passengers_data))
    f.close()



def scales_data_file(data):
    scaleData = []
  
    for x in data:
        rn = random.randint(1,50)
        scale = {
                "dgraph.type":'scale',
                "uid":'_:S'+str(rn),
                "location": x,
                "avg_wait": 0
            }
        scaleData.append(scale)
    f = open('scalesDgraph.txt','w')
    f.write(str(scaleData))
    f.close()


def flight_data_file(flightsData):
    ##### For flight data
    fdate = "{day}/{month}/{year}"
    fdata = []
    for index,row in flightsData.iterrows():   
          
        fn = random.randint(10000,99999)
        date = fdate.format(day =row["day"],month = row["month"],year =row["year"])
        flight = {
                    "uid":'_:F'+str(index),
                    "dgraph.type":'flight',
                    "airline":row["airline"],
                    "flight_number": str(fn),
                    "Ffrom": row["from"],
                    "Fto": row["to"],
                    "date":  datetime.datetime(row["year"],row["month"] , row["day"], 0, 0, 0, 0).isoformat(),
                    "connection": row["connection"]

        }
        fdata.append(flight)
    f = open('flightDgraph.txt','w')
    f.write(str(fdata))
    f.close()


create_data()