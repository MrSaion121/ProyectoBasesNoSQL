import datetime
import json
import pandas as pd 
import pydgraph
import ast
import random



def create_data_file():
    df = pd.read_csv('flight_passengers.csv',sep=',')
    flightsData = df[["airline","from","to","day","month","year","age","gender","reason","stay","transit","connection", "wait"]]

    data = []
    
    for index, row in flightsData.iterrows():
        fn = random.randint(10000,99999)
        psngr = {
            "dgraph.type":'passenger',
            "uid":'_:P'+str(index),
            "age":row["age"],
            "gender":row["gender"],
            "reason":row["reason"],
            "stay":row["stay"],
            "transit":str(row["transit"]),
            "in": {
                "uid":'_:F'+str(index),
                "dgraph.type":'flight',
                "airline":row["airline"],
                "flight_number": str(fn),
                "Ffrom": row["from"],
                "Fto": row["to"],
                "date":  datetime.datetime(row["year"],row["month"] , row["day"], 0, 0, 0, 0).isoformat(),
                "connection": row["connection"]
            }
        }
   
        data.append(psngr)

    f = open('dataDgraph.txt','w')
    f.write(str(data))
    f.close()

create_data_file()