#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 14:47:22 2022

@author: marcos
"""

from pymongo import MongoClient
  
try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
    
## a) Qual o horário do tweet mais antigo e do mais recente para cada regra?

# Connect to database
db = conn.Twitter_DB
  
# Created connection with Collection Soccer, Food and Health
collectionH = db.Health
collectionF = db.Food
collectionS = db.Soccer

# Query
pipeline = [
            { "$sort": {"Date": 1, "Hour": 1}},
            {"$group" :
        {
            "_id": "null" ,
            "lastHour": { "$last": "$Hour" },
            "lastDay": {"$last": "$Date"},
            "firstHour": { "$first": "$Hour"},
            "firstDay": {"$first": "$Date"}
         }}]


## Executing Query with Cursor
query = collectionH.aggregate(pipeline)


# Outputting Results
for doc in query:
    print("\nA) Qual o horário do tweet mais antigo e do mais recente para cada regra?")
    print("\nthe oldest Tweet was at {Hour}, date: {Date}".format(Hour=doc["lastHour"], Date=doc["lastDay"]))
    print("\nthe newest Tweet was at {Hour}, date: {Date}\n".format(Hour=doc["firstHour"], Date=doc["firstDay"]))
    

## b) Qual o período do dia em que cada regra se torna mais frequente?







   
