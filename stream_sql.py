#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 23:34:03 2022

@author: marcos
"""

import sys
import json
import pandas as pd
from database import Database
import requests

class StreamSQL():
    def __init__(self, bearer_token):
        
        self.bearer_token = bearer_token

    ## method for Stream authentication
    def bearer_oauth(self, r):
    
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    ## fetch actual rules of the Stream
    def get_rules_stream(self):
        self.response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(self.response.status_code, self.response.text)
            )
        print("\nFetching rules of Steam..\n")
        return self.response.json()
    
    ## delete actual rules of the Stream
    def delete_rules_stream(self, rules):
        if rules is None or "data" not in rules:
            return None
    
        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print("\nDeleting rules of Stream..\n")
    
    ## set new rules to the Stream
    def set_rules_stream(self, delete):
        # You can adjust the rules if needed
        sample_rules = [
            {"value": "Futebol lang:pt", "tag": "Soccer rule"},
            {"value": "Saúde lang:pt", "tag": "Health rule"},
            {"value": "Comida lang:pt", "tag": "Food rule"}
        ]
        payload = {"add": sample_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print("\nSetting rules to Stream..\n")
    
    ## Create and organize main dataframe columns 
    def column_adjust(self, dataframe):
        
        date_hour = dataframe.loc[0, 'data.created_at']
        date_hour = date_hour.split(sep='T')
        date_hour[-1] = date_hour[-1].split(sep='.')[0]
        
        dataframe.loc[0,'data.created_at'] = date_hour[0]
        dataframe['hour'] = date_hour[-1]
        
        return dataframe[['data.id','data.text', 'data.created_at','hour', 'tag']]
        
    ## Open Stream connection and send to SQL database
    def stream_sql(self, set, database, host, username, password):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at", 
            auth=self.bearer_oauth, stream=True)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print("\nStream connection stablished!\n")
        
        
        connection = Database(database, host, username, password)
        
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
    
                ### Creating Dataframe with JSON object
                json_df = pd.json_normalize(json_response, record_path =['matching_rules'], meta=[['data','text'], ['data','id'],['data', 'created_at']])
    
                ## Adjusting coloumns
                tweet_df = self.column_adjust(json_df)
                
                ## send tweet to table table_Soccer
                if tweet_df.loc[0, 'tag'] == 'Soccer rule':
                    connection.send_to_database(tweet_df, 'table_Soccer')
                    
                ## send tweet to table table_Health
                elif tweet_df.loc[0, 'tag'] == 'Health rule':
                    connection.send_to_database(tweet_df, 'table_Health')
    
                ## send tweet to table table_Food
                else:
                    connection.send_to_database(tweet_df, 'table_Food')

                    
                
                
                
                
                
                
                
                