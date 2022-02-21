# -*- coding: utf-8 -*-

import sys
import pandas as pd
import mysql.connector


class Database():
    def __init__(self, database, host, username, password):
        
        self.database = database
        self.host = host
        self.username = username
        self.password = password
        
        try:
            
            ## Connecting to database server
            self.connection = mysql.connector.connect(
                          host=self.host,
                          user=self.username,
                          password=self.password,
                          database=self.database)
            self.cursor = self.connection.cursor()            
            print("\nConnected to database %s\n"%self.database)
            
        except Exception as er:
            print(er)
        
    
    ## Send data from dataframe to database
    def send_to_database(self, dataframe, table, list_chunks = None): ## Upload Pandas dataframe to table chose
        
        self.dataframe = dataframe
        self.table = table
        self.list_chunks = list_chunks
        
        auxi_query = "INSERT INTO %s VALUES "%self.table
        fill_query = auxi_query + "(%s, %s, %s, %s, %s)"

        self.l_tuples = list(self.dataframe.itertuples(index=False, name=None))
        self.cursor.execute(fill_query, self.l_tuples[0])
        self.connection.commit()
            
        print("\nData sent to table %s\n"%(self.table))
        
        
    ## Excute desired query
    def query_database(self, query):
      
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records

        