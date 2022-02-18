# -*- coding: utf-8 -*-

import sys
import pandas as pd
import mysql.connector
import wget
import zipfile
import os

class Database():
    def __init__(self, database, host, username, password):
        
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        
        try:
            
            ## Connecting to local database
            self.connection = mysql.connector.connect(
                          host=self.host,
                          user=self.username,
                          password=self.password,
                          database=self.database)
            self.cursor = self.connection.cursor()            
            print("\nConnected to database %s\n"%database)
            
        except Exception as er:
            print(er)
        
    ## clean the database's table
    def empty_table(self, table):
        self.table = table
        empty_query = "DELETE FROM %s;"%self.table
        
        self.cursor.execute(empty_query)
        self.connection.commit()
        print("\nTable %s empty!\n"%self.table)
        
    ## Create the chunks of data if needed
    def create_chunks(self, dataframe):
        self.dataframe = dataframe
        l_tuples = list(dataframe.itertuples(index=False, name=None))
        n_rows = len(dataframe)
        
        chunk_size = 1000000;
        chunks = (n_rows//chunk_size)
        rest = (n_rows%chunk_size)
        
        
        list_chunks = [ l_tuples[i : i + chunk_size] for i in range(0, chunks*chunk_size, chunk_size)]
        list_chunks.append(l_tuples[chunks*chunk_size : chunks*chunk_size+rest]) 
    
        print("%d chunks created"%len(list_chunks))
    
        return list_chunks
        
    ## Send data from dataframe to database
    def send_to_database(self, dataframe, table, list_chunks = None): ## Upload Pandas dataframe to table chose
        
        self.dataframe = dataframe
        self.table = table
        self.list_chunks = list_chunks
        
        auxi_query = "INSERT INTO %s VALUES "%self.table
        fill_query = auxi_query + "(%s, %s, %s, %s, %s)"
        
#        if self.list_chunks != None:
#            self.chunk_size = len(list_chunks)
#            for i in range(self.chunk_size):
#                self.cursor.executemany(fill_query, self.list_chunks[i])
#                self.connection.commit()
#                print("\nchunk %d sent!\n"%i)
#        else:
        self.l_tuples = list(self.dataframe.itertuples(index=False, name=None))
        self.cursor.execute(fill_query, self.l_tuples[0])
        self.connection.commit()
            
        print("\nData sent to table %s\n"%(self.table))
        
        #self.connection.close()
        
## Download and unzip file from url
def download_unzip(url, path_to_zip_file, directory_to_extract_to): 
    
    print("Download started")
    wget.download(url, path_to_zip_file)
    print("\nZip file downloaded\n")
    
    file_zip_name = (url.split(sep="/")[-1])                        ##"file.zip"
    file_name = file_zip_name.split(sep=".zip")[0]                  ##"file"
    
    ziped_file_path = os.path.join(path_to_zip_file,file_zip_name)  ##"path/file.zip"
    
    print("Decompressing")
    with zipfile.ZipFile(ziped_file_path, 'r') as zip_ref: 
        zip_ref.extractall(directory_to_extract_to)
       
    print("\nUnziped file stored in %s\n"%directory_to_extract_to)
    
    return os.path.join(directory_to_extract_to, file_name)         ##"path/file.csv"