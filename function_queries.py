#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 01:04:09 2022

@author: marcos
"""
import sys
import mysql.connector

sys.path.append("/home/marcos/project_Hilab")
from database import Database

database = 'Tweets'
host = 'localhost'
username = 'maco'
password = 'Pampuch1998'

connection = Database(database, host, username, password)

# a) Qual o horário do tweet mais antigo e do mais recente para cada regra?
def min_max():
    
    Query_Food = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Food 
                    FROM table_Food 
                    WHERE creation_date = (SELECT MAX(creation_date) 
                                            FROM table_Food) 
                    GROUP BY creation_date 
                    UNION 
                    SELECT DATE_FORMAT(MIN(creation_hour), '%r') AS Food 
                    FROM table_Food 
                    WHERE creation_date = (SELECT MIN(creation_date) 
                                            FROM table_Food) 
                    GROUP BY creation_date;
                """
                
#                    """SELECT DATE_FORMAT(creation_hour, '%r') AS Food 
#                    FROM table_Food                                            ### SECOND OPTION OF QUERY (INDIV)
#                    ORDER BY creation_date , creation_hour LIMIT 1
#                    """
                
    Query_Soccer = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Soccer 
                    FROM table_Soccer
                    WHERE creation_date = (SELECT MAX(creation_date) 
                                            FROM table_Soccer) 
                    GROUP BY creation_date 
                    UNION 
                    SELECT DATE_FORMAT(MIN(creation_hour), '%r') AS Soccer 
                    FROM table_Soccer
                    WHERE creation_date = (SELECT MIN(creation_date) 
                                            FROM table_Soccer) 
                    GROUP BY creation_date;
                    """
                
    Query_Health = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Health 
                    FROM table_Health
                    WHERE creation_date = (SELECT MAX(creation_date) 
                                            FROM table_Health) 
                    GROUP BY creation_date 
                    UNION 
                    SELECT DATE_FORMAT(MIN(creation_hour), '%r') AS Health 
                    FROM table_Health
                    WHERE creation_date = (SELECT MIN(creation_date) 
                                            FROM table_Health) 
                    GROUP BY creation_date;
                    """   

    
            
    MIN_MAX_Food = connection.query_database(Query_Food)  
    MIN_MAX_Soccer = connection.query_database(Query_Soccer)
    MIN_MAX_Health = connection.query_database(Query_Health)      
#    MIN_MAX_Soccer = connection.query_database(Query_Soccer)   
#    MIN_MAX_Health = connection.query_database(Query_Health)   
#    return MIN_MAX_Food, MIN_MAX_Soccer, MIN_MAX_Health
    return MIN_MAX_Food, MIN_MAX_Soccer, MIN_MAX_Health

Food, Soccer, Health = min_max()

print("TABLE FOOD")
print('Horario Tweet mais antigo: %s'%Food[0][0])
print('Horario Tweet mais recente: %s'%Food[1][0])
print("\nTABLE SOCCER")
print('Horario Tweet mais antigo: %s'%Soccer[0][0])
print('Horario Tweet mais recente: %s'%Soccer[1][0])
print("\nTABLE HEALTH")
print('Horario Tweet mais antigo: %s'%Health[0][0])
print('Horario Tweet mais recente: %s'%Health[1][0])

# b) Qual o período do dia em que cada regra se torna mais frequente?     
#def max_period():
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    