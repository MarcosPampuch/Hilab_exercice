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
    
    query_Food = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Food 
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
                
    query_Soccer = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Soccer 
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
                
    query_Health = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Health 
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

    
            
    MIN_MAX_Food = connection.query_database(query_Food)  
    MIN_MAX_Soccer = connection.query_database(query_Soccer)
    MIN_MAX_Health = connection.query_database(query_Health)      
#    MIN_MAX_Soccer = connection.query_database(Query_Soccer)   
#    MIN_MAX_Health = connection.query_database(Query_Health)   
#    return MIN_MAX_Food, MIN_MAX_Soccer, MIN_MAX_Health

    print("Tag Comida")
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Food[0][0])
    print('Horario Tweet mais recente: %s'%MIN_MAX_Food[1][0])
    print("\nTag Futebol")
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Soccer[0][0])
    print('Horario Tweet mais recente: %s'%MIN_MAX_Soccer[1][0])
    print("\nTag Saude")
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Health[0][0])
    print('Horario Tweet mais recente: %s'%MIN_MAX_Health[1][0])

print('(a) Qual o horário do tweet mais antigo e do mais recente para cada regra?\n')

min_max()


# b) Qual o período do dia em que cada regra se torna mais frequente?   
  
def periods():
    
        ## periods: Dawn    ---> FROM 00:00:00 TO 05:59:59
        ## periods: Morning ---> FROM 06:00:00 TO 11:59:59
        ## periods: Evening ---> FROM 12:00:00 TO 17:59:59
        ## periods: Night   ---> FROM 18:00:00 TO 23:59:59
        
    query_Food = """SELECT Qty_Period, Period 
                    FROM (SELECT (SELECT('Madrugada (De 00h00 ate 6h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Food 
                          WHERE creation_hour BETWEEN '00:00:00' AND '05:59:59'
                          
                          UNION 
                          
                          SELECT (SELECT('Manhã (De 6h00 ate 12h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Food 
                          WHERE creation_hour BETWEEN '06:00:00' AND '11:59:59'  
                          
                          UNION 
                          
                          SELECT (SELECT('Tarde (De 12h00 ate 18h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Food WHERE creation_hour BETWEEN '12:00:00' AND '17:59:59' 
                          
                          UNION
                          
                          SELECT (SELECT('Noite (De 18h00 ate 00h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Food 
                          WHERE creation_hour BETWEEN '18:00:00' AND '23:59:59') AS tt 
                          ORDER BY Qty_Period DESC LIMIT 1;
                """
    query_Soccer = """SELECT Qty_Period, Period 
                    FROM (SELECT (SELECT('Madrugada (De 00h00 ate 6h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Soccer 
                          WHERE creation_hour BETWEEN '00:00:00' AND '05:59:59'
                          
                          UNION 
                          
                          SELECT (SELECT('Manhã (De 6h00 ate 12h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Soccer
                          WHERE creation_hour BETWEEN '06:00:00' AND '11:59:59'  
                          
                          UNION 
                          
                          SELECT (SELECT('Tarde (De 12h00 ate 18h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Soccer 
                          WHERE creation_hour BETWEEN '12:00:00' AND '17:59:59' 
                          
                          UNION
                          
                          SELECT (SELECT('Noite (De 18h00 ate 00h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Soccer 
                          WHERE creation_hour BETWEEN '18:00:00' AND '23:59:59') AS tt 
                          ORDER BY Qty_Period DESC LIMIT 1;
                """
    query_Health = """SELECT Qty_Period, Period 
                    FROM (SELECT (SELECT('Madrugada (De 00h00 ate 6h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Health
                          WHERE creation_hour BETWEEN '00:00:00' AND '05:59:59'
                          
                          UNION 
                          
                          SELECT (SELECT('Manhã (De 6h00 ate 12h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Health 
                          WHERE creation_hour BETWEEN '06:00:00' AND '11:59:59'  
                          
                          UNION 
                          
                          SELECT (SELECT('Tarde (De 12h00 ate 18h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Health 
                          WHERE creation_hour BETWEEN '12:00:00' AND '17:59:59' 
                          
                          UNION
                          
                          SELECT (SELECT('Noite (De 18h00 ate 00h00)')) AS Period, COUNT(creation_hour) AS Qty_Period 
                          FROM table_Health
                          WHERE creation_hour BETWEEN '18:00:00' AND '23:59:59') AS tt 
                          ORDER BY Qty_Period DESC LIMIT 1;
                """
                
                
    Period_Food = connection.query_database(query_Food)
    Period_Soccer = connection.query_database(query_Soccer)
    Period_Health = connection.query_database(query_Health)
    
    print('O periodo do dia para a tag Comida eh: a %s com %i tweets\n'%(Period_Food[0][1],Period_Food[0][0]))
    print('O periodo do dia para a tag Futebol eh: a %s com %i tweets\n'%(Period_Soccer[0][1],Period_Soccer[0][0]))
    print('O periodo do dia para a tag Saude eh: a %s com %i tweets'%(Period_Health[0][1],Period_Health[0][0]))
    
print('\n\n(b) Qual o período do dia em que cada regra se torna mais frequente?\n')
periods()  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    