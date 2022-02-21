#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 01:04:09 2022

@author: marcos
"""
import sys
import mysql.connector

from database import Database
from credentials import credentials

connection = Database(**credentials)

# a) Qual o horário do tweet mais antigo e do mais recente para cada regra?
def min_max():
    
    query_Food = """SELECT DATE_FORMAT(MAX(creation_hour), '%r') AS Food 
                    FROM table_Food 
                    WHERE creation_date = (SELECT MAX(creation_date) 
                                            FROM table_Food) 
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
                    UNION 
                    SELECT DATE_FORMAT(MIN(creation_hour), '%r') AS Health 
                    FROM table_Health
                    WHERE creation_date = (SELECT MIN(creation_date) 
                                            FROM table_Health) 
                    GROUP BY creation_date;
                    """   

    
    ## Executing queries in MySQL database
    MIN_MAX_Food = connection.query_database(query_Food)  
    MIN_MAX_Soccer = connection.query_database(query_Soccer)
    MIN_MAX_Health = connection.query_database(query_Health)      

    print("Tag Comida")
    print('Horario Tweet mais recente: %s'%MIN_MAX_Food[0][0])
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Food[1][0])
    print("\nTag Futebol")
    print('Horario Tweet mais recente: %s'%MIN_MAX_Soccer[0][0])
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Soccer[1][0])
    print("\nTag Saude")
    print('Horario Tweet mais recente: %s'%MIN_MAX_Health[0][0])
    print('Horario Tweet mais antigo: %s'%MIN_MAX_Health[1][0])


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
                
    ## Executing queries in MySQL database
    Period_Food = connection.query_database(query_Food)
    Period_Soccer = connection.query_database(query_Soccer)
    Period_Health = connection.query_database(query_Health)
    
    print('O periodo do dia para a tag Comida eh: a %s com %i tweets\n'%(Period_Food[0][1],Period_Food[0][0]))
    print('O periodo do dia para a tag Futebol eh: a %s com %i tweets\n'%(Period_Soccer[0][1],Period_Soccer[0][0]))
    print('O periodo do dia para a tag Saude eh: a %s com %i tweets'%(Period_Health[0][1],Period_Health[0][0]))
    

def string_sizes():
    
    query_Food = """SELECT id, CHAR_LENGTH(text) AS Length
                FROM table_Food 
                WHERE CHAR_LENGTH(text) = (SELECT MAX(CHA) AS Number_MAX 
                                          FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                FROM table_Food) AS tt1) 
                                            OR CHAR_LENGTH(text) = (SELECT MIN(CHA) AS Number_MIN 
                                                FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                      FROM table_Food) AS tt2) ORDER BY Length;
                """
    query_Soccer = """SELECT id, CHAR_LENGTH(text) AS Length
                FROM table_Soccer 
                WHERE CHAR_LENGTH(text) = (SELECT MAX(CHA) AS Number_MAX 
                                          FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                FROM table_Soccer) AS tt1) 
                                            OR CHAR_LENGTH(text) = (SELECT MIN(CHA) AS Number_MIN 
                                                FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                      FROM table_Soccer) AS tt2) ORDER BY Length;
                """
    query_Health = """SELECT id, CHAR_LENGTH(text) AS Length
                FROM table_Health 
                WHERE CHAR_LENGTH(text) = (SELECT MAX(CHA) AS Number_MAX 
                                          FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                FROM table_Health) AS tt1) 
                                            OR CHAR_LENGTH(text) = (SELECT MIN(CHA) AS Number_MIN 
                                                FROM (SELECT CHAR_LENGTH(text) AS 'CHA' 
                                                      FROM table_Health) AS tt2) ORDER BY Length;
                """
                
    ## Executing queries in MySQL database                              
    Period_Food = connection.query_database(query_Food)
    Period_Soccer = connection.query_database(query_Soccer)
    Period_Health = connection.query_database(query_Health)
    
    print('O Tweet mais curto da table Food tem %d caracteres (id: %s)'%(Period_Food[0][1],Period_Food[0][0]))
    print('O Tweet mais longo da table Food tem %d caracteres (id: %s)\n'%(Period_Food[1][1],Period_Food[1][0]))
    print('O Tweet mais curto da table Soccer tem %d caracteres (id: %s)'%(Period_Soccer[0][1],Period_Soccer[0][0]))
    print('O Tweet mais longo da table Soccer tem %d caracteres (id: %s)\n'%(Period_Soccer[1][1],Period_Soccer[1][0]))
    print('O Tweet mais curto da table Health tem %d caracteres (id: %s)'%(Period_Health[0][1],Period_Health[0][0]))
    print('O Tweet mais longo da table Health tem %d caracteres (id: %s)\n'%(Period_Health[1][1],Period_Health[1][0]))
    

######################################################################################################################
    

    
    
    
    
    
    
    
    
    