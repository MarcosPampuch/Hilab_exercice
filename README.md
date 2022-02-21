# Hilab_exercice
"Desafio Twitter Stream"

This git was made to present a code to extract and store data from Twitter's filter API to MySQL remote server.

## Requirements

The main versions used to run the code are:

  - Python 3.8.10;
  - Pandas 1.4.1;
  - Mysql-connector-python 8.0.28;
  - requests 2.22.0.
  - MySQL 8.0.28

## Executable files

To stablish the stream connection and start importing data to database: execute the Python file **main.py**;

To start the queries on the database: execute the Python file **main_queries.py**.

## STREAM

To stablish a connection with Twitter's API, the file **stream_sql.py** has a serie of steps which are represented by methods.

Those steps are:

  1. Fetch the actual rules stored on the last Stream;
  
  2. Delete those rules;
  
  3. Set the rules desired by de user;
  
  4. Connect to the stream to receive the data;
  
 The rules chose to this project are: Extract Tweets in portuguese that contain the keywords: __Futebol__, __Comida__ and __Saude__.
 
 ## SQL Storage
 
 After open the stream, all data is sent to a MySQL remote database called **Tweets** where each keyword has its own table.
 
 The methods used to open and store the data in the remote server are on the **database.py** file.
 
 ## Queries 
 
 An independent code was made to fetch data from the database created.
 The main code, written in SQL, was designed to answer three question:
 
  1. Qual o horário do tweet mais antigo e do mais recente para cada regra?
  2. Qual o período do dia em que cada regra se torna mais frequente?
  3. Qual o tweet mais longo em número de caracteres para cada regra? E o mais
curto?
 
 To execute de queries, the methods of database.py are also used.
 
 ## Observations
 
 - The key credentials to access the Stream and the server credentials to login are stored in credentials.py file.
 - More info about Twitter's API can be found [here](https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/introduction)


