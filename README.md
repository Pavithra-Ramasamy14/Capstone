                                                   YouTube Data Harvesting and Warehousing
Introduction:

This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the MongoDB database.
It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer questions.

Developer Guide:
1.Tools to Install:
  Virtual code.
  Jupyter notebook.
  Python 3.11.0 or higher.
  MySQL.
  MongoDB.
2.Requirement Packages to Install:
  pip install google-api-python-client
  pip install pymongo
  pip install mysql-connector-python
  pip install streamlit
  pip install pandas
3.Import Libraries:
  from googleapiclient.discovery import build
  from pymongo import MongoClient
  import mysql.connector
  import pandas as pd
  import streamlit as st
  import re
 4.To Run stremlit:
   streamlit run youtube.py
  
ETL Process:

a) Extract data
  Extract the particular youtube channel data by using the youtube channel id, with the help of API key from youtube API developer console.
b) Process and Transform the data
  After the extraction process, takes the required details from the extraction data and transform it into JSON format.
c) Load data
  After the transformation process, the JSON format data is stored in the MongoDB database, also It has the option to migrate the data to MySQL database from the MongoDB database.

EDA Process and Framework:

a) Access MySQL DB
    Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.
b) Filter the data
    Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
c) Visualization
    Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in      Dataframe Table format.
 
 User Guide:
   1.Search channel_id, copy and paste on the input box and click the collect and store button.
   2.Click the Migrate to SQL button to migrate the specific channel data to the MySQL database from MongoDB.
   3.Select a Question from the dropdown option you can get the results in Dataframe format.
 

 

  
