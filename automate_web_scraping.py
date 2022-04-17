# import packages
import pandas as pd
from pymongo import MongoClient
import os

# scrape the list of S&P 500
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
payload = pd.read_html(url)
symbol_list = payload[0]

# save to local csv file
symbol_list.to_csv('SAP500_symbol_list.csv', index=False)

# push symbol list to MongoDB
# establish a client connection to MongoDB
MONGODB_CONNECTION_STRING = os.environ['MONGODB_CONNECTION_STRING']
client = MongoClient(MONGODB_CONNECTION_STRING)

# convert to dictionary for uploading to MongoDB
symbol_dict = symbol_list.to_dict('records')

# point to symbolsDB collection 
db = client.symbolsDB

# emtpy symbols collection before inserting new documents
db.symbols.drop()

# insert new documents to collection
db.symbols.insert_many(symbol_dict)