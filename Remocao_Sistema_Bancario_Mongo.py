import pymongo
from pymongo.server_api import ServerApi

client = pymongo.MongoClient("mongodb+srv://carlosfalcone:1234@cluster0.tjdbym4.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
db = client.test

db['dados_bancarios'].drop()

print('\nREMOÇAO REALIZADO COM SUCESSO\n')