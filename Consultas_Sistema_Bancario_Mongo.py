import pymongo
from pymongo.server_api import ServerApi
import pprint

client = pymongo.MongoClient("mongodb+srv://carlosfalcone:1234@cluster0.tjdbym4.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
db = client.test
dados_bancarios = db.dados_bancarios

print('\n Qdte de documentos: ',dados_bancarios.count_documents({}))
print('\n Qtd CPF 04208661674 : ',dados_bancarios.count_documents({'cpf':'04208661674'}))
print('\n Qtde nº conta = 3: ',dados_bancarios.count_documents({'conta':'3'}))

for item in dados_bancarios.find():
    if item['conta'] == 3:
        print('\nResultado busca 1:')
        pprint.pprint(item)
    if item['cpf'] == '04208661674':
        print('\nResultado busca 2:')
        pprint.pprint(item)

print('\n FIM DAS CONSULTAS\n')