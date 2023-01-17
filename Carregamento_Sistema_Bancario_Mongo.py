import pymongo
from pymongo.server_api import ServerApi

class Saldo:

    def ler_saldo(cpf,agencia,conta): # static method
        with open(f'C:/Users/Falcone/Documents/0_DIO/IntegracaoPython_BancoDados/IntegrationWithSQL/Contas/{cpf}_{agencia}_{conta}.txt','r') as file:
                for line in file:
                    if 'Saldo' in line:
                        dividir_linha=line.split(' ')
                        saldo=dividir_linha[-1]
                        saldo_formatado=saldo
                        saldo=float(saldo.replace('R$',''))
                return saldo

class BancoDeDados():
    
    client = pymongo.MongoClient("mongodb+srv://carlosfalcone:1234@cluster0.tjdbym4.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
    db = client.test

    dados_bancarios = db.dados_bancarios

    # leitura da conta principal
    with open('C:/Users/Falcone/Documents/0_DIO/IntegracaoPython_BancoDados/IntegrationWithSQL/Clientes_Cadastrados.txt', 'r') as file:
        next(file) # pular a leitura do cabeçalho
        for line in file:
            coluna = line.split(',')
            valor_saldo = Saldo.ler_saldo(coluna[0],coluna[3],coluna[4])
            dados = {
                'cpf':coluna[0],
                'nome':coluna[1],
                'agencia':coluna[3],
                'conta':coluna[4],
                'saldo':valor_saldo
                }
            insercao = dados_bancarios.insert_one(dados).inserted_id


    # leitura da conta secundaria
    with open('C:/Users/Falcone/Documents/0_DIO/IntegracaoPython_BancoDados/IntegrationWithSQL/Clientes_Cadastrados.txt', 'r') as file:
        next(file) # pular a leitura do cabeçalho
        for line in file:
            coluna = line.split(',')
            if coluna[5] != 'fim\n':
                valor_saldo = Saldo.ler_saldo(coluna[0],coluna[3],coluna[5])
                dados = {
                    'cpf':coluna[0],
                    'nome':coluna[1],
                    'agencia':coluna[3],
                    'conta':coluna[5],
                    'saldo':valor_saldo
                    }
                insercao = dados_bancarios.insert_one(dados).inserted_id

BancoDeDados()

print('\nCARREGAMENTO REALIZADO COM SUCESSO\n')