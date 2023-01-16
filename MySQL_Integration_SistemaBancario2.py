# Importação dos módulos
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine as ce
from sqlalchemy.orm import Session
import os
from sqlalchemy import select


# Criação das classes
Base = declarative_base()


class Cliente(Base):
    __tablename__ = 'tbl_cliente'

    cpf = Column(String(11), primary_key=True)
    nome_completo = Column(String(30))

    contas = relationship('Conta', back_populates='cliente')

    def __repr__(self):
        return f'Cliente(CPF={self.cpf!r}, Nome completo={self.nome_completo!r})'


class Conta(Base):
    __tablename__ = 'tbl_conta'

    id = Column(Integer, primary_key=True)
    tipo = Column(String(20), default='Conta Corrente')
    agencia = Column(String(4))
    conta = Column(String(3))
    cliente_cpf = Column(String(11), ForeignKey('tbl_cliente.cpf'), nullable=False)
    saldo = Column(Float, nullable=False)

   # cliente_id = Column(Integer, ForeignKey('tbl_cliente.id'), nullable=False)

    cliente = relationship('Cliente', back_populates='contas')

    def ler_saldo(cpf,agencia,conta): # static method
        with open(f'C:/Users/Falcone/Documents/0_DIO/IntegracaoPython_BancoDados/IntegrationWithSQL/Contas/{cpf}_{agencia}_{conta}.txt','r') as file:
                for line in file:
                    if 'Saldo' in line:
                        dividir_linha=line.split(' ')
                        saldo=dividir_linha[-1]
                        saldo_formatado=saldo
                        saldo=float(saldo.replace('R$',''))
                print('### Saldo atual:',saldo_formatado)
                return saldo

    def __repr__(self):
        return f'Conta(id={self.id!r}, Tipo={self.tipo!r}, Agencia={self.agencia!r}, ' \
               f'Conta={self.conta}, CPF={self.cliente_cpf}, Saldo={self.saldo!r})'


# Conexão com a banco de dados MySQL
engine = ce('mysql+pymysql://root:senha@localhost:3306/db_sistemabancario', echo=True, future=True)
Base.metadata.create_all(engine)


# Carregamento das informações nas tabelas do banco de dados
# tabela cliente (tbl_cliente)
with Session(engine) as session:
    with open('Clientes_Cadastrados.txt', 'r') as file:
        next(file) # pular a leitura do cabeçalho
        lista_cpfs=[]
        for line in file:
            usuario = line.split(',')
            cliente = Cliente(cpf=usuario[0], nome_completo=usuario[1])
            session.add_all([cliente])
            session.commit()
            lista_cpfs.append(usuario[0])
        lista_cpfs.pop(0)

# tabela conta (tbl_conta)
for files in os.walk('C:/Users/Falcone/Documents/0_DIO/IntegracaoPython_BancoDados/IntegrationWithSQL/Contas'):
    pass
with Session(engine) as session:
    for i in range(len(files[2])):
        nome_arquivo = files[2][i]
        nome_arquivo = nome_arquivo.split('_')
        num_cpf = nome_arquivo[0]
        num_agencia = nome_arquivo[1]
        num_conta = nome_arquivo[2]
        num_conta = num_conta.split('.')
        num_conta = num_conta[0]
        # print(num_cpf,num_agencia,num_conta)

        valor_saldo = Conta.ler_saldo(num_cpf,num_agencia,num_conta)
        conta = Conta(agencia=num_agencia, conta=num_conta, cliente_cpf=num_cpf,saldo=valor_saldo)
        session.add_all([conta])
        session.commit()


# recuperação das informações
session = Session(engine)

print('\nRESULTADO DA BUSCA SIMPLES POR NOME COMPLETO ')
stmt = select(Cliente).where(Cliente.nome_completo.in_(['carlos falcone']))
resultado = session.scalars(stmt).one()
print(resultado)

print('\nRESULTADO DA BUSCA POR CPF')
stmt = (select(Conta).join(Conta.cliente).where(Cliente.cpf == '04208661674'))
resultado = session.scalars(stmt).all()
print(resultado)

print('\nRESULTADO DA BUSCA POR NOME COMPLETO 1')
stmt = (select(Conta).join(Conta.cliente).where(Cliente.nome_completo == 'giovana falcone'))
resultado = session.scalars(stmt).all()
print(resultado)

print('\nRESULTADO BUSCA NOME COMPLETO 2')
stmt = (select(Conta).join(Conta.cliente).where(Cliente.nome_completo == 'stella falcone'))
resultado = session.scalars(stmt).all()
print(resultado)

session.close()