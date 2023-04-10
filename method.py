import pandas as pd
import scrap
import chardet
import sqlite3 as sql
import numpy as np
 
#Retorna encoding de um CSV
def DescobreEncoding(csv):
    with open(csv, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

#Primeira atividade requisitada em aula
def PrimeiraAtividade() :
    req = input("Deseja baixar os dados?\nS - 1\nN - 2\n")
    if req == 1: 
        scrap.requisitar()
    
    x = pd.read_csv("MICRODADOS.csv", encoding='latin-1', sep=";", low_memory=False)
    x.query('Municipio == "CARIACICA"\
            #and ComorbidadeTabagismo =="Sim"\
            #and Evolucao == "Óbito pelo COVID-19"\
            #and DataObito > "2019"', inplace = True)
    print(x)

#C1
def CriaDb():

    x = pd.read_csv("MICRODADOS.csv", encoding='latin-1', sep=";", low_memory=False)

    dim_data = x[['DataNotificacao', 'DataCadastro', 'DataDiagnostico', 'DataColeta_RT_PCR',
                'DataColetaTesteRapido', 'DataColetaSorologia', 'DataColetaSorologiaIGG',
                'DataEncerramento', 'DataObito']].drop_duplicates().reset_index(drop=True)


    dim_paciente = x[['FaixaEtaria', 'IdadeNaDataNotificacao', 'Sexo', 'RacaCor', 'Escolaridade',
                    'Gestante', 'PossuiDeficiencia', 'MoradorDeRua', 'ProfissionalSaude']].drop_duplicates().reset_index(drop=True)


    dim_municipio = x[['Municipio', 'Bairro']].drop_duplicates().reset_index(drop=True)


    dim_comorbidades = x[['ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal',
                        'ComorbidadeDiabetes', 'ComorbidadeTabagismo', 'ComorbidadeObesidade']].drop_duplicates().reset_index(drop=True)


    dim_viagens = x[['ViagemBrasil', 'ViagemInternacional']].drop_duplicates().reset_index(drop=True)


    fatos = x[['DataNotificacao', 'Classificacao', 'Evolucao', 'CriterioConfirmacao',
                    'StatusNotificacao', 'Municipio', 'FaixaEtaria', 'IdadeNaDataNotificacao',
                    'Sexo', 'RacaCor', 'Escolaridade', 'Gestante', 'Febre', 'DificuldadeRespiratoria',
                    'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia', 'ComorbidadePulmao',
                    'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 'ComorbidadeTabagismo',
                    'ComorbidadeObesidade', 'FicouInternado', 'ViagemBrasil', 'ViagemInternacional',
                    'ProfissionalSaude', 'PossuiDeficiencia', 'MoradorDeRua', 'ResultadoRT_PCR',
                    'ResultadoTesteRapido']].reset_index(drop=True)

    connection = sql.connect('COVIDBI.db')
        

    x.to_sql('fatos', connection, if_exists='replace', index=False)
    x.to_sql('dim_data', connection, if_exists='replace', index=False)
    x.to_sql('dim_paciente', connection, if_exists='replace', index=False)
    x.to_sql('dim_municipio', connection, if_exists='replace', index=False)
    x.to_sql('dim_comorbidades', connection, if_exists='replace', index=False)
    x.to_sql('dim_viagens', connection, if_exists='replace', index=False)

