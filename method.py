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
    x = pd.read_csv("MICRODADOS.csv", encoding='latin-1', sep=";", low_memory=False)
    x.query('Municipio == "CARIACICA"\
            #and ComorbidadeTabagismo =="Sim"\
            #and Evolucao == "Óbito pelo COVID-19"\
            #and DataObito > "2019"', inplace = True)
    print(x)

#C1
def CriaDb():


    while True:
        req = input("Deseja baixar os dados?\nS - 1\nN - 2\n")
        if req == '1': 
            print("Iniciando download...")
            scrap.requisitar()
            break
        elif req == '2':
            print("Prosegguindo...")
            break
        else:
            print("Entrada inválida. Por favor, insira 1 ou 2.")

    print("Lendo CSV...")
    x = pd.read_csv("MICRODADOS.csv", encoding='latin-1', sep=";", low_memory=False)

    print("Criando dataframes...")
    dim_data = x[['DataNotificacao', 'DataCadastro', 'DataDiagnostico', 'DataColeta_RT_PCR',
                'DataColetaTesteRapido', 'DataColetaSorologia', 'DataColetaSorologiaIGG',
                'DataEncerramento', 'DataObito']].drop_duplicates().reset_index(drop=True)


    dim_paciente = x[['FaixaEtaria', 'IdadeNaDataNotificacao', 'Sexo', 'RacaCor', 'Escolaridade',
                    'Gestante', 'PossuiDeficiencia', 'MoradorDeRua', 'ProfissionalSaude']].drop_duplicates().reset_index(drop=True)


    dim_municipio = x[['Municipio', 'Bairro']].drop_duplicates().reset_index(drop=True)


    dim_comorbidades = x[['ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal',
                        'ComorbidadeDiabetes', 'ComorbidadeTabagismo', 'ComorbidadeObesidade']].drop_duplicates().reset_index(drop=True)


    dim_viagens = x[['ViagemBrasil', 'ViagemInternacional']].drop_duplicates().reset_index(drop=True)

    fatos = x[['Classificacao', 'Evolucao', 'CriterioConfirmacao']].reset_index(drop=True)

    print("Criando indexes...")

    fatos['data_id'] = fatos.reset_index().index
    fatos['paciente_id'] = fatos.reset_index().index
    fatos['municipio_id'] = fatos.reset_index().index
    fatos['comorbidades_id'] = fatos.reset_index().index
    fatos['viagens_id'] = fatos.reset_index().index

    dim_data['data_id'] = dim_data.index
    dim_paciente['paciente_id'] = dim_paciente.index
    dim_municipio['municipio_id'] = dim_municipio.index
    dim_comorbidades['comorbidades_id'] = dim_comorbidades.index
    dim_viagens['viagens_id'] = dim_viagens.index

    # Substitua as colunas de ID pelos IDs gerados pelas tabelas de dimensão
    fatos = fatos.merge(dim_data, on='data_id', how='left')
    fatos['data_id'] = fatos['data_id'].fillna(-1).astype(int)

    fatos = fatos.merge(dim_paciente, on='paciente_id', how='left')
    fatos['paciente_id'] = fatos['paciente_id'].fillna(-1).astype(int)

    fatos = fatos.merge(dim_municipio, on='municipio_id', how='left')
    fatos['municipio_id'] = fatos['municipio_id'].fillna(-1).astype(int)

    fatos = fatos.merge(dim_comorbidades, on='comorbidades_id', how='left')
    fatos['comorbidades_id'] = fatos['comorbidades_id'].fillna(-1).astype(int)

    fatos = fatos.merge(dim_viagens, on='viagens_id', how='left')
    fatos['viagens_id'] = fatos['viagens_id'].fillna(-1).astype(int)


    print("Criando merges(FKs)")
    fatos = fatos.merge(dim_data, on='data_id', how='left')

    fatos = fatos.merge(dim_paciente, on='paciente_id', how='left')

    fatos = fatos.merge(dim_municipio, on='municipio_id', how='left')

    fatos = fatos.merge(dim_comorbidades, on='comorbidades_id', how='left')

    fatos = fatos.merge(dim_viagens, on='viagens_id', how='left')

    print("Conectando...")
    connection = sql.connect('COVIDBI.db')
        

    x.to_sql('fatos', connection, if_exists='replace', index=False)
    x.to_sql('dim_data', connection, if_exists='replace', index=False)
    x.to_sql('dim_paciente', connection, if_exists='replace', index=False)
    x.to_sql('dim_municipio', connection, if_exists='replace', index=False)
    x.to_sql('dim_comorbidades', connection, if_exists='replace', index=False)
    x.to_sql('dim_viagens', connection, if_exists='replace', index=False)
    print("Concluído!")


