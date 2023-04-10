import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

#Método para fazer Web Scrapping
def requisitar():
    response = requests.get('https://coronavirus.es.gov.br/painel-covid-19-es')
    html = BeautifulSoup(response.content, 'html.parser')
    download = html.find('a', href='https://bi.s3.es.gov.br/covid19/MICRODADOS.csv')
    file_url = download['href']

    response = requests.get(file_url, stream=True)
    file_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)

    with open('MICRODADOS.csv', 'wb') as f:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            f.write(data)

    progress_bar.close()


#Caso queria testar o método
#requisitar()
