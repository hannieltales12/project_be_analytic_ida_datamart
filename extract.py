import requests
import ezodf
import pandas as pd
from io import BytesIO

URL_BASE_IDA = "https://dados.gov.br/api/publico/conjuntos-dados/indice-desempenho-atendimento"


HEADERS_IDA = {
    "authority": "dados.gov.br",
    "method": "GET",
    "accept": "application/json, text/plain, */*",
    "scheme": "https",
    "path": "/dados/conjuntos-dados/indice-desempenho-atendimento",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7,es-US;q=0.6,es;q=0.5",
    "cache-control": "max-age=0",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}


response_base = requests.get(URL_BASE_IDA, headers=HEADERS_IDA)

teste = response_base.json()

resources_list = teste.get("resources")

lista = []

for resource in response_base.json().get("resources"):
    lista.append(resource.get("recursoForm"))


for resource_download in lista:

    url_download_csv = resource_download.get("link")

    if url_download_csv:

        link_corrigido = url_download_csv.replace("\\", "/")

        response_download = requests.get(link_corrigido)
        # Verifique se a requisição foi bem-sucedida
        if response_download.status_code == 200:
            # Carregar arquivo ODS direto dos bytes
            # Ler ODS dos bytes
            arquivo = BytesIO(response_download.content)
            planilha = ezodf.opendoc(arquivo)

            # Seleciona a primeira aba
            aba = planilha.sheets[0]

            # Converte para DataFrame
            data = []
            for row in aba.rows():
                data.append([cell.value for cell in row])

            df = pd.DataFrame(data)