import requests
import pandas as pd
import pyexcel_ods3
import os
from io import BytesIO

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ida_pipeline.tools.ida_package import (
    URL_BASE_IDA,
    HEADERS_IDA,
)


class LandingIDATask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context) -> None:
        """
        Executa a tarefa de extração e processamento dos dados do IDA.

        Realiza uma requisição na API do IDA, extrai os recursos ODS
        presentes no JSON de resposta e processa cada um deles para
        criar um DataFrame. Os DataFramess sãoosalvos em arquivos .parquet
        para uso posterior.

        Raises:
            Exception: Caso a requisição na API do IDA falhe
        """

        resources_json = self._extract_ida(URL_BASE_IDA, HEADERS_IDA)

        data_base = self._extract_data(resources_json)

        self._save_data(data_base)

    def _extract_ida(self, url_base: str, headers_ida: dict) -> dict:
        """Extrai dados da API do IDA

        Args:
            url_base (str): URL base da API do IDA
            headers_ida (dict): Headers para requisição

        Returns:
            dict: JSON response com dados do IDA
        """

        print("Realizando requisição na base do IDA")

        response_base = requests.get(url_base, headers=headers_ida)

        if response_base.status_code == 200:
            return response_base.json()
        else:
            raise Exception(
                f"Erro ao extrair dados do IDA: {response_base.status_code}"
            )

    def _extract_data(self, resources_json: dict) -> list:
        """Extrai e processa dados dos recursos ODS

        Args:
            resources_json (dict): JSON com informações dos recursos

        Returns:
            list: Lista com DataFrames processados
        """

        print("Extraindo recursos do JSON")

        resources_list = resources_json.get("resources", [])

        lista_resources = []
        for resource in resources_list:
            recurso_form = resource.get("recursoForm")
            if recurso_form:
                lista_resources.append(recurso_form)

        dataframes_list = []

        for resource_download in lista_resources:
            url_download_csv = resource_download.get("link")

            if url_download_csv:
                print(f"Processando arquivo: {url_download_csv}")

                # Corrige barras invertidas na URL
                link_corrigido = url_download_csv.replace("\\", "/")

                try:
                    # Download do arquivo
                    response_download = requests.get(link_corrigido)

                    if response_download.status_code == 200:
                        # Processa arquivo ODS
                        data = pyexcel_ods3.get_data(
                            BytesIO(response_download.content)
                        )
                        sheet_data = list(data.values())[0]
                        # Pula as 8 primeiras linhas — começa na 9ª, onde estão os nomes das colunas
                        sheet_data = sheet_data[8:]

                        # Remove linhas completamente vazias
                        sheet_data = [
                            row
                            for row in sheet_data
                            if any(
                                cell != "" and cell is not None for cell in row
                            )
                        ]

                        # Usa a primeira linha como cabeçalho completo (todas as colunas)
                        columns = sheet_data[0]

                        # Dados: a partir da linha seguinte
                        dados = [
                            row
                            for row in sheet_data[1:]
                            if len(row) >= len(columns)
                        ]

                        # Cria o DataFrame
                        df = pd.DataFrame(dados, columns=columns)

                        # Adiciona à lista
                        dataframes_list.append(df)

                        print(
                            f"Arquivo processado com sucesso. Linhas: {len(df)}"
                        )
                    else:
                        print(
                            f"Erro ao baixar arquivo: {response_download.status_code}"
                        )

                except Exception as e:
                    print(
                        f"Erro ao processar arquivo {url_download_csv}: {str(e)}"
                    )

        return dataframes_list

    def _save_data(self, dataframes_list: list) -> None:
        """
        Salva cada DataFrame da lista em arquivos .parquet para uso posterior.

        Args:
            dataframes_list (list): Lista de DataFrames a serem salvos.

        Cria um diretório especificado para armazenar os arquivos .parquet
        caso ele não exista. Cada DataFrame na lista é salvo como um arquivo .parquet
        nomeado sequencialmente. Imprime o caminho do arquivo salvo para confirmação.
        """

        output_dir = "/opt/airflow/data/landing"

        os.makedirs(output_dir, exist_ok=True)

        for idx, df in enumerate(dataframes_list):

            file_path = os.path.join(output_dir, f"ida_raw_{idx}.parquet")

            df.to_parquet(file_path, index=False)

            print(f"Arquivo salvo em: {file_path}")
