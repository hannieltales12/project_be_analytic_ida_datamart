import os
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from ida_pipeline.tools.normalizar_colunas import normalizar_nomes_colunas


class RawIDATask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context) -> None:
        """
        Lê os arquivos da camada landing, aplica normalização de colunas
        e salva os arquivos tratados na camada raw.
        """

        input_dir = "/opt/airflow/data/landing"
        output_dir = "/opt/airflow/data/raw"

        dataframe = self._read_landing(input_dir)

        self._save_data(dataframe, output_dir)

    def _read_landing(self, input_dir: str) -> list:
        """
        Lê os arquivos da camada landing, aplica normalização de colunas
        e retorna lista de DataFrames processados.

        Args:
            input_dir (str): Caminho da camada landing.

        Returns:
            list: Lista de DataFrames normalizados.
        """
        dataframes_processados = []

        if os.path.exists(input_dir):
            print("Lendo arquivos da camada landing...")

            arquivos = [
                f for f in os.listdir(input_dir) if f.endswith(".parquet")
            ]

            if not arquivos:
                print("Nenhum arquivo encontrado na camada landing.")
                return dataframes_processados

            for idx, arquivo in enumerate(arquivos):
                path_entrada = os.path.join(input_dir, arquivo)
                df = pd.read_parquet(path_entrada)

                print(f"Processando arquivo: {arquivo} — Linhas: {len(df)}")

                # Aplicar normalização das colunas
                df_normalizado = normalizar_nomes_colunas(df)
                dataframes_processados.append(df_normalizado)

        return dataframes_processados

    def _save_data(self, dataframes_list: list, output_dir: str) -> None:
        """
        Salva cada DataFrame da lista em arquivos .parquet para uso posterior.

        Args:
            dataframes_list (list): Lista de DataFrames a serem salvos.
            output_dir (str): Caminho da camada raw.

        Cria um diretório especificado para armazenar os arquivos .parquet
        caso ele não exista. Cada DataFrame na lista é salvo como um arquivo .parquet
        nomeado sequencialmente. Imprime o caminho do arquivo salvo para confirmação.
        """

        output_dir = "/opt/airflow/data/raw"

        os.makedirs(output_dir, exist_ok=True)

        for idx, df in enumerate(dataframes_list):

            path_saida = os.path.join(output_dir, f"ida_raw_{idx}.parquet")
            df.to_parquet(path_saida, index=False)

            print(f"Arquivo salvo na camada raw: {path_saida}")
