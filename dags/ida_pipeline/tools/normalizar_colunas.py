import pandas as pd
import re


def normalizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas do DataFrame.

    Args:
        df: DataFrame com colunas a serem normalizadas

    Returns:
        DataFrame com colunas normalizadas
    """
    df_normalizado = df.copy()

    # Normalizar nomes das colunas
    colunas_normalizadas = []
    for coluna in df_normalizado.columns:
        # Converter para lowercase
        coluna_norm = str(coluna).lower()

        # Remover acentos e caracteres especiais
        coluna_norm = re.sub(r"[àáâãäå]", "a", coluna_norm)
        coluna_norm = re.sub(r"[èéêë]", "e", coluna_norm)
        coluna_norm = re.sub(r"[ìíîï]", "i", coluna_norm)
        coluna_norm = re.sub(r"[òóôõö]", "o", coluna_norm)
        coluna_norm = re.sub(r"[ùúûü]", "u", coluna_norm)
        coluna_norm = re.sub(r"[ç]", "c", coluna_norm)
        coluna_norm = re.sub(r"[ñ]", "n", coluna_norm)

        # Substituir espaços e caracteres especiais por underscore
        coluna_norm = re.sub(r"[\s\-\.\/\\]+", "_", coluna_norm)

        # Remover caracteres não alfanuméricos exceto underscore
        coluna_norm = re.sub(r"[^a-z0-9_]", "", coluna_norm)

        # Remover underscores múltiplos consecutivos
        coluna_norm = re.sub(r"_+", "_", coluna_norm)

        # Remover underscore no início e fim
        coluna_norm = coluna_norm.strip("_")

        # Garantir que não comece com número
        if coluna_norm and coluna_norm[0].isdigit():
            coluna_norm = "col_" + coluna_norm

        # Se ficou vazio, usar nome genérico
        if not coluna_norm:
            coluna_norm = f"col_{len(colunas_normalizadas)}"

        colunas_normalizadas.append(coluna_norm)

    df_normalizado.columns = colunas_normalizadas
    return df_normalizado
