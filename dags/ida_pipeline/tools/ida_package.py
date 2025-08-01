# ARQUIVO CONTENDO AS INFORMAÇÕES BASES PARA EXTRAÇÃO DO SITE


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


URL_DOWNLOAD = "https://dados.gov.br/api/publico/recurso/registrar-download"

PAYLOAD_DOWNLOAD = {
    "id": "",
    "idConjuntoDados": "",
    "titulo": "",
    "descricao": "",
    "link": "",
    "formato": "ods",
    "tipo": "",
}
