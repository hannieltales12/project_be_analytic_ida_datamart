# project_be_analytic_ida_datamart

Este projeto cria um pipeline de dados com Airflow e PostgreSQL para normalização e visualização da variação de preços da IDA. O processo é dividido em três camadas:

- **Landing**: ingestão dos dados crus
- **Raw**: normalização e padronização
- **Datamart**: criação de uma view final para análise

## Tecnologias Utilizadas

- Python 3.10+
- Airflow
- PostgreSQL 15+
- Docker / Docker Compose

## Inicialização

Clone o repositório e suba os containers:

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
docker build -t airflow:project-be-analytic-ida-datamart .
docker-compose up -d
```

## Acesso ao Airflow
http://localhost:8080

Credenciais padrão:

Usuário: admin
Senha: admin

## Estrutura

Landing: realiza a ingestão dos dados brutos.

Raw: normaliza e padroniza os dados da landing, salvando em raw_ida.csv.

Datamart: define uma view SQL (vw_variacao_ida) que consolida os dados para visualização.