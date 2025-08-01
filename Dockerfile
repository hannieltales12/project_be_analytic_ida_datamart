FROM python:3.11.12-bookworm

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# 1. Instalar dependências de sistema necessárias para ezodf e odfpy
RUN apt-get update && apt-get install -y \
    libxml2 \
    libxslt1.1 \
    libxslt1-dev \
    libxslt-dev \
    build-essential \
    libffi-dev \
    libssl-dev \
    libjpeg-dev \
    zlib1g-dev

# 2. Instalar pacotes Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt


# 3. Copiar seus arquivos de projeto
COPY dags/ dags/
COPY sql/ sql/
