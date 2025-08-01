-- Criação das tabelas dimensionais

CREATE TABLE IF NOT EXISTS dim_operadora (
    id SERIAL PRIMARY KEY,
    nome_grupo_economico TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_servico (
    id SERIAL PRIMARY KEY,
    nome_servico TEXT UNIQUE NOT NULL  -- Ex: SCM, SMP, STFC
);

CREATE TABLE IF NOT EXISTS dim_uf (
    id SERIAL PRIMARY KEY,
    sigla_uf CHAR(2) UNIQUE NOT NULL
);

-- Criação da tabela fato

CREATE TABLE IF NOT EXISTS fato_ida (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    id_dim_operadora INTEGER NOT NULL REFERENCES dim_operadora(id),
    id_dim_servico INTEGER NOT NULL REFERENCES dim_servico(id),
    id_dim_uf INTEGER NOT NULL REFERENCES dim_uf(id),
    resolvidas_5_dias_percentual NUMERIC(5,2)
);

-- Inserts iniciais (exemplares)

INSERT INTO dim_operadora (nome_grupo_economico) VALUES
('ALGAR'), ('CLARO'), ('OI'), ('TIM'), ('VIVO')
ON CONFLICT DO NOTHING;

INSERT INTO dim_servico (nome_servico) VALUES
('SCM'), ('SMP'), ('STFC')
ON CONFLICT DO NOTHING;

INSERT INTO dim_uf (sigla_uf) VALUES
('AC'), ('AL'), ('AM'), ('AP'), ('BA'), ('CE'), ('DF'), ('ES'), ('GO'),
('MA'), ('MG'), ('MS'), ('MT'), ('PA'), ('PB'), ('PE'), ('PI'), ('PR'),
('RJ'), ('RN'), ('RO'), ('RR'), ('RS'), ('SC'), ('SE'), ('SP'), ('TO')
ON CONFLICT DO NOTHING;

-- View de variação da taxa de resolução

CREATE OR REPLACE VIEW vw_variacao_ida AS
WITH base AS (
    SELECT
        data,
        o.nome_grupo_economico,
        resolvidas_5_dias_percentual,
        LAG(resolvidas_5_dias_percentual) OVER (PARTITION BY o.nome_grupo_economico ORDER BY data) AS valor_anterior
    FROM fato_ida f
    JOIN dim_operadora o ON f.id_dim_operadora = o.id
)
, variacoes AS (
    SELECT
        data,
        nome_grupo_economico,
        ((resolvidas_5_dias_percentual - valor_anterior) / NULLIF(valor_anterior, 0)) * 100 AS taxa_variacao
    FROM base
    WHERE valor_anterior IS NOT NULL
)
, media_variacao AS (
    SELECT
        data,
        AVG(taxa_variacao) AS taxa_variacao_media
    FROM variacoes
    GROUP BY data
)
SELECT
    v.data,
    m.taxa_variacao_media,
    v.nome_grupo_economico,
    v.taxa_variacao,
    (v.taxa_variacao - m.taxa_variacao_media) AS diferenca_da_media
FROM variacoes v
JOIN media_variacao m ON v.data = m.data;
