{{
    config(
        materialized='table',
        tags=['metricflow']
    )
}}

-- Spine de datas requerida pelo MetricFlow para métricas com dimensão temporal.
-- O MetricFlow usa esta tabela internamente para calcular métricas por grain (day, month, etc.).
-- Para expandir o range histórico, ajuste a data de início abaixo.
SELECT date_day
FROM UNNEST(
    GENERATE_DATE_ARRAY(
        DATE '2020-01-01',
        CURRENT_DATE()
    )
) AS date_day
