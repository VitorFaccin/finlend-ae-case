{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        partition_by={
            "field": "settlement_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- var('lookback_days', 10): quantos dias retroativos processar nos settlements.
-- Janela customizada:   dbt run --vars '{"lookback_days": 30}'
-- Reprocessamento total: dbt run --vars '{"lookback_days": null}'
--                     OU dbt run --full-refresh
{% set lookback = var('lookback_days', 10) %}

WITH unnested AS (
    -- Único ponto do projeto que acessa raw.settlements diretamente.
    -- Transforma o array transaction_ids em uma linha por transação.
    SELECT
        s.settlement_id,
        SAFE_CAST(s.net_amount_cents AS NUMERIC) / 100  AS net_amount,
        SAFE_CAST(s.fee_amount_cents AS NUMERIC) / 100  AS fee_amount,
        s.settlement_date,
        s.paid_at,
        tid AS transaction_id
    FROM {{ source('raw', 'settlements') }} s
    CROSS JOIN UNNEST(s.transaction_ids) AS tid

    {% if is_incremental() and lookback is not none %}
    WHERE s.settlement_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ lookback }} DAY)
    {% endif %}
),

deduped AS (
    -- Uma linha por transaction_id. Uma transação pode aparecer em múltiplos lotes
    -- de settlement (reprocessamentos, correções). Mantemos apenas o mais recente.
    -- settlement_id DESC garante ordenação determinística em caso de empate por data.
    SELECT
        t.transaction_id,
        t.merchant_id,
        t.customer_id,
        t.amount_cents,
        t.amount_brl,
        t.status,
        t.payment_method,
        t.created_at,
        t.updated_at,
        t.transaction_date,
        t.metadata,
        u.settlement_id,
        u.net_amount,
        u.fee_amount,
        u.settlement_date,
        u.paid_at,
        ROW_NUMBER() OVER (
            PARTITION BY t.transaction_id
            ORDER BY u.settlement_date DESC, u.settlement_id DESC
        ) AS rn
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN unnested u USING (transaction_id)
    WHERE t.status IN ('captured', 'refunded', 'chargeback')
)

-- rn é um artefato interno do ROW_NUMBER; nunca expor para camadas downstream
SELECT * EXCEPT (rn)
FROM deduped
WHERE rn = 1
