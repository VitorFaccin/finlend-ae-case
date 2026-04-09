SELECT
    SAFE_CAST(transaction_id AS STRING)          AS transaction_id,
    SAFE_CAST(merchant_id AS STRING)             AS merchant_id,
    SAFE_CAST(customer_id AS STRING)             AS customer_id,
    SAFE_CAST(amount_cents AS INT64)             AS amount_cents,
    SAFE_CAST(amount_cents AS NUMERIC) / 100     AS amount_brl,
    SAFE_CAST(status AS STRING)                  AS status,
    SAFE_CAST(payment_method AS STRING)          AS payment_method,
    SAFE_CAST(created_at AS TIMESTAMP)           AS created_at,
    SAFE_CAST(updated_at AS TIMESTAMP)           AS updated_at,
    DATE(created_at)                             AS transaction_date,
    metadata
FROM {{ source('raw', 'transactions') }}
WHERE status != 'test'
