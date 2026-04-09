SELECT
    SAFE_CAST(id AS STRING)           AS merchant_id,
    SAFE_CAST(trade_name AS STRING)   AS trade_name,
    SAFE_CAST(mcc_code AS STRING)     AS mcc_code,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at
FROM {{ source('raw', 'merchants') }}
