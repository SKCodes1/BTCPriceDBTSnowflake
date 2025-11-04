{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'HASH_KEY'
) }}

SELECT
  *
FROM {{ source('BTC', 'BTC') }}

{% if is_incremental() %}
WHERE BLOCK_TIMESTAMP >= (
  SELECT max(BLOCK_TIMESTAMP)
  FROM {{ this }}
)
{% endif %}
