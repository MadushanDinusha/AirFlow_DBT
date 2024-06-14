{{ config(materialized='incremental',
    schema='datavault',
    unique_key='security_id',
) }}

WITH json_data AS (
  SELECT
    attbr_json::jsonb AS genericSMF_data
  FROM {{ source('securities', 'securities_tbl') }}
)
SELECT
  nextval('datavault.hub_security_id_seq') AS security_id,
  genericSMF_data -> 'genericSMF' ->> 'primaryAssetId' AS security_key,
  current_timestamp AS load_date,
  genericSMF_data -> 'genericSMF' ->> 'sourceName' AS record_source
FROM json_data
