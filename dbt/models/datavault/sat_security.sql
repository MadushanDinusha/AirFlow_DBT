{{ config(materialized='incremental',
    schema='datavault',
    unique_key='sat_security_id',
) }}

WITH json_data AS (
    SELECT
        row_number() OVER () AS row_num,
        attbr_json::jsonb AS genericSMF_data
    FROM {{ source('securities', 'securities_tbl') }}
),


sec_data AS (
    SELECT
        json_data.row_num,
        hub_security.security_id,
        json_data.genericSMF_data
    FROM json_data
    LEFT JOIN datavault.hub_security
    ON hub_security.security_key = json_data.genericSMF_data -> 'genericSMF' ->> 'primaryAssetId'
)

SELECT 
    nextval('datavault.sat_security_id_seq') AS sat_security_id,
    sec_data.security_id AS security_id,
    current_timestamp AS load_date,
    sec_data.genericSMF_data -> 'genericSMF' ->> 'effectiveDate' AS effective_date,
    sec_data.genericSMF_data -> 'genericSMF' ->> 'issueName' AS security_name,
    sec_data.genericSMF_data AS attributes,
    'test' AS risk_profile,
    sec_data.genericSMF_data -> 'genericSMF' ->> 'sourceName' AS record_source

FROM 
    sec_data
WHERE
    sec_data.security_id IS NOT NULL
