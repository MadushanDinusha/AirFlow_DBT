{{ config(materialized='incremental',
  schema='datavault',
  unique_key='link_id',
) }}

WITH hub_data AS (
  SELECT
    security_id
  FROM datavault.hub_security
),

sat_data AS (
  SELECT
    sat_security_id,
    security_id
  FROM datavault.sat_security
),

link_data AS (
  SELECT
    nextval('datavault.link_security_id_seq') AS link_id,
    hub_data.security_id,
    sat_data.sat_security_id,
    current_timestamp AS load_date,
    'test' AS record_source
  FROM hub_data
  JOIN sat_data
    ON hub_data.security_id = sat_data.security_id
)


SELECT
  link_id,
  security_id,
  sat_security_id,
  load_date,
  record_source
FROM link_data
WHERE (security_id, sat_security_id) NOT IN (
  SELECT security_id, sat_security_id
  FROM {{ this }}
)
