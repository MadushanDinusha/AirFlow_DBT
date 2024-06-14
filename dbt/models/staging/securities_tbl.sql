{{ config(materialized='table') }}

WITH raw_seed AS (
    SELECT
        attbr_xml,attbr_json
    FROM {{ source('xml_data', 'xml_data') }}
),
parsed_xml AS (
    SELECT
        (xpath('/genericSMF/primaryAssetId/text()', attbr_xml::xml))[1]::text AS security_id,
        (xpath('/genericSMF/primaryAssetType/text()', attbr_xml::xml))[1]::text AS security_type,
        (xpath('/genericSMF/effectiveDate/text()',attbr_xml::xml))[1]::text AS issue_date,
        attbr_xml,
        attbr_json
    FROM raw_seed
)
SELECT
    security_id,
    security_type,
    issue_date,
    CAST(attbr_xml AS xml) as attrb_xml ,
    CAST(attbr_json AS jsonb) as attbr_json,
    current_timestamp AS load_timestamp
    
FROM parsed_xml