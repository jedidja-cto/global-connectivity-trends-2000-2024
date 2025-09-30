{{
    config(
        materialized='incremental',
        unique_key=['Country_Code', 'Year', 'Indicator_Code']
    )
}}

-- Stage the cleaned connectivity data
WITH source_data AS (
    SELECT
        Country,
        Country_Code,
        Year,
        Value,
        UNIT_MEASURE,
        UNIT_TYPE,
        OBS_STATUS,
        Indicator,
        Indicator_Code,
        connection_type,
        processed_at
    FROM {{ source('raw', 'cleaned_data') }}
    
    {% if is_incremental() %}
    -- Only process new/updated records when running incrementally
    WHERE processed_at > (SELECT max(processed_at) FROM {{ this }})
    {% endif %}
)

SELECT
    Country,
    Country_Code,
    Year,
    Value as connectivity_percentage,
    UNIT_MEASURE,
    UNIT_TYPE,
    OBS_STATUS as observation_status,
    Indicator,
    Indicator_Code,
    connection_type,
    processed_at,
    current_timestamp() as loaded_at
FROM source_data