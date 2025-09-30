{{
    config(
        materialized='table'
    )
}}

-- Analytics-ready model for connectivity data analysis
SELECT
    Country,
    Country_Code,
    Year,
    connectivity_percentage,
    connection_type,
    
    -- Add year-over-year growth calculation
    connectivity_percentage - LAG(connectivity_percentage) OVER (
        PARTITION BY Country_Code, connection_type 
        ORDER BY Year
    ) AS yoy_growth,
    
    -- Add percent change calculation
    (connectivity_percentage - LAG(connectivity_percentage) OVER (
        PARTITION BY Country_Code, connection_type 
        ORDER BY Year
    )) / NULLIF(LAG(connectivity_percentage) OVER (
        PARTITION BY Country_Code, connection_type 
        ORDER BY Year
    ), 0) * 100 AS yoy_percent_change,
    
    -- Add global average comparison
    connectivity_percentage - AVG(connectivity_percentage) OVER (
        PARTITION BY Year, connection_type
    ) AS diff_from_global_avg,
    
    -- Add regional grouping (simplified for now)
    CASE
        WHEN Country IN ('United States of America', 'Canada', 'Mexico') THEN 'North America'
        WHEN Country IN ('Brazil', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Venezuela') THEN 'South America'
        WHEN Country IN ('United Kingdom', 'France', 'Germany', 'Italy', 'Spain', 'Portugal', 'Netherlands', 
                         'Belgium', 'Switzerland', 'Austria', 'Sweden', 'Norway', 'Denmark', 'Finland') THEN 'Europe'
        WHEN Country IN ('China', 'Japan', 'South Korea', 'India', 'Indonesia', 'Malaysia', 
                         'Thailand', 'Vietnam', 'Philippines') THEN 'Asia'
        WHEN Country IN ('Nigeria', 'South Africa', 'Kenya', 'Egypt', 'Morocco', 'Algeria', 
                         'Ghana', 'Ethiopia', 'Tanzania', 'Uganda', 'Namibia') THEN 'Africa'
        WHEN Country IN ('Australia', 'New Zealand') THEN 'Oceania'
        ELSE 'Other'
    END AS region,
    
    observation_status,
    loaded_at
FROM {{ ref('stg_connectivity_data') }}