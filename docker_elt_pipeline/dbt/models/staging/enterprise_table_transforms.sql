SELECT
    "Year" AS survey_year,
    "Industry_code_NZSIOC" AS industry_code,
    "Industry_name_NZSIOC" AS industry_name,
    "Units" AS units,
    "Variable_code" AS variable_code,
    "Variable_name" AS variable_name,
    "Variable_category" AS variable_category,

    -- convert "Value" to a numeric type based on "Units"
    -- CASE
    --     WHEN "Units" = 'Dollars (millions)' THEN TRY_CAST("Value" AS DECIMAL(18, 2)) * 1000000
    --     WHEN "Units" = 'Dollars' THEN TRY_CAST("Value" AS DECIMAL(18, 2))
    --     ELSE TRY_CAST("Value" AS DECIMAL(18, 2)) -- Default for any other units
    -- END AS value
    CASE
    WHEN "Units" = 'Dollars (millions)' THEN CAST(NULLIF("Value", '') AS NUMERIC(18, 2)) * 1000000
    WHEN "Units" = 'Dollars' THEN CAST(NULLIF("Value", '') AS NUMERIC(18, 2))
    ELSE CAST(NULLIF("Value", '') AS NUMERIC(18, 2))
    END AS value


FROM {{ source('enterprise_survey', 'enterprise_survey_2023') }}
