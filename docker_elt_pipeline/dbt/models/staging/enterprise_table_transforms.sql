SELECT
    "Year" AS survey_year,
    "Industry_code_NZSIOC" AS industry_code,
    "Industry_name_NZSIOC" AS industry_name,
    "Units" AS units,
    "Variable_code" AS variable_code,
    "Variable_name" AS variable_name,
    "Variable_category" AS variable_category,

    CASE
        WHEN "Units" = 'Dollars (millions)' THEN CAST(REPLACE(NULLIF("Value", ''), ',', '') AS NUMERIC(18, 2)) * 1000000
        WHEN "Units" = 'Dollars' THEN CAST(REPLACE(NULLIF("Value", ''), ',', '') AS NUMERIC(18, 2))
        ELSE CAST(REPLACE(NULLIF("Value", ''), ',', '') AS NUMERIC(18, 2))
    END AS value


FROM {{ source('enterprise_survey', 'enterprise_survey_2023') }}
WHERE "Value" NOT IN ('C', 'S')
