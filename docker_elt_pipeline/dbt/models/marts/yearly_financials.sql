
SELECT
    survey_year,
    industry_code,
    industry_name,

    -- Pivot each metric into its own column using a SUM(CASE WHEN...) expression
    SUM(CASE WHEN variable_name = 'Total income' THEN value ELSE 0 END) AS total_income,
    SUM(CASE WHEN variable_name = 'Total expenditure' THEN value ELSE 0 END) AS total_expenditure,
    SUM(CASE WHEN variable_name = 'Indirect taxes' THEN value ELSE 0 END) AS indirect_taxes,

    -- This is where the wide format becomes powerful: add calculated metrics
    (total_income - total_expenditure) AS operating_profit,
    (total_income - total_expenditure - indirect_taxes) AS profit_after_tax,
    

FROM
    {{ ref('enterprise_table_transforms') }}

GROUP BY
    survey_year,
    industry_code,
    industry_name