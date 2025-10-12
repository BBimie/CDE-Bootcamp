
SELECT * ,
    (total_income - total_expenditure) AS operating_profit,
    (total_income - total_expenditure - indirect_taxes) AS profit_after_tax
FROM (
    SELECT
                survey_year,
                industry_code,
                industry_name,
                SUM(CASE WHEN variable_name = 'Total income' THEN value ELSE 0 END) AS total_income,
                SUM(CASE WHEN variable_name = 'Total expenditure' THEN value ELSE 0 END) AS total_expenditure,
                SUM(CASE WHEN variable_name = 'Indirect taxes' THEN value ELSE 0 END) AS indirect_taxes

    FROM {{ ref('enterprise_table_transforms') }}
    GROUP BY survey_year, industry_code, industry_name 
    ORDER BY survey_year
	) T 
