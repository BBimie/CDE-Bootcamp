/* Find a list of order IDs where either gloss_qty or poster_qty is greater than 4000. Only include the id field in the resulting table. */

SELECT id FROM orders 
    WHERE poster_qty > 4000 OR gloss_qty > 4000;


/* Write a query that returns a list of orders where the standard_qty is zero and either the gloss_qty or poster_qty is over 1000. */
SELECT * FROM orders 
    WHERE standard_qty = 0 
    AND (poster_qty > 1000 OR gloss_qty > 1000);

/* Find all the company names that start with a 'C' or 'W', and where the primary contact contains 'ana' or 'Ana', but does not contain 'eana'. */

SELECT name AS company_name, primary_poc FROM accounts 
    WHERE (name LIKE 'C%' OR name LIKE 'W%') 
        AND (primary_poc LIKE '%ana%' OR primary_poc LIKE '%Ana%') 
        AND primary_poc NOT LIKE '%eana%';


/* Provide a table that shows the region for each sales rep along with their associated accounts. Your final table should include three columns: the region name, the sales rep name, and the account name. Sort the accounts alphabetically (A-Z) by account name. */

SELECT r.name AS region_name, 
			 sr.name AS sales_rep_name, 
			 a.name AS account_name
		FROM sales_reps sr 
	JOIN region r ON r.id = sr.region_id
	LEFT JOIN accounts a ON a.sales_rep_id = sr.id
	ORDER BY account_name;