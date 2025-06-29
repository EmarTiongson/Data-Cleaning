-- DATASET USED
-- https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training
-- Imported all data as TEXT first to avoid row import failure from missing or incorrect data
CREATE TABLE dirty_sales(
	transaction_id TEXT,
    item TEXT,
    quantity TEXT,
    price_per_unit TEXT,
    total_spent TEXT,
    payment_method TEXT,
    location TEXT,
    transaction_date TEXT
);

-- find and place the file here
SHOW VARIABLES LIKE "secure_file_priv";

-- load the file to import
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dirty_cafe_sales.csv'
INTO TABLE dirty_sales
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Create Staging Table
CREATE TABLE dirty_sales_staging
SELECT *
FROM dirty_sales;

-- Create temp table for prices
CREATE TEMPORARY TABLE prices
(
	item VARCHAR(20),
    price DECIMAL(3,1)
);

INSERT INTO prices
VALUES('Coffee', 2.0),
	  ('Tea', 1.5),
      ('Sandwich', 4.0),
      ('Salad', 5.0),
      ('Cake', 3.0),
      ('Cookie',1.0),
      ('Smoothie',4.0),
      ('Juice',3.0);

-- CHECK FOR DUPLICATE TRANSACTIONS
SELECT transaction_id, COUNT(transaction_id)
FROM dirty_sales_staging
GROUP BY transaction_id
HAVING COUNT(transaction_id) > 1;
-- Standardize Data
-- All of the columns except transaction_id consists of blanks, 'ERROR' and 'UNKNOWN'
-- Change those inconsistencies to NULL
UPDATE dirty_sales_staging
SET item = NULL
WHERE item IN ('0', 'ERROR', 'UNKNOWN', '');

UPDATE dirty_sales_staging
SET quantity = NULL
WHERE quantity IN ('0', 'ERROR', 'UNKNOWN', '');

UPDATE dirty_sales_staging
SET price_per_unit = NULL
WHERE price_per_unit IN ('0', 'ERROR', 'UNKNOWN', '');

UPDATE dirty_sales_staging
SET total_spent = NULL
WHERE total_spent IN ('0', 'ERROR', 'UNKNOWN', '');

UPDATE dirty_sales_staging
SET payment_method = NULL
WHERE payment_method IN ('0', 'ERROR', 'UNKNOWN', '');

UPDATE dirty_sales_staging
SET location = NULL
WHERE location IN ('0', 'ERROR', 'UNKNOWN', '');

-- hidden carriage return because last column
UPDATE dirty_sales_staging
SET transaction_date = NULL
WHERE transaction_date IN ('ERROR\r', 'UNKNOWN\r', '\r');

-- Change the columns to appropriate data type
-- Get the max length of value for varchar columns to avoid allocating unnecessary space
ALTER TABLE dirty_sales_staging
MODIFY COLUMN transaction_id VARCHAR(20),
MODIFY COLUMN item VARCHAR(20),
MODIFY COLUMN quantity INT,
-- up to 3 digits with 1 decimal
MODIFY COLUMN price_per_unit DECIMAL(3,1),
MODIFY COLUMN total_spent DECIMAL(3,1),
MODIFY COLUMN payment_method VARCHAR(20),
MODIFY COLUMN location VARCHAR(20),
MODIFY COLUMN transaction_date DATE;
            
-- Remove these rows as it is impossible to populate
DELETE
FROM dirty_sales_staging
WHERE quantity IS NULL
AND total_spent IS NULL;

DELETE
FROM dirty_sales_staging
WHERE item IS NULL
AND quantity IS NULL
AND price_per_unit IS NULL;

DELETE
FROM dirty_sales_staging
WHERE item IS NULL
AND total_spent IS NULL
AND price_per_unit IS NULL;

-- Populate rows when possible
UPDATE dirty_sales_staging
SET quantity = total_spent / price_per_unit
WHERE quantity IS NULL
AND price_per_unit IS NOT NULL
AND total_spent IS NOT NULL;

UPDATE dirty_sales_staging
SET price_per_unit = total_spent / quantity
WHERE price_per_unit IS NULL
AND quantity IS NOT NULL
AND total_spent IS NOT NULL;

UPDATE dirty_sales_staging
SET total_spent = quantity * price_per_unit
WHERE total_spent IS NULL
AND quantity IS NOT NULL
AND price_per_unit IS NOT NULL;

-- 
-- get the prices that appear more than once in the table
WITH ambiguous_prices AS 
(
SELECT price
FROM prices
GROUP BY price
HAVING COUNT(price) > 1
)
-- if a.price is null - meaning that a.price is not ambiguous
-- find and populate the null items whose price_per_unit is not ambiguous
UPDATE dirty_sales_staging s
JOIN prices p 
ON p.price = s.price_per_unit
LEFT JOIN ambiguous_prices a
ON p.price = a.price
SET s.item = p.item
WHERE s.item IS NULL
AND a.price IS NULL;



-- find and populate NULL price_per_unit based on item
UPDATE dirty_sales_staging s
JOIN prices p
ON p.item = s.item
SET s.price_per_unit = p.price
WHERE s.price_per_unit IS NULL;


-- Create another table for further cleaning, leave the dirty_sales_staging behind to serve as a checkpoint
CREATE TABLE dirty_sales_staging2
SELECT * FROM dirty_sales_staging;


-- add date_imputed column to keep track of rows whose values are populated from other rows
ALTER TABLE dirty_sales_staging2
ADD COLUMN date_imputed BOOLEAN DEFAULT FALSE;

-- Fill null transaction dates based previous dates based on transaction id
WITH prev_filler AS
(
SELECT transaction_id, transaction_date,
LAG(transaction_date) OVER (ORDER BY transaction_id) AS prev_date
FROM dirty_sales_staging2
)
UPDATE dirty_sales_staging2 as s2
JOIN prev_filler as p
ON p.transaction_id = s2.transaction_id
SET s2.transaction_date = p.prev_date,
	s2.date_imputed = TRUE
WHERE s2.transaction_date IS NULL
AND p.prev_date IS NOT NULL;

-- Fill remaining NULL transaction date with the date from the next row
WITH next_filler AS
(
SELECT transaction_id, transaction_date,
LEAD(transaction_date) OVER(ORDER BY transaction_id) AS next_date
FROM dirty_sales_staging2
)
UPDATE dirty_sales_staging2 s2
JOIN next_filler n
ON n.transaction_id = s2.transaction_id
SET s2.transaction_date = n.next_date,
	s2.date_imputed = TRUE
WHERE s2.transaction_date IS NULL
AND n.next_date IS NOT NULL;


-- add item_imputed column for populating item column based on different factors
ALTER TABLE dirty_sales_staging2 ADD COLUMN item_imputed BOOLEAN DEFAULT FALSE;
-- USE MODE IMPUTATION
-- Look for pattern of what payment method an item is mostly bought with
-- get count for each combination of item based on group by
-- get the sum of all the combination with same price per unit and payment method
-- divide to get the percentage
-- rank to see which item is most common per price_per_unit and payment_method
-- get the difference of rank 1 and 2 to see the difference
WITH pers AS
(
SELECT price_per_unit, payment_method, item, COUNT(*) AS cnt,
ROUND( 
100.0 * COUNT(*) /
SUM(COUNT(*)) OVER(PARTITION BY price_per_unit, payment_method), 2) AS Percentage,
RANK() OVER(PARTITION BY payment_method,price_per_unit ORDER BY COUNT(*) DESC) AS Ranking
FROM dirty_sales_staging2
WHERE item IS NOT NULL
AND price_per_unit IN (3.0, 4.0)
AND payment_method IS NOT NULL
GROUP BY price_per_unit, payment_method, item
ORDER BY price_per_unit, payment_method, cnt DESC
),
 Sec AS
 (
 SELECT *,
LEAD(Percentage) OVER(PARTITION BY payment_method,price_per_unit ORDER BY cnt) AS Difference_Percentage
FROM pers
GROUP BY price_per_unit, payment_method, item
ORDER BY price_per_unit, payment_method, cnt DESC)

SELECT price_per_unit, payment_method, item, cnt, Percentage, Ranking,
Percentage - Difference_Percentage AS Diference_of_Percentage
FROM Sec;

-- As shown, the minimum difference of percentage per rank is between 0.20% - 7.16%  which is a very unstable threshold
-- for mode imputation, proceeding with mode imputation with threshold below 10% could introduce bias and is not recommended.
-- Due to lack of data that correlates with items, I decided to label the Null items with ambiguous price_per_unit as 'Ambiguous'

UPDATE dirty_sales_staging2 
SET item = 'Ambiguous'
WHERE item IS NULL;

-- try to impute payment method using mode imputation
-- get the frequency of payment_method per location
-- partitions by location = impute payment method
WITH payment_percentage AS
(SELECT location, payment_method, COUNT(*) AS cnt,
ROUND( 
100.0 * COUNT(*) /
SUM(COUNT(*)) OVER(PARTITION BY  location), 2) AS Percentage,
RANK() OVER(PARTITION BY location ORDER BY COUNT(*) DESC) AS Ranking
FROM dirty_sales_staging2
WHERE location IS NOT NULL
AND payment_method IS NOT NULL
GROUP BY location, payment_method
),
diff_percentage AS
(
	SELECT *,
    LEAD(Percentage,1) OVER(PARTITION BY location ORDER BY Ranking) AS Rank2_diff,
    LEAD(Percentage,2) OVER(PARTITION BY location ORDER BY Ranking) AS Rank3_diff
    FROM payment_percentage
),
Final_diff AS
(
SELECT location, payment_method, cnt, Percentage, Ranking,
CASE
	WHEN Ranking = 1 THEN Percentage - Rank2_diff 
    END AS gap_of_1_and_2,
CASE
    WHEN Ranking = 1 THEN Percentage - Rank3_diff
	END AS gap_of_1_and_3
FROM diff_percentage
)
SELECT *
FROM Final_diff
ORDER BY location, Ranking;

-- Here we can see just like in the Null items that the threshold is between 0.39% - 3.77% which is very unstable
-- We cannot use mode imputation as the values are very close to each other
-- We will set NULL payment_method to 'UNKNOWN'

UPDATE dirty_sales_staging2
SET payment_method = 'UNKNOWN'
WHERE payment_method IS NULL;

-- Try to impute location using the same logic
-- partitions by payment_method = impute location
WITH location_percentage AS
(
SELECT payment_method, location, COUNT(*) AS cnt,
ROUND( 
100.0 * COUNT(*) /
SUM(COUNT(*)) OVER(PARTITION BY payment_method), 2) AS Percentage,
RANK() OVER(PARTITION BY payment_method ORDER BY COUNT(*) DESC) AS Ranking
FROM dirty_sales_staging2
WHERE payment_method != 'UNKNOWN'
AND location IS NOT NULL
GROUP BY payment_method, location
),
 diffs AS
 (
	SELECT *,
    LEAD(Percentage,1) OVER(PARTITION BY payment_method ORDER BY Ranking) AS Rank2_diff
    FROM location_percentage
),
Final_diff AS
(
SELECT payment_method, location, cnt, Percentage, Ranking,
CASE
	WHEN Ranking = 1 THEN Percentage - Rank2_diff 
    END AS gap_of_1_and_2
FROM diffs
)
SELECT *
FROM Final_diff
ORDER BY payment_method, Ranking;

-- The result of the analyis of most location type per payment method is also a low threshold which cannot be used for
-- mode imputation, note that the analysis excluded those with payment method = UNKNOWN as they will introduce bias, those
-- included in the analysis are rows with payment method that have valid value from the original dataset.
-- We will set NULL locations to UNKNOWN

UPDATE dirty_sales_staging2
SET location = 'UNKNOWN'
WHERE location IS NULL;


-- Check for NULL values or rows that were overlooked.
SELECT *
FROM dirty_sales_staging2
WHERE item IS NULL
OR quantity IS NULL
OR price_per_unit IS NULL
OR total_spent IS NULL
OR payment_method IS NULL
OR location IS NULL
OR transaction_date IS NULL;

-- Create final table
CREATE TABLE cleaned_sales
SELECT * FROM dirty_sales_staging2;
 
SELECT *
FROM cleaned_sales;
