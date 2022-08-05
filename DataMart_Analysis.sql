
USE Data_Mart;

# A) DATA CLEANSING

# In a single query, perform the following operations and generate a new table in 
# the data_mart schema named clean_weekly_sales:

# 1. Add a week_number as the second column for each week_date value, for 
# example any value from the 1st of January to 7th of January will be 1, 8th to 
# 14th will be 2, etc.

# 2. Add a month_number with the calendar month for each week_date value as 
# the 3rd column.

# 3. Add a calendar_year column as the 4th column containing either 2018, 2019 
# or 2020 values.

# 4. Add a new column called age_band after the original segment column using 
# the following mapping on the number inside the segment value.
# That is, 1 should signify 'Young Adults', 2 should signify 'Middle Aged' and 3 or 4 should signify
# 'Retirees'.

# 5. Add a new demographic column using the following mapping for the first 
# letter in the segment values:
# 'C': Couples; 'F': Families

# 6. Ensure all null string values with an "unknown" string value in the 
# original segment column as well as the new age_band and demographic columns.

# 7. Generate a new avg_transaction column as the sales value divided 
# by transactions rounded to 2 decimal places for each record.

CREATE TABLE clean_weekly_sales as
SELECT week_date,
week(week_date) as week_number,
month(week_date) as month_number,
year(week_date) as calendar_year,
region, platform, 
CASE
WHEN segment = null THEN 'Uknown'
ELSE segment
END AS segment,
CASE
WHEN right(segment,1) = '1' THEN 'Young Adults'
WHEN right(segment,1) = '2' THEN 'Middle Aged'
WHEN right(segment,1) in ('3','4') THEN 'Retirees'
ELSE 'Unknown'
END AS age_band,
CASE
WHEN left(segment,1) = 'C' THEN 'Couples'
WHEN left(segment,1) = 'F' THEN 'Families'
ELSE 'Unknown'
END AS demographic,
sales, transactions,
round(sales/transactions,2) as avg_transactions
FROM weekly_sales;

SELECT* FROM clean_weekly_sales LIMIT 10;


# B) DATA EXPLORATION

# 1. Which week numbers are missing from the dataset?

# There are 52 weeks in a year.
CREATE TABLE seq52(x int auto_increment primary key);
INSERT INTO seq52 VALUES (),(),(),(),(),(),(),(),(),();
INSERT INTO seq52 VALUES (),(),(),(),(),(),(),(),(),();
INSERT INTO seq52 VALUES (),(),(),(),(),(),(),(),(),();
INSERT INTO seq52 VALUES (),(),(),(),(),(),(),(),(),();
INSERT INTO seq52 VALUES (),(),(),(),(),(),(),(),(),();
INSERT INTO seq52 VALUES (),();

SELECT * FROM seq52;

SELECT DISTINCT x as missing_week_day FROM seq52
WHERE x NOT IN (SELECT DISTINCT week_number FROM clean_weekly_sales);

# 2. How many total transactions were there for each year in the dataset?

SELECT year(week_date) as Year, SUM(transactions) as 'Total Transactions' FROM weekly_sales
GROUP BY year(week_date);

# 3. What are the total sales for each region for each month?

SELECT monthname(week_date) as Month, region as Region, SUM(sales) as 'Total Sales' FROM weekly_sales
GROUP BY region, monthname(week_date)
ORDER BY month(week_date);

# 4. What is the total count of transactions for each platform?

SELECT platform as Platform, COUNT(transactions) as 'Count of Transactions for each platform'
FROM weekly_sales GROUP BY platform;

# 5. What is the percentage of sales for Retail vs Shopify for each month?

# Here, CTE = Common Table Expression
WITH cte_monthly_platform_sales AS (
  SELECT
    month_number,calendar_year,
    platform,
    SUM(sales) AS monthly_sales
  FROM clean_weekly_sales
  GROUP BY month_number,calendar_year, platform
)
SELECT
  month_number,calendar_year,
  ROUND(
    100 * MAX(CASE WHEN platform = 'Retail' THEN monthly_sales ELSE NULL END) /
      SUM(monthly_sales),
    2
  ) AS retail_percentage,
  ROUND(
    100 * MAX(CASE WHEN platform = 'Shopify' THEN monthly_sales ELSE NULL END) /
      SUM(monthly_sales),
    2
  ) AS shopify_percentage
FROM cte_monthly_platform_sales
GROUP BY month_number,calendar_year
ORDER BY month_number,calendar_year;

# 6. What is the percentage of sales by demographic for each year in the dataset?

SELECT
  calendar_year,
  demographic,
  SUM(SALES) AS yearly_sales,
  ROUND(
    (
      100 * SUM(sales)/
        SUM(SUM(SALES)) OVER (PARTITION BY demographic)
    ),
    2
  ) AS percentage
FROM clean_weekly_sales
GROUP BY
  calendar_year,
  demographic
ORDER BY
  calendar_year,
  demographic;
  
# 7. Which age_band and demographic values contribute the most to Retail sales?

SELECT age_band, demographic, SUM(sales) as 'Total Sales'
FROM clean_weekly_sales
WHERE platform='Retail'
GROUP BY age_band, demographic
ORDER BY sum(sales) DESC;