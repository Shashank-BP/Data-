use sales_analysis;

set sql_safe_updates = 0;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Creating a table and importing data from csv file
create table online_retail (
  InvoiceNo varchar(20),
  StockCode varchar(20),
  Description text,
  Quantity int,
  InvoiceDate datetime,
  UnitPrice decimal(8,2),
  CustomerID varchar(50) default null,
  Country varchar(50)
);

select * from online_retail;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Data/sales_analysis/online_retail.csv'
INTO TABLE online_retail
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

select count(*) from online_retail;
-- Result: The total count of the dataset loaded is 541909 rows. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DATA CLEANING
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1.Handling missing values
-- Check for null values 
select count(*) as Total_value,
	   sum(case when InvoiceNo is null then 1 else 0 end) as InvoiceNo_missing,
       sum(case when StockCode is null or StockCode = '' then 1 else 0 end) as StockCode_missing,
       sum(case when Description is null or Description = '' then 1 else 0 end) as Description_missing,
       sum(case when Quantity is null then 1 else 0 end) as Quantity_missing,
       sum(case when InvoiceDate is null then 1 else 0 end) as InvoiceDate_missing,
       sum(case when UnitPrice is null then 1 else 0 end) as UnitPrice_missing,
       sum(case when CustomerID = 0 then 1 else 0 end) as CustomerID_missing,
       sum(case when Country is null or Country = '' then 1 else 0 end) as Country_missing
from online_retail;
/* Result:Total_value=541909,InvoiceNo_missing=0,StockCode_missing=0,Description_missing=1445,Quantity_missing=0,InvoiceDate_missing=0,UnitPrice_missing=0,
CustomerID_missing=135080,Country_missing=0 */

-- Removing the null values
delete from online_retail where CustomerID = 0;
/* While looking at the number of null values in the dataset, it is interesting to note that almost 25% of the entries are not assigned to a particular customer. 
With the data available, it is impossible to impute values for the user and these entries are thus useless for the current exercise. So I delete them from the dataset. */

-- 2.Checking for duplicate values
select count(*) as Total_duplicates
from (
    select InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate, count(*) as Count
    from online_retail
    group by InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate
    having COUNT(*) > 1
) as temp;

-- Deleting the duplicate values
delete from online_retail
where (InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate) in (
    select InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate
    from (
        select InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate, 
               row_number() over(partition by InvoiceNo, CustomerId, Country, StockCode, Description, InvoiceDate order by InvoiceNo) as rn
        from online_retail
    ) as temp
    where rn > 1
);

-- 3.Check for inconsistent values
-- Check for inconsistent of country names
select distinct Country from online_retail
order by Country;

-- Check for outliers
select * from (
  select *,
         (Quantity - avg(Quantity) over()) / stddev(Quantity) over() as z_score
  from online_retail
) subquery
where z_score > 3 or z_score < -3;
-- Result:179 values of outlliers
select * from (
  select *,
         (UnitPrice - avg(UnitPrice) over()) / stddev(UnitPrice) over() as z_score
  from online_retail
) subquery
where z_score > 3 or z_score < -3;
-- Result:130 values of outliers

-- Deleting the outliers in Quantity column
delete from online_retail
where Quantity in (
  select Quantity
  from (
    select *,
           (Quantity - avg(Quantity) over()) / stddev(Quantity) over() as z_score
    from online_retail
  ) subquery
  where z_score > 3 or z_score < -3
);

-- Deleting the outliers in UnitPrice column
delete from online_retail
where UnitPrice in (
  select UnitPrice
  from (
    select *,
           (UnitPrice - avg(UnitPrice) over()) / stddev(UnitPrice) over() as z_score
    from online_retail
  ) subquery
  where z_score > 3 or z_score < -3
);
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- EXPLORATORY DATA ANALYSIS
-- Exploratory data analysis (EDA) is used by data scientists to analyze and investigate data sets and summarize their main characteristics.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Let's lokk at the detailed description of the dataset.
describe online_retail;
/* Overview: This dataframe contains 8 variables that correspond to:
*	InvoiceNo: Invoice number. *Nominal, a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation.*
*	StockCode: Product (item) code. *Nominal, a 5-digit integral number uniquely assigned to each distinct product.
*	Description: Product (item) name. Nominal.
*	Quantity: The quantities of each product (item) per transaction. Numeric.
*	InvoiceDate: Invoice Date and time. Numeric, the day and time when each transaction was generated.
*	UnitPrice: Unit price. Numeric, Product price per unit in sterling.
*	CustomerID: Customer number. Nominal, a 5-digit integral number uniquely assigned to each customer.
*	Country: Country name. Nominal*, the name of the country where each customer resides. */

-- 2. Let's check the number of customers, transactions, and items sold in the dataset.
-- a. Number of unique customers.
select count(distinct CustomerID) as NumCustomers
from online_retail;
-- Result: There are 4356 unique records customers in the dataset.

--  b. Number of transactions.
select count(distinct InvoiceNo) as NumTransactions
from online_retail;
--  Result: There are 21982 unique records transactions in the dataset.

-- c. Number of items sold.
select sum(Quantity) as NumItemsSold
from online_retail;
-- Result: In total there are 4630045 items sold.

-- 3. Analyze the distribution of sales across different countries, time of the day, day of the week, and month of the year.
-- a. Sales distribution across different countries.
select Country, sum(Quantity*UnitPrice) as TotalSales
from online_retail
group by Country
order by TotalSales desc;
/*
Query summary: This query will group the sales data by country and calculate the total sales for each country, then sort the results in descending order by total sales.
Result: 
Total sales in each country are United Kingdom=6525542.84, Netherland=273346.98, EIRE=250447.48, Germany=220093.16, France=194765.68, Australia=134453.20, Spain=57412.76, 
Switzerland=55678.20, Belgium=40910.96,Sweden=36574.91, Norway=34443.56, Portugal=28750.62, Japan=26176.22, Finland=21521.14, Channel Islands=20066.49, Denmark=18581.89, 
Italy=16590.51, Cyprus=12579.33, Austria=10154.32, Singapore=9120.39,Poland=7213.14, Israel=6982.55, Greece=4710.52, Iceland=4310.00, Unspecified=2640.77, Malta=2505.47, 
United Arab Emirates=1902.28, USA=1730.92, Lebanon=1693.88, Lithuania=1661.06, European Community=1291.75,Brazil=1143.60, RSA=1002.31, Czech Republic=707.72, Saudi Arabia=131.17.
*/

-- b. Sales distribution by time of the day
select hour(InvoiceDate) as HourOfDay, sum(Quantity*UnitPrice) as TotalSales
from online_retail
group by HourOfDay
order by HourOfDay asc;
/*
Query summary: This query will group the sales data by hour of the day and calculate the total sales for each hour, then sort the results in ascending order by hour of the day.
Resutl: 
The total sales for each hour are 6=-497.35, 7=29376.84, 8=265859.47, 9=651293.74, 10=1110274.64, 11=1013277.00, 12=1288293.27, 13=1080950.30, 14=936643.41, 15=868288.44, 16=426855.25, 
17=208515.84, 18=92429.90, 19=43376.38, 20=15564.49
*/

-- c. Sales distribution by day of the week
select dayname(InvoiceDate) as DayOfWeek, sum(Quantity*UnitPrice) as TotalSales
from online_retail
group by DayOfWeek
order by field(DayOfWeek, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
/*
Query summary: This query will group the sales data by day of the week and calculate the total sales for each day, then sort the results in the order of days of the week.
Result: The total sales for each day are Monday=1259959.62, Tuesday=1479045.45, Wednesday=1498516.09, Thursday=1835752.90, Friday=1204687.11, Sunday=752540.45,
*/

-- d. Sales distribution by month of the year.
select monthname(InvoiceDate) as month, sum(Quantity*UnitPrice) as TotalSales
from online_retail
group by month
order by field(month, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December');
/*
Query summary: This query will group the sales data by month of the year and calculate the total sales for each month, then sort the results in the order of months of the year.
Results: 
The total sales for each month are January=439766.80, February=423042.99, March=566197.03, April=415802.32, May=637596.35, July=578894.89, September=899674.81, November=1084676.37, December=843284.74,
*/

-- 4. What is the percentage of cancelled Invoices.
select 
    count(case when InvoiceNo like 'C%' then InvoiceNo end) as Total_cancellation,
    count(case when InvoiceNo like 'C%' then InvoiceNo end) / count(distinct InvoiceNo) * 100 as Cancellation_percentage
from online_retail;
/* 
Query summary: This query counts the number of invoices that start with the letter 'C' and calculates the percentage of total invoices that are cancellations. 
The case statement is used to count only the invoices that start with 'C' and return null for all other invoices. 
The count function then counts the number of non-null values returned by the case statement. 
Result: 
Based on the results we foound that there is a large percentage of cancelled orders which is about 40%. More analysis on the cancelled orders can help in preventing future cacellation.
*/

-- 5. What are the top-10 selling items.
select StockCode, Description, sum(Quantity) as Total_Quantity, round(sum(Quantity * UnitPrice), 2) as Total_Sales from online_retail 
group by StockCode, Description
order by Total_Quantity desc, Total_Sales desc 
limit 10;
/*
Query summary: This query will group the sales data by StockCode and Description, and calculate the total quantity and sales for each item. It will then sort the results by 
total quantity and sales in descending order, and limit the output to the top 10 items.
Results: 
The top-selling items in the online retail dataset include Jumbo Bag Red Retrospot, World War 2 Gliders Asstd Designs, and White Hanging Heart T-Light Holder. 
These items have high total quantities sold and total sales values. This information can help identify popular products and guide marketing strategies to increase sales.
*/

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CUSTOMER SEGMENTATION
/* Customer segmentation is the process of dividing a customer base into distinct groups of individuals that have similar characteristics. This process makes it easier to 
target specific groups of customers with tailored products, services, and marketing strategies. By segmenting customers into different classes, businesses can better 
understand their needs, preferences, and buying patterns, allowing them to create more personalized and effective marketing campaigns. */
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Segmenting customers based on the Geography.
select Country, count(*) as Num_Customers
from online_retail
group by Country;
/*
Query summary: This query will count the number of customers in each country and group them by country. You can modify this query to group customers by state or city by replacing 
the "Country" field with the appropriate field in the "group by" clause.
Results: 
Based on the customer segmentation analysis, the majority of customers in the online retail dataset are from the United Kingdom. This information can be used to tailor 
marketing strategies and promotions specifically to customers in this region. Additionally, businesses can use this information to explore potential expansion opportunities 
in other regions with a smaller customer base. Overall, geographic segmentation provides valuable insights into customer behavior and preferences based on their location.
*/

-- Keeping geography in mind let us solve some questions
-- a. Group customers by country and calculate their total spending:
select Country, round(sum(Quantity * UnitPrice), 2) as TotalSpending
from online_retail
group by Country;
/*
Query summary: This query groups customers by country and calculates their total spending. 
Results: 
Based on the results, customers can be segmented into three categories: high-spending countries (UK, France, and Australia), moderate-spending countries (Netherlands, Germany, EIRE, and Switzerland),
and low-spending countries (Saudi Arabia, Czech Republic, and Brazil). Understanding these customer segments can help inform targeted marketing strategies to increase sales and revenue.
*/

-- b. Identify high-value customers in each country:
select t.Country, t.CustomerID, t.TotalSpending
from (
  select Country, CustomerID, round(sum(Quantity * UnitPrice), 2) as TotalSpending, 
         row_number() over (partition by Country order by round(sum(Quantity * UnitPrice), 2) desc) as rn
  from online_retail
  group by Country, CustomerID
  having TotalSpending > 1000
) t
where t.rn = 1;
/*
Query summary: This query helps identify high-value customers in each country by grouping customers by country and customer ID, and then selecting the customer with the highest total spending in each country. 
Results: Understanding these high-value customers is critical as they can provide the most significant revenue for a business and should be prioritized for targeted marketing and sales strategies.
*/

-- c. Segment customers based on their location and shopping behavior ( average order value, total spending):
select 
    Country, 
    count(distinct CustomerID) as TotalCustomers,
    count(distinct InvoiceNo) as TotalOrders,
    avg(Quantity * UnitPrice) as AvgOrderValue,
    sum(Quantity * UnitPrice) as TotalSpending
from online_retail
group by Country;
/*
Query summary: The query groups customers by their country and calculates the total number of customers, orders, average order value, and total spending for each country. 
Results: This segmentation can help identify which countries have the most valuable customers or potential for growth. For example, the United Kingdom has the highest number of customers and orders, 
		 but their average order value is relatively low compared to other countries like Japan or Sweden.
		 By segmenting customers based on their location and shopping behavior, businesses can tailor their marketing and sales strategies to better target and serve each group. For instance, 
         they could offer country-specific promotions, optimize shipping and delivery options, or personalize product recommendations based on each customer's spending habits.
*/

-- 2. Segmenting customers based on Behaviour.
-- a. Calculate the total number of purchases made by each customer:
select CustomerID, count(*) as total_purchases
from online_retail
group by CustomerID;
/*
Query summary: The query helps us identify the total number of purchases made by each customer. 
Result: From the query results, we can observe that the total purchases by customers range from 1 to 7279. This information is useful for segmenting customers based on their purchase behavior, 
		such as frequent buyers, occasional buyers, and one-time buyers. By understanding the behavior of different customer segments, businesses can develop targeted marketing strategies to 
        improve customer retention and increase sales.
*/

-- b. Calculate the total amount spent by each customer:
select CustomerID, SUM(Quantity * UnitPrice) as total_spent
from online_retail
group by CustomerID;
/*
Result: 
Based on the query result, we can observe that the customers have a wide range of total spending amounts, varying from negative values to a maximum of 268174.46. This information can 
be used for segmentation purposes, as customers can be grouped based on their total spending amount into different segments, such as high-spending, medium-spending, and low-spending customers. 
This can help businesses to tailor their marketing strategies and offers to each segment accordingly.
*/

-- c. Calculate the average purchase value for each customer:
select CustomerID, avg(Quantity * UnitPrice) as avg_purchase_value
from online_retail
group by CustomerID;
/*
Result: 
The query results show a wide range in the amount that customers spend on average, indicating potential different customer segments based on purchasing power and interests. 
Analyzing these segments further can help businesses tailor their marketing strategies and promotions, such as targeting premium products to high average purchase value customers and 
offering discounts to lower average purchase value customers.
*/

-- d. Calculate the frequency of each customer purchase:
select CustomerID, count(distinct InvoiceNo) as num_purchases
from online_retail
where InvoiceDate between '2010-12-01' and '2011-12-09'
group by CustomerID;
/*
Result: 
The above query helps to segment customers based on their purchase frequency between a certain time period. From the result, it can be observed that the frequency of purchases ranges 
from 1 day to 243 days. This information can be used to identify customers who make frequent purchases and those who make infrequent purchases, which can aid in developing marketing 
strategies targeted towards each segment.
*/

-- e. Segmenting customers based on purchase behaviour metrics:
select CustomerID,
       case
           when total_spent > 1000 then 'High Spenders'
           when total_purchases > 10 then 'Frequent Shoppers'
           when total_purchases >= 1 and total_purchases <= 2 then 'Occasional Buyers'
           when total_purchases >= 3 and total_purchases <= 5 then 'Regular Buyers'
           when total_purchases >= 6 and total_purchases <= 10 then 'Loyal Buyers'
           else 'Other'
       end as segment
from (
    select CustomerID, count(distinct InvoiceNo) as total_purchases, sum(Quantity * UnitPrice) as total_spent
    from online_retail
    where InvoiceDate between '2010-12-01' and '2011-12-09'
    group by CustomerID
) sub;
/*
Query summary: 
* High Spenders: Customers who have spent more than 1000 have been identified as high spenders.
* Frequent Shoppers: Customers who made more than 10 purchases during the specified period are categorized as frequent shoppers.
* Occasional Buyers: Customers who made 1-2 purchases during the specified period are classified as occasional buyers.
* Regular Buyers: Customers who made 3-5 purchases during the specified period are categorized as regular buyers.
* Loyal Buyers: Customers who made 6-10 purchases during the specified period are identified as loyal buyers.
Result: 
From the results of the query, we can see that the majority of customers (1990) fall into the "Occasional Buyers" category, followed by "High Spenders" (1588), "Regular Buyers" (666), 
"Loyal Buyers" (107), and "Frequent Shoppers" (4). These categories can help businesses tailor their marketing strategies to better target each group, such as offering promotions or 
discounts to the "Occasional Buyers" to encourage more purchases or developing loyalty programs for the "Loyal Buyers" to increase customer retention.
*/

/*
Conclusion: 
In summary, the online retail dataset contains information about transactions of a company with 21982 unique transactions made by 4356 customers across different countries. The dataset 
includes 8 variables, such as the invoice number, product code and description, quantity, invoice date and time, unit price, customer number, and country. The analysis of sales across 
different countries revealed that the United Kingdom has the highest total sales, followed by the Netherlands and EIRE. The distribution of sales by time of the day showed that the highest 
sales occurred during 12:00 PM and 3:00 PM, while the lowest sales occurred at 6:00 AM. The analysis of sales by day of the week revealed that Thursday had the highest sales, followed by 
Wednesday and Tuesday. Finally, the sales distribution by month of the year showed that November had the highest sales, followed by December and October. These findings provide valuable 
insights into the sales patterns of the company and can help inform future business decisions.
*/

/*
Note: The customer ID column in the dataset had up to 25% null values, which were removed prior to the analysis and segmentation. As a result, the results presented in this analysis 
are based on a reduced dataset and may not be representative of the entire population. Additionally, the analysis was limited to a specific date range, and assumptions were made 
regarding customer behavior. These limitations should be considered when interpreting the results.
*/

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



