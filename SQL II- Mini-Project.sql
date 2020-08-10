##########################  SQL-2 MINI PROJECT ##########################

create database combine_data;
use combine_data;


## 1. Join all the tables and create a new table called combined_table. (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen) 

create table combined_table 
as
select * from market_fact mf join cust_dimen using(Cust_id)
join orders_dimen using (Ord_id)
join prod_dimen using (Prod_id)
join shipping_dimen using (Ship_id,Order_ID);

## 2. Find the top 3 customers who have the maximum number of orders 
 
select Customer_Name, count(distinct Ord_id) as no_of_orders
from combined_table
group by Customer_Name,Cust_id
order by no_of_orders desc limit 3;
 
## 3. Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date. 

set sql_safe_updates=0;
set autocommit=0;
ALTER TABLE combined_table ADD (ord_date DATE);
UPDATE combined_table SET ord_date=STR_TO_DATE(Order_Date ,'%d-%m-%Y');
ALTER TABLE combined_table DROP Order_Date;
ALTER TABLE combined_table RENAME column ord_date TO Order_Date;
ALTER TABLE combined_table ADD (shp_date DATE);
UPDATE combined_table SET shp_date=STR_TO_DATE(Ship_Date ,'%d-%m-%Y');
ALTER TABLE combined_table DROP Ship_Date;
ALTER TABLE combined_table RENAME column shp_date TO Ship_Date;
select * from combined_table;
commit;

set sql_safe_updates=0;
alter table combined_table add DaysTakenForDelivery int;
update combined_table set DaysTakenForDelivery=datediff(Ship_Date,Order_Date); 

## 4. Find the customer whose order took the maximum time to get delivered. 

select Customer_Name,DaysTakenForDelivery
from combined_table
where DaysTakenForDelivery = (select max(DaysTakenForDelivery) from combined_table);
 
## 5. Retrieve total sales made by each product from the data (use Windows function) 
 
select distinct Prod_id,Product_Category,Product_Sub_Category,
round(sum(Sales) over(partition by Prod_id),2) as Total_Sales
from combined_table
order by 4 desc;
 
## 6. Retrieve total profit made from each product from the data (use windows function) 

select distinct Prod_id,Product_Category,Product_Sub_Category,
round(sum(Profit) over(partition by Prod_id),2) as Total_Profit
from combined_table
order by 4 desc;


## 7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011 

select count(distinct Cust_id) Total_no_of_uniq_cus from 
(
	select Cust_id ,Customer_Name,
	monthname(Order_Date) as Month_name ,
	year(Order_Date) as Year,
	sum(count( distinct Cust_id)) over(partition by Cust_id) as no_of_months
	from combined_table
	where year(Order_Date)=2011 and monthname(Order_Date)='January'
	group by Cust_id,Customer_Name,Month_name,year
) as Temp_tab;


select count(distinct Cust_id) Total_no_of_uniq_cus,no_of_months from 
(
	select Cust_id ,Customer_Name,
	monthname(Order_Date) as Month_name ,
	year(Order_Date) as Year,
	sum(count( distinct Cust_id)) over(partition by Cust_id) as no_of_months
	from combined_table
	where year(Order_Date)=2011
	group by Cust_id,Customer_Name,Month_name,year

) as Temp_tab
where no_of_months=12;


## 8. Retrieve month-by-month customer retention rate since the start of the business.(using views) 
 
create view MONTH_LOG as
select Cust_id,Customer_Name ,
month(Order_Date) as Month ,
year(Order_Date) as Year,
sum(count( distinct Cust_id)) over(partition by Cust_id) as no_of_months
from combined_table
group by Cust_id,Customer_Name,Month;

select 1+5,6+4 from dual;

create view Retention_CAl as
select Current_Month,(sum(if(Retention_Type='Retained',1,0))/count(Retention_Type))*100 as Retention_Rate from (
SELECT *,
CASE
	WHEN Gap IS NULL THEN "Churned"
    WHEN (Gap>1 AND Prev_Month!=0) THEN "Irregular"
    WHEN (Gap=1 AND Prev_Month!=0) THEN "Retained"
    ELSE "Initial Month"
    END AS Retention_Type
FROM(
SELECT *,IF((Prev_Month=0 AND Total_Purchase_Month=1),NULL,(Current_Month-Prev_Month)) AS Gap
FROM 
(
SELECT cust_id,Month AS Current_Month,
LAG(Month, 1, 0) OVER(PARTITION BY cust_id ORDER BY Month) AS Prev_Month,
count(Month) OVER (PARTITION BY cust_id) as Total_Purchase_Month
FROM MONTH_LOG
) AS temp) AS temp_2) as temp_3
group by Current_Month;

select * from Retention_CAl;
