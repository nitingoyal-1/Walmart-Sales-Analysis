
use walmartdb;
show tables;
select * from walmart;


# Task_01 IDENTIFYING TOP BRANCH BY SALES GROWTH RATE

with growth as(select branch,date_format(date,'%Y-%m') as month,sum(total) as current_sale,
lag(sum(total))
over(partition by branch order by date_format(date,'%Y-%m')) as last_sale 
from walmart group by branch,month)
select branch,round(avg(((last_sale - current_sale)/(last_sale))*100),2) as avg_growth_rate 
from growth group by branch 
order by avg_growth_rate desc limit 1;

# Task_02 FINDING THE MOST PROFITABLE PRODUCT LINE FOR EACH BRANCH

with p_line as(select branch,`product line`,sum(`gross income`) as income,
rank() over (partition by branch order by sum(`gross income`) desc) as rank_order
from walmart group by branch,`product line`)
select branch,`product line` from p_line where rank_order=1;

# Task_03 ANALYZING CUSTOMER SEGMENTATION BASED ON SPENDING 

with customertype as(select `customer id`,sum(total) as spending,
ntile(3) over(order by sum(total)desc) as spending_type
from walmart group by `customer id`)
select `customer id`,spending,
case 
when spending_type=1 then 'High'
when spending_type=2 then 'medium'
else 'low'
end as spending_category
from customertype;

# Task_04 DETECTING ANOMALIES IN SALES TRANSACTIONS
#company detects some flagged transaction 
#which is 30% above or below the averge productline sales
with productline_avg  as (
select `product line`,avg(total) as average_total from walmart group by `product line`)
select w.`invoice id`,w.branch,w.`product line`,w.total,p.average_total, 
case 
when p.average_total*1.3 < w.total then 'anomaly'
when p.average_total*0.7 > w.total then 'anomaly'
else 'normal'
end as status
from walmart as w join productline_avg as p on p.`product line`=w.`product line`
where p.average_total*1.3 < w.total or p.average_total*0.7 > w.total ;

# Task_05 MOST POPULAR PAYMENT METHOD BY CITY

with popular as (select city,payment,count(*) as popularity,
rank() over (partition by city order by count(*) desc) as rnk 
from walmart group by city,payment order by city,payment )
select city,payment,popularity from popular where rnk=1;

# Task_06 MONTHLY SALES DISTRIBUTION BY GENDER

select year(date) as year,month(date) as month,gender,round(sum(total),2) as distribution_of_sales from walmart 
group by year(date),month(date),gender order by year(date),month(date);

# Task_07 BEST PRODUCT LINE BY CUSTOMER TYPE

select `customer type`,`product line`,purchase_frequency 
from (select `customer type`,`product line`,count(*) as purchase_frequency,
rank() over (partition by `customer type` order by count(*) desc) as rnk 
from walmart group by `customer type`,`product line`)as ranked_data where rnk=1;

# Task_08 IDENTIFYING REPEAT CUSTOMERS
SELECT * FROM (
    SELECT 
        `customer id`, 
        `invoice id`, 
        date AS current_purchase_date,
        LAG(date) OVER (PARTITION BY `customer id` ORDER BY date) AS previous_purchase_date,
        DATEDIFF(date, LAG(date) OVER (PARTITION BY `customer id` ORDER BY date)) AS days_between_purchases,
        CASE  
            WHEN LAG(date) OVER (PARTITION BY `customer id` ORDER BY date) IS NULL THEN 'First-time Customer'  
            WHEN DATEDIFF(date, LAG(date) OVER (PARTITION BY `customer id` ORDER BY date)) <= 30 THEN 'Frequent Buyer'  
            ELSE 'Occasional Buyer'  
        END AS customer_type  
    FROM walmart  
) subquery
WHERE customer_type != 'First-time Customer';

# Task_09 FINDING TOP 5 CUSTOMERS BY SALES VOLUME

select `customer id`,round(sum(cogs),2) as sales_volume 
from walmart group by `customer id` order by sum(cogs) desc limit 5;

# Task_10 ANALYZING THE SALES TRENDS BY THE DAYS OF A WEEK

select dayname(date) as Days,round(sum(cogs),2) as Total_sales 
from walmart group by days order by Total_sales desc;