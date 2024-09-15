#1.)How many customers has Foodie-Fi ever had?
select count(distinct customer_id) as total_customer from subscriptions;

#2.)What is the monthly distribution of trial plan start_date values for our dataset - use the start of the month as the group by value
select start_date from subscriptions
where plan_id = '0'
order by start_date;

#3.)What plan start_date values occur after the year 2020 for our dataset? Show the breakdown by count of events for each plan_name
select plan_name,start_date,count(start_date) over (partition by plan_name) as count_events from(
select plans.plan_name,subscriptions.start_date from subscriptions
join plans on plans.plan_id = subscriptions.plan_id 
where year(subscriptions.start_date) > 2020) as a;

#4.)What is the customer count and percentage of customers who have churned rounded to 1 decimal place?
with cte1 as (
select count( distinct customer_id) as customer_count,row_number() 
over (partition by count( distinct customer_id)) as id  from subscriptions),
cte2 as (
select count(distinct customer_id) as churn_customers,row_number()
over (partition by count( distinct customer_id)) as id from subscriptions
where plan_id = '4')
select cte1.customer_count,round((cte2.churn_customers/cte1.customer_count)*100,1)as churn_ratio from cte1
join cte2 on cte1.id = cte2.id;

#5.)How many customers have churned straight after their initial free trial - what percentage is this rounded to the nearest whole number?
with cte1 as 
(select count(customer_id) as churn_customers,row_number() over(partition by count(customer_id)) as id from(
select customer_id,plan_id,start_date,dense_rank() over ( partition by customer_id order by start_date )
as row_no from subscriptions) as a
where plan_id in ('0','4') and row_no in ('2')),
cte2 as (select count(distinct customer_id) as total_customers,row_number() over (partition by 
count(distinct customer_id)) as id from subscriptions)
select cte1.churn_customers,round((cte1.churn_customers/cte2.total_customers)*100) as churn_percent from cte1 
join cte2 on cte1.id = cte2.id;

#6.)What is the number and percentage of customer plans after their initial free trial?
with cte1 as (
select count(plan_id) as count_beforetrail,row_number()
over (partition by count(plan_id)) as serial_num from subscriptions),
cte2 as (
select count(plan_id) as count_aftertrail,row_number()
over (partition by count(plan_id)) as serial_num  from subscriptions
where plan_id not in ('0'))
select cte2.count_aftertrail,(cte2.count_aftertrail/cte1.count_beforetrail)*100 as percentage_aftertrail
from cte1 join cte2 on cte1.serial_num = cte2.serial_num;

#7.)How many customers have upgraded to an annual plan in 2020?
select count(customer_id) as annual_plan_customerscount from subscriptions
where plan_id = '3' and year(start_date) = '2020';

#8.)How many days on average does it take for a customer to an annual plan from the day they join Foodie-Fi?
with cte1 as (
select customer_id,datediff(start_date,annual_order) as days from(
select customer_id,lag(start_date) over (partition by customer_id) as annual_order,start_date from subscriptions
where plan_id in ('0','3')) as a
where datediff(start_date,annual_order) not in ('NULL'))
select days,count(days) over (partition by days) as avg_days from cte1;

#9.)Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)
with cte1 as (
select customer_id,datediff(start_date,annual_order) as days from(
select customer_id,lag(start_date) over (partition by customer_id) as annual_order,start_date from subscriptions
where plan_id in ('0','3')) as a
where datediff(start_date,annual_order) not in ('NULL'))
select avg_btw_periods,count(avg_btw_periods) over (partition by avg_btw_periods) as value_btw_periods from(
select days,case when days between 0 and 30 then '0-30'
when days between 30 and 60 then '30-60'
when days between 60 and 90 then '60-90'
when days between 90 and 120 then '90-120'
when days between 120 and 150 then '120-150'
when days between 150 and 180 then '150-180'
when days between 180 and 210 then '180-210'
when days between 210 and 240 then '210-240'
when days between 240 and 270 then '240-270'
when days between 270 and 300 then '270-300'
else 'above 300' 
end as avg_btw_periods from cte1) as b;

#10.)How many customers downgraded from a pro monthly to a basic monthly plan in 2020?
with cte1 as (
select customer_id,basic_monthly,ifnull(pro_monthly,'no_value') as pro_monthly from(
select customer_id,lag(start_date) over ( partition by customer_id) as pro_monthly,start_date as 
basic_monthly from subscriptions
where plan_id in ('2','1')) as a
where ifnull(pro_monthly,'no_value') not in ('no_value'))
select count(*) from(
select customer_id,case
when basic_monthly > pro_monthly then 'upgraded'
else 'downgraded' end as statu_s from cte1) as b
where statu_s = 'downgraded';

#11.)he Foodie-Fi team wants you to create a new payments table for the year 2020 that includes amounts paid by each customer in the subscriptions table
select subscriptions.customer_id,plans.plan_id,plans.plan_name,subscriptions.start_date as
payment_date,lag(subscriptions.start_date) over (partition by subscriptions.customer_id) as start_date,
lead(subscriptions.start_date) over (partition by subscriptions.customer_id) as end_date ,plans.price as amount,
row_number() over (partition by subscriptions.customer_id
order by subscriptions.start_date) as payment_order from plans
join subscriptions on subscriptions.plan_id = plans.plan_id
where year(subscriptions.start_date) = '2020' and plans.plan_id not in ('0')                    
order by subscriptions.customer_id;

