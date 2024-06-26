 -- How many pizzas were ordered?
select count(pizza_id) AS Total_ordered_pizza from customer_orders
  
 -- How many unique customer orders were made?
select count(distinct customer_id) as customer_count from customer_orders
  
 -- How many successful orders were delivered by each runner?
select runner_orders.runner_id,count(runner_orders.runner_id) over (partition by 
runner_orders.runner_id) as delivery_success from customer_orders
join runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')
  
 -- How many of each type of pizza was delivered?
select customer_orders.pizza_id,count(customer_orders.pizza_id) over (partition by 
customer_orders.pizza_id) as delivery_success from customer_orders
join runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')
  
 -- How many Vegetarian and Meatlovers were ordered by each customer?
select customer_id,count(case
when pizza_id = 1 then 1 end) over (partition by customer_id) as Meat_lovers,
count(case
when pizza_id = 2 then 1 end) over (partition by customer_id) as Vegetarian
from customer_orders
order by customer_id
  
 -- What was the maximum number of pizzas delivered in a single order?
select customer_orders.order_id,count(customer_orders.pizza_id) over 
(partition by customer_orders.order_id) as order_count from customer_orders
join runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')
  
 -- For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
with cte1 as (select customer_id,count(customer_id) over (partition by customer_id) as with_changes from(
select customer_orders.order_id,customer_orders.pizza_id,customer_orders.customer_id,
customer_orders.exclusions,customer_orders.extras from customer_orders
JOIN runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')) AS A
where exclusions not in('NULL') or extras not in ('NULL')),
cte2 as (select customer_id,count(customer_id) over (partition by customer_id) as without_changes from(
select customer_orders.order_id,customer_orders.pizza_id,customer_orders.customer_id,
customer_orders.exclusions,customer_orders.extras from customer_orders
JOIN runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')) AS A
where exclusions is NULL and extras is NULL)
SELECT * FROM cte1
LEFT JOIN cte2 ON cte1.customer_id = cte2.customer_id
UNION
SELECT * FROM cte1
RIGHT JOIN cte2 ON cte1.customer_id = cte2.customer_id
  
  -- How many pizzas were delivered that had both exclusions and extras?
select customer_id,count(customer_id) over (partition by customer_id) as with_both_changes from(
select customer_orders.order_id,customer_orders.pizza_id,customer_orders.customer_id,
customer_orders.exclusions,customer_orders.extras from customer_orders
JOIN runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.distance not in ('NULL')) AS A
where exclusions not in('NULL') and extras not in ('NULL')
  
 -- What was the total volume of pizzas ordered for each hour of the day?
with cte1 as (select pizza_id,order_date,order_hour,case when order_date = '2020-01-01' then 'Day_1'
when order_date = '2020-01-02' then 'Day_2' when order_date = '2020-01-04' then 'Day_3'
when order_date = '2020-01-08' then 'Day_4' when order_date = '2020-01-09' then 'Day_5'
when order_date ='2020-01-10' then 'Day_6' else 'Day_7' end as order_day from(
select pizza_id,date(order_time) as order_date,hour(order_time) as order_hour from customer_orders) as a)
select count(order_day) as order_count,order_hour,order_day from cte1 group by order_day,order_hour
  
 -- What was the volume of orders for each day of the week?
with cte1 as (select pizza_id,order_date,order_hour,case when order_date = '2020-01-01' then 'Day_1'
when order_date = '2020-01-02' then 'Day_2' when order_date = '2020-01-04' then 'Day_3'
when order_date = '2020-01-08' then 'Day_4' when order_date = '2020-01-09' then 'Day_5'
when order_date ='2020-01-10' then 'Day_6' else 'Day_7' end as order_day from(
select pizza_id,date(order_time) as order_date,hour(order_time) as order_hour from customer_orders) as a)
select count(order_day) as order_count,order_day from cte1 group by order_day
  
 -- How many runners signed up for each 1 week period?
with cte1 as (select runner_id,registration_date,case when
ceil(datediff(registration_date,'2021-01-01')/7) = 0 then 'week1'
when ceil(datediff(registration_date,'2021-01-01')/7) = 1 then 'week1'
else 'week2' end as weeks  from runners)
select runner_id,weeks,count(runner_id) over (partition by weeks) as runner_signed from cte1
  
 -- What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?
with cte1 as (
select runner_orders.runner_id,customer_orders.order_time,runner_orders.pickup_time,
timestampdiff(minute,customer_orders.order_time,runner_orders.pickup_time) as time_diff from runner_orders
join customer_orders on customer_orders.order_id = runner_orders.order_id
)
select runner_id,avg(time_diff) from cte1
WHERE time_diff not in ('NULL')
group by 1
  
 -- Is there any relationship between the number of pizzas and how long the order takes to prepare?
with cte1 as (select customer_orders.order_id,count(customer_orders.pizza_id) over (partition by customer_orders.order_id) as 
order_count,timestampdiff(minute,customer_orders.order_time,runner_orders.pickup_time) as time_taken 
from customer_orders
join runner_orders on runner_orders.order_id = customer_orders.order_id)
select order_id,order_count,sum(time_taken) over (partition by order_id) as total_time
from cte1 where time_taken not in ('NULL')
order by 3 DESC
  
 -- What was the average distance travelled for each customer?
select customer_orders.customer_id,avg(runner_orders.distance) as avg_distance from runner_orders
join customer_orders on customer_orders.order_id = runner_orders.order_id
where runner_orders.distance not in ('NULL')
group by customer_orders.customer_id
  
 -- What was the difference between the longest and shortest delivery times for all orders?
select max(duration) - min(duration) as time_difference from runner_orders
  
 -- What was the average speed for each runner for each delivery and do you notice any trend for these values?
select order_id,runner_id,avg(distance/(duration/60)) as avg_distance from runner_orders
where distance not in ('NULL')
group by 1,2 order by 2
  
 -- What is the successful delivery percentage for each runner?
select runner_id,avg(percent) as delivery_percentage from(
select runner_id,case when cancellation is NULL then '100'
else '0' end as percent from runner_orders) as a
group by runner_id
  
 -- What are the standard ingredients for each pizza?
select * from pizza_toppings
join pizza_recipes on pizza_toppings.topping_id = pizza_recipes.topping
  
 -- What was the most commonly added extra?
select topping_name as Most_common_extras from pizza_toppings
where topping_id = (select extras from(
select extras,count(*) as most_occurence from customer_orders
where extras not in (' ','')
group by 1 order by 2 desc limit 1) as id)
  
 -- What was the most common exclusion?
select topping_name as Most_common_exclusions from pizza_toppings
where topping_id = (select exclusions from(
select exclusions,count(*) as most_occurence from customer_orders
where exclusions not in ('',' ')
group by 1 order by 2 desc limit 1) as id)
  
 -- If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes 
 -- how much money has Pizza Runner made so far if there are no delivery fees?
select pizza_id,sum(prize)as earned_money from (
select customer_orders.order_id,customer_orders.pizza_id,
case 
when customer_orders.pizza_id = 1 then '12' 
when customer_orders.pizza_id = 2
then '10' else '0' end as prize  from customer_orders
join runner_orders on runner_orders.order_id = customer_orders.order_id
where runner_orders.cancellation not in ('Restaurant Cancellation')) as a
group by pizza_id
  
 -- What if there was an additional $1 charge for any pizza extras?
with cte1 as (select pizza_id,prize,case
when extras = 1 then '1'
else 0
end as extra_prize from (
select pizza_id,extras,
case
when pizza_id = 1 then '12'
when pizza_id = 2 then '10'
else '0'
end as prize from customer_orders) as a)
select pizza_id,prize+extra_prize as total_prize from cte1
  
 -- Using your newly generated table - can you join all of the information together to form a table which has the following 
  #information for successful deliveries?
with cte1 as(select ratings.order_id,customer_orders.customer_id,runner_orders.runner_id,ratings.ratings,
customer_orders.order_time,runner_orders.pickup_time,runner_orders.duration,customer_orders.pizza_id,
runner_orders.distance from ratings
join customer_orders on ratings.order_id = customer_orders.order_id
join runner_orders on runner_orders.order_id = ratings.order_id)
select customer_id,order_id,runner_id,ratings,order_time,pickup_time,timestampdiff(minute,order_time,pickup_time)
as time_between_order_pickup,duration,distance/(duration/60) as avg_speed,
row_number() over (order by order_id) as pizza_count
from cte1 where duration not in ('NULL')
  
 -- If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is paid $0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?
select runner_id,SUM(delivery_amount) over (partition by runner_id) as runner_earnings from (
select order_id,runner_id,distance*0.30 as delivery_amount from runner_orders) as a
where delivery_amount not in ('NULL')
