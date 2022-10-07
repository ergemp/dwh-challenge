
--top5 products according to number of sales
select product_id, count(1) from order_items_fact
group by product_id
order by 2 desc
limit 5;


--number of orders by year by month
select
  dd.year,
  dd.month,
  count(1) as number_of_orders
from orders_dim od
left join date_dim dd on (to_date(od.order_purchase_timestamp, 'YYYY-MM-DD') = dd.ddate)
group by dd.year, dd.month;


--number of orders by city
select
	ld.location_city ,
	count(1) as number_of_orders
from orders_dim od
left join location_dim ld on (od.location_id = ld.location_id)
group by ld.location_city order by 2 desc;

--control sql
select
  od.order_id,
  string_agg(oidd.order_item_id ,','),
  sum(cast(oidd.price as decimal(9,4))) as order_item_price,
  sum(cast(oidd.freight_value as decimal(9,4))),
  sum(cast(oidd.price as decimal(9,4)) + cast(oidd.freight_value as decimal(9,4))) as should_pay,
  max(pd.payment_value)
from order_items_fact oidd
inner join orders_dim od on (oidd.order_id = od.order_id)
inner join payments_dim pd on (pd.order_id = od.order_id)
group by od.order_id;

--total profit by year and month
select
	dd.year,
	dd.month,
	sum(cast(oidd.price as decimal(9,4))) as total_profit
from order_items_fact oidd
left join orders_dim od on (oidd.order_id = od.order_id)
left join date_dim dd on (to_date(od.order_purchase_timestamp, 'YYYY-MM-DD') = dd.ddate)
group by dd.year ,dd.month ;


--pct of orders that takes above 24h = 1 day
select round(cast(late_orders as decimal(8,2))/ cast(total_orders as decimal(8,2)) * 100 , 2) as late_delivery_pct
from
(
select
  count(1) as total_orders,
  (select
	  count(1) as late_orders
	from
	(
	select
	    order_status,
		to_timestamp(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') as  order_purchase_timestamp,
		to_timestamp(order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') as  order_delivered_customer_date
	from orders_dim od
	) sub
	where
	  order_delivered_customer_date - order_purchase_timestamp > interval '1' day
	  and order_status='delivered'
	) as late_orders
from orders_dim od
) sub


--top5 product categories most commonly purchased by new customers
select cd.category_name_english , count(1) total_sold from
  	order_items_fact oif
  	left join
	(
		select
		  od2.order_id
		from
		  orders_dim od2
		where (customer_id, to_timestamp(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')) in
		(
		  select customer_id, min(to_timestamp(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')) as first_order from orders_dim od group by customer_id
		)
	) first_purchases
	on (oif.order_id = first_purchases.order_id)
	left join products_dim pd on (oif.product_id = pd.product_id)
	left join category_dim cd on (pd.product_category_name = cd.category_name)
group by cd.category_name_english
order by 2 desc
limit 5
	;




