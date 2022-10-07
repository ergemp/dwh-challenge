select product_id, count(1) from order_items_fact
group by product_id
order by 2 desc;


select * from date_dim dd2 limit 100;

select
  dd.year,
  dd.month,
  count(1)
from orders_dim od
left join date_dim dd on (to_date(od.order_purchase_timestamp, 'YYYY-MM-DD') = dd.ddate)
group by dd.year, dd.month;


select
	ld.location_city ,
	count(1)
from orders_dim od
left join location_dim ld on (od.location_id = ld.location_id)
group by ld.location_city order by 2 desc;


select
  od.order_id,
  oidd.order_item_id,
  oidd.price as order_item_price,
  pd.payment_value
from order_items_fact oidd
inner join orders_dim od on (oidd.order_id = od.order_id)
inner join payments_dim pd on (pd.order_id = od.order_id)



select * from payments_dim where order_id='000229ec398224ef6ca0657da4fc703e'
select * from orders_dim od where order_id='000229ec398224ef6ca0657da4fc703e'
select * from order_items_fact where order_id ='000229ec398224ef6ca0657da4fc703e'




