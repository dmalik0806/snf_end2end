use schema landing_zone;

create or replace stream landing_customer_stm on table landing_customer
append_only = true;

select system$stream_has_data('landing_customer_stm');

create or replace stream landing_item_stm on table landing_item
append_only = true;

create or replace stream landing_order_stm on table landing_order
append_only = true;
show streams;

-- now we will be creating tasks, these tasks will check the streams created in the landing_zone schema first 
-- before inserting the data in the curated_zone schema tables 
use schema curated_zone;

create or replace task order_curated_tsk
    warehouse = compute_wh 
    schedule  = '1 minute'
when
    system$stream_has_data('INTERVIEW_PREP.landing_zone.landing_order_stm')
as
  merge into INTERVIEW_PREP.curated_zone.curated_order curated_order 
  using INTERVIEW_PREP.landing_zone.landing_order_stm landing_order_stm on
  curated_order.order_date = landing_order_stm.order_date and 
  curated_order.order_time = landing_order_stm.order_time and 
  curated_order.item_id = landing_order_stm.item_id and
  curated_order.item_desc = landing_order_stm.item_desc 
when matched 
   then update set 
      curated_order.customer_id = landing_order_stm.customer_id,
      curated_order.salutation = landing_order_stm.salutation,
      curated_order.first_name = landing_order_stm.first_name,
      curated_order.last_name = landing_order_stm.last_name,
      curated_order.store_id = landing_order_stm.store_id,
      curated_order.store_name = landing_order_stm.store_name,
      curated_order.order_quantity = landing_order_stm.order_quantity,
      curated_order.sale_price = landing_order_stm.sale_price,
      curated_order.disount_amt = landing_order_stm.disount_amt,
      curated_order.coupon_amt = landing_order_stm.coupon_amt,
      curated_order.net_paid = landing_order_stm.net_paid,
      curated_order.net_paid_tax = landing_order_stm.net_paid_tax,
      curated_order.net_profit = landing_order_stm.net_profit
    when not matched then 
    insert (
      order_date ,
      order_time ,
      item_id ,
      item_desc ,
      customer_id ,
      salutation ,
      first_name ,
      last_name ,
      store_id ,
      store_name ,
      order_quantity ,
      sale_price ,
      disount_amt ,
      coupon_amt ,
      net_paid ,
      net_paid_tax ,
      net_profit ) 
    values (
      landing_order_stm.order_date ,
      landing_order_stm.order_time ,
      landing_order_stm.item_id ,
      landing_order_stm.item_desc ,
      landing_order_stm.customer_id ,
      landing_order_stm.salutation ,
      landing_order_stm.first_name ,
      landing_order_stm.last_name ,
      landing_order_stm.store_id ,
      landing_order_stm.store_name ,
      landing_order_stm.order_quantity ,
      landing_order_stm.sale_price ,
      landing_order_stm.disount_amt ,
      landing_order_stm.coupon_amt ,
      landing_order_stm.net_paid ,
      landing_order_stm.net_paid_tax ,
      landing_order_stm.net_profit );


create or replace task customer_curated_tsk
    warehouse = compute_wh 
    schedule  = '2 minute'
when
    system$stream_has_data('INTERVIEW_PREP.landing_zone.landing_customer_stm') AND system$stream_has_data('INTERVIEW_PREP.landing_zone.landing_order_stm')
as
merge into INTERVIEW_PREP.curated_zone.curated_customer curated_customer 
using INTERVIEW_PREP.landing_zone.landing_customer_stm landing_customer_stm on
curated_customer.customer_id = landing_customer_stm.customer_id
when matched 
   then update set 
      curated_customer.salutation = landing_customer_stm.salutation,
      curated_customer.first_name = landing_customer_stm.first_name,
      curated_customer.last_name = landing_customer_stm.last_name,
      curated_customer.birth_day = landing_customer_stm.birth_day,
      curated_customer.birth_month = landing_customer_stm.birth_month,
      curated_customer.birth_year = landing_customer_stm.birth_year,
      curated_customer.birth_country = landing_customer_stm.birth_country,
      curated_customer.email_address = landing_customer_stm.email_address
when not matched then 
  insert (
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address ) 
  values (
    landing_customer_stm.customer_id ,
    landing_customer_stm.salutation ,
    landing_customer_stm.first_name ,
    landing_customer_stm.last_name ,
    landing_customer_stm.birth_day ,
    landing_customer_stm.birth_month ,
    landing_customer_stm.birth_year ,
    landing_customer_stm.birth_country ,
    landing_customer_stm.email_address );

create or replace task item_curated_tsk
    warehouse = compute_wh 
    schedule  = '3 minute'
when
    system$stream_has_data('INTERVIEW_PREP.landing_zone.landing_item_stm')
as
merge into INTERVIEW_PREP.curated_zone.curated_item item using INTERVIEW_PREP.landing_zone.landing_item_stm landing_item_stm on
item.item_id = landing_item_stm.item_id and 
item.item_desc = landing_item_stm.item_desc and 
item.start_date = landing_item_stm.start_date
when matched 
   then update set 
      item.end_date = landing_item_stm.end_date,
      item.price = landing_item_stm.price,
      item.item_class = landing_item_stm.item_class,
      item.item_category = landing_item_stm.item_category
when not matched then 
  insert (
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category) 
  values (
    landing_item_stm.item_id,
    landing_item_stm.item_desc,
    landing_item_stm.start_date,
    landing_item_stm.end_date,
    landing_item_stm.price,
    landing_item_stm.item_class,
    landing_item_stm.item_category);

show tasks;

-- create streams for curated zone tables
create or replace stream curated_item_stm on table curated_item;
create or replace stream curated_customer_stm on table curated_customer;
create or replace stream curated_order_stm on table curated_order;

-- Create tasks for the consumption_zone schema tables
use schema consumption_zone;

create or replace task item_consumption_tsk
  warehouse = compute_wh 
  schedule  = '4 minute'
when
    system$stream_has_data('curated_zone.curated_item_stm')
as
  merge into consumption_zone.item_dim item using curated_zone.curated_item_stm curated_item_stm on
  item.item_id = curated_item_stm.item_id and 
  item.start_date = curated_item_stm.start_date and 
  item.item_desc = curated_item_stm.item_desc
when matched 
  and curated_item_stm.METADATA$ACTION = 'INSERT'
  and curated_item_stm.METADATA$ISUPDATE = 'TRUE'
  then update set 
      item.end_date = curated_item_stm.end_date,
      item.price = curated_item_stm.price,
      item.item_class = curated_item_stm.item_class,
      item.item_category = curated_item_stm.item_category
when matched 
  and curated_item_stm.METADATA$ACTION = 'DELETE'
  and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
  then update set 
      item.active_flag = 'N',
      updated_timestamp = current_timestamp()
when not matched 
  and curated_item_stm.METADATA$ACTION = 'INSERT'
  and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
then 
  insert (
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category) 
  values (
    curated_item_stm.item_id,
    curated_item_stm.item_desc,
    curated_item_stm.start_date,
    curated_item_stm.end_date,
    curated_item_stm.price,
    curated_item_stm.item_class,
    curated_item_stm.item_category);

create or replace task customer_consumption_tsk
    warehouse = compute_wh 
schedule  = '5 minute'
when
  system$stream_has_data('curated_zone.curated_customer_stm')
as
  merge into consumption_zone.customer_dim customer using curated_zone.curated_customer_stm curated_customer_stm on
  customer.customer_id = curated_customer_stm.customer_id 
when matched 
  and curated_customer_stm.METADATA$ACTION = 'INSERT'
  and curated_customer_stm.METADATA$ISUPDATE = 'TRUE'
  then update set 
      customer.salutation = curated_customer_stm.salutation,
      customer.first_name = curated_customer_stm.first_name,
      customer.last_name = curated_customer_stm.last_name,
      customer.birth_day = curated_customer_stm.birth_day,
      customer.birth_month = curated_customer_stm.birth_month,
      customer.birth_year = curated_customer_stm.birth_year,
      customer.birth_country = curated_customer_stm.birth_country,
      customer.email_address = curated_customer_stm.email_address
when matched 
  and curated_customer_stm.METADATA$ACTION = 'DELETE'
  and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
  then update set 
      customer.active_flag = 'N',
      customer.updated_timestamp = current_timestamp()
when not matched 
  and curated_customer_stm.METADATA$ACTION = 'INSERT'
  and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
then 
  insert (
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address ) 
  values (
    curated_customer_stm.customer_id ,
    curated_customer_stm.salutation ,
    curated_customer_stm.first_name ,
    curated_customer_stm.last_name ,
    curated_customer_stm.birth_day ,
    curated_customer_stm.birth_month ,
    curated_customer_stm.birth_year ,
    curated_customer_stm.birth_country ,
    curated_customer_stm.email_address);

create or replace task order_fact_tsk
warehouse = compute_wh 
schedule  = '6 minute'
when
  system$stream_has_data('curated_zone.curated_order_stm')
as
insert overwrite into consumption_zone.order_fact (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit) 
select 
      co.order_date,
      cd.customer_dim_key ,
      id.item_dim_key,
      count(1) as order_count,
      sum(co.order_quantity) ,
      sum(co.sale_price) ,
      sum(co.disount_amt) ,
      sum(co.coupon_amt) ,
      sum(co.net_paid) ,
      sum(co.net_paid_tax) ,
      sum(co.net_profit)  
  from curated_zone.curated_order co 
    join consumption_zone.customer_dim cd on cd.customer_id = co.customer_id
    join consumption_zone.item_dim id on id.item_id = co.item_id and id.item_desc = co.item_desc
    group by 
        co.order_date,
        cd.customer_dim_key ,
        id.item_dim_key
        order by co.order_date; 

select *  from table(information_schema.task_history()) 
where name in ('ITEM_CONSUMPTION_TSK' ,'CUSTOMER_CONSUMPTION_TSK','ORDER_FACT_TSK')
order by scheduled_time;
