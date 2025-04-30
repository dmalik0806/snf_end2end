-- Create 3 schemas, 1 for each of the zones i.e. Landing Zone, Curated Zone, Consumption Zone
-- Intially load data via WebUI
create schema landing_zone;
create schema curated_zone;
create schema consumption_zone;

--Step2 : Create transient tables in the landing_zone
use schema landing_zone
create transient table landing_zone.landing_item (
        item_id varchar,
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar,
        item_CATEGORY varchar
) comment ='this is item table with in landing schema';

create or replace transient table landing_zone.landing_customer (
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    birth_day varchar,
    birth_month varchar,
    birth_year varchar,
    birth_country varchar,
    email_address varchar
) comment ='this is customer table with in landing schema';

create or replace transient table landing_zone.landing_order (
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar
) comment ='this is order table with in landing schema';

show tables;

-- creating a file format before uploading the data into the tables
create file format landing_zone_csv_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N');

--Step 3: Insert the data in the landing Zone tables via WebUI

--Step 4: Create transient tables in the CURATED_ZONE schema
use schema curated_zone;

create or replace transient table curated_zone.curated_customer (
      customer_pk number autoincrement,
      customer_id varchar(18),
      salutation varchar(10),
      first_name varchar(20),
      last_name varchar(30),
      birth_day number,
      birth_month number,
      birth_year number,
      birth_country varchar(20),
      email_address varchar(50)
) comment ='this is customer table with in curated schema';

create or replace transient table curated_zone.curated_item (
      item_pk number autoincrement,
      item_id varchar(16),
      item_desc varchar,
      start_date date,
      end_date date,
      price number(7,2),
      item_class varchar(50),
      item_category varchar(50)
) comment ='this is item table with in curated schema';

create or replace transient table curated_zone.curated_order (
      order_pk number autoincrement,
      order_date date,
      order_time varchar,
      item_id varchar(16),
      item_desc varchar,
      customer_id varchar(18),
      salutation varchar(10),
      first_name varchar(20),
      last_name varchar(30),
      store_id varchar(16),
      store_name VARCHAR(50),
      order_quantity number,
      sale_price number(7,2),
      disount_amt number(7,2),
      coupon_amt number(7,2),
      net_paid number(7,2),
      net_paid_tax number(7,2),
      net_profit number(7,2)
) comment ='this is order table with in curated schema';

show tables;

--Step 4: creating dimension and fact tables in the CONSUMPTION_ZONE schema
use schema consumption_zone;

  create or replace table item_dim (
        item_dim_key number autoincrement,
        item_id varchar(16),
        item_desc varchar,
        start_date date,
        end_date date,
        price number(7,2),
        item_class varchar(50),
        item_category varchar(50),
        added_timestamp timestamp default current_timestamp() ,
        updated_timestamp timestamp default current_timestamp() ,
        active_flag varchar(1) default 'Y'
    ) comment ='this is item table with in consumption schema';

    create or replace table customer_dim (
        customer_dim_key number autoincrement,
        customer_id varchar(18),
        salutation varchar(10),
        first_name varchar(20),
        last_name varchar(30),
        birth_day number,
        birth_month number,
        birth_year number,
        birth_country varchar(20),
        email_address varchar(50),
        added_timestamp timestamp default current_timestamp() ,
        updated_timestamp timestamp default current_timestamp() ,
        active_flag varchar(1) default 'Y'
    ) comment ='this is customer table with in consumption schema';

    create or replace table order_fact (
      order_fact_key number autoincrement,
      order_date date,
      customer_dim_key number,
      item_dim_key number,
      order_count number,
      order_quantity number,
      sale_price number(20,2),
      disount_amt number(20,2),
      coupon_amt number(20,2),
      net_paid number(20,2),
      net_paid_tax number(20,2),
      net_profit number(20,2)
    ) comment ='this is order table with in consumption schema';

show tables;
