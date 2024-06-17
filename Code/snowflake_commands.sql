-- create a snowpark user (it can only be created using accountadmin role)
use role accountadmin;

create user Developer_SPK 
  password = 'Test@12$4' 
  comment = 'this is a s snowpark user' 
  default_role = sysadmin
  default_secondary_roles = ('ALL')
  must_change_password = false;

-- grants
grant role sysadmin to user snowpark_user;
grant USAGE on warehouse snowpark_etl_wh to role sysadmin;


-- create database
create database if not exists sales_dwh;

use database sales_dwh;

create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create schema if not exists audit; -- to capture all audit records
create schema if not exists common; -- for file formats sequence object etc


-- creating internal stage within source schema.
use schema source;
create or replace stage SALES_DWH.SOURCE.my_internal_stg;

list @SALES_DWH.SOURCE.MY_INTERNAL_STG;

select * from SALES_DWH.INFORMATION_SCHEMA.STAGES ;

use schema common;
-- create file formats csv (India), json (France), Parquet (USA)
create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'Null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- json file format with strip outer array true
create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- parquet file format
create or replace file format my_parquet_format
  type = parquet
  compression = snappy;


use schema sales_dwh.source;

  -- Internal Stage - Query The CSV Data File Format
select 
    t.$1::text as order_id, 
    t.$2::text as customer_name, 
    t.$3::text as mobile_key,
    t.$4::number as order_quantity, 
    t.$5::number as unit_price, 
    t.$6::number as order_valaue,  
    t.$7::text as promotion_code , 
    t.$8::number(10,2)  as final_order_amount,
    t.$9::number(10,2) as tax_amount,
    t.$10::date as order_dt,
    t.$11::text as payment_status,
    t.$12::text as shipping_status,
    t.$13::text as payment_method,
    t.$14::text as payment_provider,
    t.$15::text as mobile,
    t.$16::text as shipping_address
 from 
   @sales_dwh.source.my_internal_stg/
   (file_format => 'sales_dwh.common.my_csv_format', pattern => '.*\.csv.gz') t; 

ls @sales_dwh.source.my_internal_stg;

  -- Internal Stage - Query The Parquet Data File Format
select 
  $1:"Order ID"::text as orde_id,
  $1:"Customer Name"::text as customer_name,
  $1:"Mobile Model"::text as mobile_key,
  to_number($1:"Quantity") as quantity,
  to_number($1:"Price per Unit") as unit_price,
  to_decimal($1:"Total Price") as total_price,
  $1:"Promotion Code"::text as promotion_code,
  $1:"Order Amount"::number(10,2) as order_amount,
  to_decimal($1:"Tax") as tax,
  $1:"Order Date"::date as order_dt,
  $1:"Payment Status"::text as payment_status,
  $1:"Shipping Status"::text as shipping_status,
  $1:"Payment Method"::text as payment_method,
  $1:"Payment Provider"::text as payment_provider,
  $1:"Phone"::text as phone,
  $1:"Delivery Address"::text as shipping_address
 from 
   @my_internal_stg/
   (file_format => 'sales_dwh.common.my_parquet_format', pattern => '.*\.snappy.parquet') t; 


-- Internal Stage - Query The JSON Data File Format
select                                                       
    $1:"Order ID"::text as orde_id,                   
    $1:"Customer Name"::text as customer_name,          
    $1:"Mobile Model"::text as mobile_key,              
    to_number($1:"Quantity") as quantity,               
    to_number($1:"Price per Unit") as unit_price,       
    to_decimal($1:"Total Price") as total_price,        
    $1:"Promotion Code"::text as promotion_code,        
    $1:"Order Amount"::number(10,2) as order_amount,    
    to_decimal($1:"Tax") as tax,                        
    $1:"Order Date"::date as order_dt,                  
    $1:"Payment Status"::text as payment_status,        
    $1:"Shipping Status"::text as shipping_status,      
    $1:"Payment Method"::text as payment_method,        
    $1:"Payment Provider"::text as payment_provider,    
    $1:"Phone"::text as phone,                          
    $1:"Delivery Address"::text as shipping_address
from                                                
@sales_dwh.source.my_internal_stg/
(file_format => sales_dwh.common.my_json_format , pattern => '.*\.json');


-- Echange Rate data set for Money conversion
use schema common;
create or replace transient table exchange_rate(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);


select * from exchange_rate order by date ;


-- order table
use schema source;

create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';

show sequences;

-- India Sales Table in Source Schema (CSV File)
use schema sales_dwh.source;

create or replace transient table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 mobile varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Source Schema (Parquet File)
create or replace transient table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- France Sales Table in Source Schema (JSON File)
create or replace transient table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);


select * from fr_sales_order limit 100;

-- Command to check the load history of COPY INTO COMMAND
select *
from table(sales_dwh.information_schema.copy_history(TABLE_NAME=>'in_sales_order', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP()))) ;


-- Following are for curated schema
-- -----------------------------------
use schema curated;
create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';


use schema curated;
-- curated India sales order table

create or replace table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- curated US sales order table
create or replace table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- -- curated FR sales order table
create or replace table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);


select * from sales_dwh.common.exchange_rate;


-- region dimension
use schema consumption;
create or replace sequence region_dim_seq start = 1 increment = 1;
create or replace transient table region_dim(
    region_id_pk number primary key,
    Country text, 
    Region text,
    isActive text(1)
);


-- product dimension
use schema consumption;
create or replace sequence product_dim_seq start = 1 increment = 1;
create or replace transient table product_dim(
    product_id_pk number primary key,
    Mobile_key text,
    Brand text, 
    Model text,
    Color text,
    Memory text,
    isActive text(1)
);
-- promo_code dimension
use schema consumption;
create or replace sequence promo_code_dim_seq start = 1 increment = 1;
create or replace transient table promo_code_dim(
    promo_code_id_pk number primary key,
    promo_code text,
    isActive text(1)
);
-- customer dimension
use schema consumption;
create or replace sequence customer_dim_seq start = 1 increment = 1;
create or replace transient table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    CONCTACT_NO text,
    SHIPPING_ADDRESS text,
    country text,
    region text,
    isActive text(1)
);
-- payment dimension
use schema consumption;
create or replace sequence payment_dim_seq start = 1 increment = 1;
create or replace transient table payment_dim(
    payment_id_pk number primary key,
    PAYMENT_METHOD text,
    PAYMENT_PROVIDER text,
    country text,
    region text,
    isActive text(1)
);
-- date dimension
use schema consumption;
create or replace sequence date_dim_seq start = 1 increment = 1;
create or replace transient table date_dim(
    date_id_pk int primary key,
    order_dt date,
    order_year int,
    oder_month int,
    order_quater int,
    order_day int,
    order_dayofweek int,
    order_dayname text,
    order_dayofmonth int,
    order_weekday text
);

-- fact tables
use schema consumption;
create or replace sequence SALES_FACT_SEQ start = 1 increment = 1;

create or replace table sales_fact (
 order_id_pk number(38,0),
 order_code varchar(),
 date_id_fk number(38,0),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 promo_code_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8)
);



-- Table Containts
alter table sales_fact add
    constraint fk_sales_region FOREIGN KEY (REGION_ID_FK) REFERENCES region_dim (REGION_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_date FOREIGN KEY (DATE_ID_FK) REFERENCES date_dim (DATE_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_customer FOREIGN KEY (CUSTOMER_ID_FK) REFERENCES customer_dim (CUSTOMER_ID_PK) NOT ENFORCED;
--
alter table sales_fact add
    constraint fk_sales_payment FOREIGN KEY (PAYMENT_ID_FK) REFERENCES payment_dim (PAYMENT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_product FOREIGN KEY (PRODUCT_ID_FK) REFERENCES product_dim (PRODUCT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_promot FOREIGN KEY (PROMO_CODE_ID_FK) REFERENCES promo_code_dim (PROMO_CODE_ID_PK) NOT ENFORCED;




alter table SALES_DWH.CONSUMPTION.PROMO_CODE_DIM rename column PROMO_CODE to PROMOTION_CODE ;

select *  from SALES_DWH.CONSUMPTION.SALES_FACT limit 10 ;

ls @sales_dwh.source.my_internal_stg;

select count(*) from sales_dwh.source.in_sales_order ; -- 33911
