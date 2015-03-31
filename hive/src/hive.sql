drop table if exists stock_data;
CREATE EXTERNAL TABLE stock_data
(stock_date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume FLOAT, adj_closeprice FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///data'
tblproperties ("skip.header.line.count"="1");

drop table if exists stock_temp_1;
create table stock_temp_1 as
select 
regexp_replace(regexp_replace(INPUT__FILE__NAME,'.*/',''),'\\..*','') as stock_name, 
stock_date, 
substr(stock_date,0,7) as stock_month_year, 
adj_closeprice from stock_data;

drop table if exists stock_temp_2;
create table stock_temp_2 as select stock_name,min(stock_date) as min_month_date, max(stock_date) as max_month_date from stock_temp_1 group by stock_name,stock_month_year;


drop table if exists stock_temp_3;
create table stock_temp_3 as select t1.stock_name,t2.min_month_date, t2.max_month_date,t1.adj_closeprice as min_adjcloseprice,t1_other.adj_closeprice as max_adjcloseprice from  stock_temp_1 t1,stock_temp_2 t2,stock_temp_1 t1_other where t1.stock_name=t2.stock_name and t1.stock_date=t2.min_month_date and t1_other.stock_date=t2.max_month_date
and t1_other.stock_name=t2.stock_name;

drop table if exists stock_temp_4;
create table stock_temp_4 as 
select stock_name,(max_adjcloseprice-min_adjcloseprice)/min_adjcloseprice as xi_Values from  stock_temp_3 group by stock_name,max_adjcloseprice,min_adjcloseprice;

drop table if exists stock_temp_5;
create table stock_temp_5 as
select stock_name,stddev_samp(xi_values) as Variance from 
stock_temp_4 group by stock_name;


drop table if exists stock_temp_6;
create table stock_temp_6 as select stock_name,Variance, rank() over (order by Variance asc) as min_rank, rank() over (order by Variance desc) as max_rank from stock_temp_5 where Variance > 0.0 and Variance is not null;

