maindata = LOAD 'hdfs:///pigdata' USING PigStorage(',','-tagFile');
temp1 = FILTER maindata BY $1 neq 'Date';
temp2 = FOREACH temp1 GENERATE $0,$1,$7; 
temp3 = FOREACH temp2 generate $0 AS stock_name,SUBSTRING($1,0,7) AS stock_month_year,(float)SUBSTRING((chararray)$1,8,10) AS stock_day, (float)$2 AS adj_close;
temp4 = GROUP temp3 BY (stock_name,stock_month_year);
temp5 = FOREACH temp4 
{ dSort = order temp3 BY stock_day desc;
  max = LIMIT dSort 1;	
  ASort = order temp3 BY stock_day; 
  min = LIMIT ASort 1;	
  GENERATE flatten(max),flatten(min);
}; 
temp6 = FOREACH temp5 GENERATE $0 AS stock_name, $1 AS stock_month_year,$3 AS max_adj,$7 AS min_adj; 
temp7 = FOREACH temp6 GENERATE stock_name AS stock_name, stock_month_year AS stock_month_year,((max_adj-min_adj)/min_adj) AS xiValues;
temp8 = Group temp7 by stock_name;
temp9 = FOREACH temp8 GENERATE group,AVG(temp7.xiValues);
temp10 = JOIN temp9 BY $0,temp7 by stock_name;
temp11 = FOREACH temp10 GENERATE $0 AS stock_name,$3 As stock_month_year,($4-$1)*($4-$1) AS averageXiValues2;
temp12 = Group temp11 by stock_name;
temp13 = FOREACH temp12 GENERATE group,SUM(temp11.averageXiValues2),COUNT(temp11.averageXiValues2)-1 AS noOfMonths;
temp14 = FOREACH temp13 GENERATE $0 AS stock_name,$1 As averageXiValues2,$2 AS noOfMonths;
temp15 = FOREACH temp14 GENERATE $0 AS stock_name,SQRT(averageXiValues2/noOfMonths) AS Volatility;
FinalVolatility = FILTER temp15 BY Volatility != 0.0;
Max = Order FinalVolatility BY $1 desc;
Max_top10 = LIMIT Max 10;
Min = Order FinalVolatility BY $1;
Min_top10 = LIMIT Min 10;
Final_output = UNION Max_top10,Min_top10;
store Final_output into 'hdfs:///pigdata/wc_out';


 
