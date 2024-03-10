/**************************
  Код для PostgreSQL
***************************/

CREATE TABLE fifo_example (agrmnt text,
						   period DATE,
						   agrmnt_sum int,
						   type_operation character(1));
						  
-- Генерируем данные
do 
$$
declare 
 agr record;
 pr record;
begin
-- Пишем начисления
 delete from fifo_example; 
 for agr in (with recursive tmp(id, agrmnt, agrmnt_sum, type_operation) as 
						  (select 0 id, '' agrmnt, 0::float agrmnt_sum, '' type_operation
 			  			    union all
						   select t.id, '001/01/'||string_agg(substr('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(round(random()*25)+1)::int,1),'') agrmnt, round(random()*1000+1000) agrmnt_sum, 'A' type_operation 
 			  			     from pg_catalog.generate_series(1, 9)
 			  			    cross join (select id+1 id from tmp) t
 			  			    where id <= 2
 			  			    group by t.id)
 			 select agrmnt, agrmnt_sum, type_operation from tmp where id > 0)
 loop
  for pr in (select period
  	  		   from pg_catalog.generate_series(date_trunc('years',current_date), 
  								  			   date_trunc('years',current_date) + interval '12 months - 1 day', 
  								  			   interval  '1 months') as period)
  loop
	insert into fifo_example values (agr.agrmnt, pr.period, agr.agrmnt_sum, agr.type_operation);   	
  end loop; 
 end loop;

 -- Пишем оплаты, делаем так что может образоваться по итогу оплат либо перебор либо недобор
 for agr in (select agrmnt, period + (round(random()*19)+1)::int as period, round(abs(random()*200+(agrmnt_sum-100))) agrmnt_sum, 'P' type_operation  from fifo_example)
 loop
  insert into fifo_example values (agr.agrmnt, agr.period, agr.agrmnt_sum, agr.type_operation);   	
 end loop;  
 commit;
end;
$$

SELECT agrmnt, type_operation, sum(agrmnt_sum) agrmnt_sum FROM fifo_example GROUP BY agrmnt, type_operation order by agrmnt, type_operation;

-- Алгортим, специально разбит по шагам что бы можно было посмотреть результат каждого действия
DROP TABLE FIFO_EXAMPLE_RES;
CREATE TABLE FIFO_EXAMPLE_RES as
WITH tmp_accrual AS (SELECT fe.*,
							sum(fe.agrmnt_sum) OVER (PARTITION BY agrmnt ORDER BY period) agrmnt_sum_sum
					   FROM fifo_example fe
					  WHERE type_operation = 'A'),
	 tmp_payment AS (SELECT fe.*,
							sum(fe.agrmnt_sum) OVER (PARTITION BY agrmnt ORDER BY period) agrmnt_sum_sum
					   FROM fifo_example fe
					  WHERE type_operation = 'P'),
	  tmp_step_1 AS (SELECT coalesce(ta.agrmnt,tp.agrmnt) agrmnt,
						    ta.period period_acr,
						    ta.agrmnt_sum agrmnt_sum_acr,
						    tp.period period_pr,
						    tp.agrmnt_sum agrmnt_sum_pr,
						    ta.agrmnt_sum_sum agrmnt_sum_sum_acr,
						    tp.agrmnt_sum_sum agrmnt_sum_sum_pr
					   FROM tmp_accrual ta
					   FULL JOIN tmp_payment tp ON (ta.agrmnt = tp.agrmnt AND
					   							    ta.agrmnt_sum_sum = tp.agrmnt_sum_sum)
					  ORDER BY coalesce(ta.agrmnt,tp.agrmnt), coalesce(ta.agrmnt_sum_sum,tp.agrmnt_sum_sum)), -- сортировка для наглядности результата данного действия	
	  tmp_step_2 AS (SELECT ts1.agrmnt,
					 	    last_value(period_acr) OVER (PARTITION BY agrmnt, gb_acr ORDER BY coalesce(ts1.agrmnt_sum_sum_acr,ts1.agrmnt_sum_sum_pr) ROWS  BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) period_acr,
						    last_value(agrmnt_sum_acr) OVER (PARTITION BY agrmnt, gb_acr ORDER BY coalesce(ts1.agrmnt_sum_sum_acr,ts1.agrmnt_sum_sum_pr) ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) agrmnt_sum_acr,
						    last_value(period_pr) OVER (PARTITION BY agrmnt, gb_pr ORDER BY coalesce(ts1.agrmnt_sum_sum_acr,ts1.agrmnt_sum_sum_pr) ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) period_pr,
						    last_value(agrmnt_sum_pr) OVER (PARTITION BY agrmnt, gb_pr ORDER BY coalesce(ts1.agrmnt_sum_sum_acr,ts1.agrmnt_sum_sum_pr) ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) agrmnt_sum_pr,
						    coalesce(agrmnt_sum_sum_acr,agrmnt_sum_sum_pr) agrmnt_sum_sum
					   FROM (select ts1.*,
					   				sum(case when agrmnt_sum_acr is not null then 1 end) over (partition by agrmnt order by coalesce(agrmnt_sum_sum_acr,agrmnt_sum_sum_pr) desc) gb_acr,
					   				sum(case when agrmnt_sum_pr is not null then 1 end) over (partition by agrmnt order by coalesce(agrmnt_sum_sum_acr,agrmnt_sum_sum_pr) desc) gb_pr
	   		   				   from tmp_step_1 ts1) ts1),
	  tmp_step_3 AS (SELECT ts2.agrmnt,
					 	    ts2.period_acr,
						    ts2.agrmnt_sum_acr,
						    ts2.period_pr,
						    ts2.agrmnt_sum_pr,
						    ts2.agrmnt_sum_sum - coalesce(lag(ts2.agrmnt_sum_sum) OVER (PARTITION BY ts2.agrmnt ORDER BY agrmnt_sum_sum),0) agrmnt_sum_new
					   FROM tmp_step_2 ts2),
      tmp_step_4 AS (SELECT ts3.agrmnt,
					  	    ts3.period_acr,
						    CASE WHEN ts3.agrmnt_sum_acr IS NOT NULL THEN ts3.agrmnt_sum_new END agrmnt_sum_acr,
						    ts3.period_pr,
						    CASE WHEN ts3.agrmnt_sum_pr IS NOT NULL THEN ts3.agrmnt_sum_new END agrmnt_sum_pr
					   FROM tmp_step_3 ts3)
SELECT * FROM tmp_step_4;

-- Проверяем
  WITH tmp_data_res AS (SELECT agrmnt, sum(agrmnt_sum_acr) agrmnt_sum_acr_fifo, sum(agrmnt_sum_pr) agrmnt_sum_pr_fifo
					 	  FROM FIFO_EXAMPLE_RES
					 	 GROUP BY agrmnt),
 	    tmp_data_ex AS (SELECT agrmnt, 
 	    					   sum(agrmnt_sum) filter (where type_operation = 'A') agrmnt_sum_acr,
 	    					   sum(agrmnt_sum) filter (where type_operation = 'P') agrmnt_sum_pr	   
 	    			      FROM fifo_example
   					  	 group by agrmnt)
 SELECT res.*, ex.* 
   FROM tmp_data_res res
   JOIN tmp_data_ex ex ON (res.agrmnt = ex.agrmnt);
   
