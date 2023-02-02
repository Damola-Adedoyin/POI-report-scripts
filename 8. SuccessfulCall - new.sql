--SUCCESSFUL CALL :::::::::::::::::::::::::::::
WITH range_values AS (
  SELECT date_trunc('day', min(time_start)) as minval,
        date_trunc('day', max(time_start)) as maxval
  FROM cdr
  WHERE 
	EXTRACT ('year' FROM time_start)=2023
	AND EXTRACT('month' FROM time_start) = 1		--CHANGE MONTH HERE
	),  

day_range AS (
  SELECT generate_series(minval, maxval, '1 day'::interval) as day
  FROM range_values
),

successCount AS (
  SELECT date_trunc('day', time_start) as day,
        count(*) as successCount, customer_id, vendor_id, pop_id, sum(duration) as duration
  FROM cdr
  WHERE 
	EXTRACT ('year' FROM time_start)=20223
	AND EXTRACT('month' FROM time_start) = 1 	  --CHANGE MONTH HERE
  AND (vendor_id = 1 or vendor_id = 2 or vendor_id = 3 or vendor_id = 26 or customer_id = 1 or customer_id = 2 or customer_id = 3 or customer_id = 26) 
  and success
  and (extract(hour from time_start) < 22 and extract(hour from time_start) >= 19)
  GROUP BY 1,3,4,5
)

SELECT day_range.day,
      successCount.customer_id,
      successCount.vendor_id,
      successCount.successCount,
      successCount.pop_id,
      successCount.duration      
FROM day_range
LEFT OUTER JOIN successCount on day_range.day = successCount.day
order by day;

