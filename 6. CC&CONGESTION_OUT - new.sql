WITH range_values AS (
  SELECT date_trunc('day', min(time_start)) as minval,
         date_trunc('day', max(time_start)) as maxval
  FROM cdr
  WHERE 
	EXTRACT ('year' FROM time_start)=2023
	AND EXTRACT('month' FROM time_start) = 1			--<<< CHANGE MONTH
),

day_range AS (
  SELECT generate_series(minval, maxval, '1 day'::interval) as day
  FROM range_values
),

daily_all_calls AS (
  SELECT date_trunc('day', time_start) as day, customer_id, customer_auth_id, vendor_id, pop_id, internal_disconnect_code, count(*) as c
  FROM cdr
  WHERE 
	EXTRACT ('year' FROM time_start)=2023
	AND EXTRACT('month' FROM time_start) = 1			--<<< CHANGE MONTH
  	AND (vendor_id in (1,2,3,26)  or customer_id in (1,2,3,26))
  	AND (pop_id in (4,5,7)  or customer_auth_id in (20092,20095,20087,20101,20102,20151,20141,20150,20142,20132))
  GROUP BY 1,2,3,4,5,6
),

daily_connected_calls AS (
  SELECT day, customer_id, customer_auth_id, vendor_id, pop_id, sum(COALESCE (c, 0)) as cc
  FROM daily_all_calls
  WHERE internal_disconnect_code in (200, 480, 486, 487)
  GROUP BY 1,2,3,4,5
),

daily_failed_calls AS (
  SELECT day, customer_id, customer_auth_id, vendor_id, pop_id, sum(COALESCE (c, 0)) as cc
  FROM daily_all_calls
  WHERE internal_disconnect_code NOT in (200, 480, 486, 487)
  GROUP BY 1,2,3,4,5
),


--Connected Calls Out --MTN, AIRTEL, 9MOBILE, AND GLO as vendors
 daily_connected_calls_out AS (
SELECT day, vendor_id, pop_id , sum(COALESCE (cc, 0)) AS cc_out
FROM daily_connected_calls AS dcc
WHERE vendor_id IN (1,2,3,26) and pop_id  in (4,5,7)
GROUP BY 1,2,3
),

 daily_failed_calls_out AS (
SELECT day, vendor_id, pop_id , sum(COALESCE (cc, 0)) AS cong_out
FROM daily_failed_calls AS dfc
WHERE vendor_id IN (1,2,3,26) and pop_id  in (4,5,7)
GROUP BY 1,2,3
),

--MTN, AIRTEL, 9MOBILE, AND GLO FOR LAGOS 
MTN_LAG AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS MTN_LAG_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 4 AND vendor_id = 1
GROUP BY 1,2,3
),
MTN_LAG_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS MTN_LAG_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 4 AND vendor_id = 1
GROUP BY 1,2,3
),


AIRTEL_LAG AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS AIRTEL_LAG_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 4 AND vendor_id = 2
GROUP BY 1,2,3
),
AIRTEL_LAG_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS AIRTEL_LAG_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 4 AND vendor_id = 2
GROUP BY 1,2,3
),


NINE_MOBILE_LAG AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS NINE_MOBILE_LAG_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 4 AND vendor_id = 3
GROUP BY 1,2,3
),
NINE_MOBILE_LAG_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS NINE_MOBILE_LAG_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 4 AND vendor_id = 3
GROUP BY 1,2,3
),

GLO_LAG AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS GLO_LAG_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 4 AND vendor_id = 26
GROUP BY 1,2,3
),
GLO_LAG_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS GLO_LAG_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 4 AND vendor_id = 26
GROUP BY 1,2,3
),


--MTN AND AIRTEL FOR ABUJA

MTN_ABJ AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS MTN_ABJ_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 5 AND vendor_id = 1
GROUP BY 1,2,3
),
MTN_ABJ_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS MTN_ABJ_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 5 AND vendor_id = 1
GROUP BY 1,2,3
),


AIRTEL_ABJ AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS AIRTEL_ABJ_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 5 AND vendor_id = 2
GROUP BY 1,2,3
),
AIRTEL_ABJ_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS AIRTEL_ABJ_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 5 AND vendor_id = 2
GROUP BY 1,2,3
),


--MTN AND AIRTEL FOR ASABA
MTN_ASABA AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS MTN_ASABA_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 7 AND vendor_id = 1
GROUP BY 1,2,3
),
MTN_ASABA_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS MTN_ASABA_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 7 AND vendor_id = 1
GROUP BY 1,2,3
),


AIRTEL_ASABA AS (
SELECT day,vendor_id, pop_id, sum(cc_out) AS AIRTEL_ASABA_CC_OUT
FROM daily_connected_calls_out
WHERE pop_id = 7 AND vendor_id = 2
GROUP BY 1,2,3
),
AIRTEL_ASABA_CONG AS (
SELECT day,vendor_id, pop_id, sum(cong_out) AS AIRTEL_ASABA_CONG_OUT
FROM daily_failed_calls_out
WHERE pop_id = 7 AND vendor_id = 2
GROUP BY 1,2,3
),

--FINAL RESULT/OUTPUT. OUTPUT CAN BE MODIFIED TO CONFIRM JOINS
Final_CC_OUT AS (
SELECT 
	day_range.day AS day_CC,
	ROUND(MTN_LAG_CC_OUT,2) AS MTN_LAG_CC_OUT, ROUND(AIRTEL_LAG_CC_OUT,2) AS AIRTEL_LAG_CC_OUT, ROUND(NINE_MOBILE_LAG_CC_OUT,2) AS NINE_MOBILE_LAG_CC_OUT, ROUND(GLO_LAG_CC_OUT,2) AS GLO_LAG_CC_OUT, --LAGOS
	ROUND(MTN_ABJ_CC_OUT,2) AS MTN_ABJ_CC_OUT, ROUND(AIRTEL_ABJ_CC_OUT,2) AS AIRTEL_ABJ_CC_OUT, --ABUJA
	ROUND(MTN_ASABA_CC_OUT,2) AS MTN_ASABA_CC_OUT, ROUND(AIRTEL_ASABA_CC_OUT,2) AS AIRTEL_ASABA_CC_OUT  --ASABA
FROM day_range
	LEFT JOIN MTN_LAG ON day_range.day = MTN_LAG.day
	LEFT JOIN AIRTEL_LAG ON AIRTEL_LAG.day = day_range.day
	LEFT JOIN NINE_MOBILE_LAG ON NINE_MOBILE_LAG.day = day_range.day
	LEFT JOIN GLO_LAG ON GLO_LAG.day = day_range.day
	LEFT JOIN MTN_ABJ ON MTN_ABJ.day = day_range.day
	LEFT JOIN AIRTEL_ABJ ON AIRTEL_ABJ.day = day_range.day
	LEFT JOIN MTN_ASABA ON MTN_ASABA.day = day_range.day
	LEFT JOIN AIRTEL_ASABA ON AIRTEL_ASABA.day = day_range.day
),

Final_CONG_OUT AS (
SELECT 
	day_range.day AS day_CONG,
	ROUND(MTN_LAG_CONG_OUT,2) AS MTN_LAG_CONG_OUT, ROUND(AIRTEL_LAG_CONG_OUT,2) AS AIRTEL_LAG_CONG_OUT, ROUND(NINE_MOBILE_LAG_CONG_OUT,2) AS NINE_MOBILE_LAG_CONG_OUT, ROUND(GLO_LAG_CONG_OUT,2) AS GLO_LAG_CONG_OUT, --LAGOS
	ROUND(MTN_ABJ_CONG_OUT,2) AS MTN_ABJ_CONG_OUT, ROUND(AIRTEL_ABJ_CONG_OUT,2) AS AIRTEL_ABJ_CONG_OUT, --ABUJA
	ROUND(MTN_ASABA_CONG_OUT,2) AS MTN_ASABA_CONG_OUT, ROUND(AIRTEL_ASABA_CONG_OUT,2) AS AIRTEL_ASABA_CONG_OUT  --ASABA
FROM day_range
	LEFT JOIN MTN_LAG_CONG ON day_range.day = MTN_LAG_CONG.day
	LEFT JOIN AIRTEL_LAG_CONG ON AIRTEL_LAG_CONG.day = day_range.day
	LEFT JOIN NINE_MOBILE_LAG_CONG ON NINE_MOBILE_LAG_CONG.day = day_range.day
	LEFT JOIN GLO_LAG_CONG ON GLO_LAG_CONG.day = day_range.day
	LEFT JOIN MTN_ABJ_CONG ON MTN_ABJ_CONG.day = day_range.day
	LEFT JOIN AIRTEL_ABJ_CONG ON AIRTEL_ABJ_CONG.day = day_range.day
	LEFT JOIN MTN_ASABA_CONG ON MTN_ASABA_CONG.day = day_range.day
	LEFT JOIN AIRTEL_ASABA_CONG ON AIRTEL_ASABA_CONG.day = day_range.day
)

SELECT * 
FROM Final_CC_OUT
FULL JOIN Final_CONG_OUT ON Final_CC_OUT.day_CC = Final_CONG_OUT.day_CONG

