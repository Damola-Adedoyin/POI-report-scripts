WITH selected_range AS (
SELECT
	rccr.id
	,created_at :: DATE
	,date_start :: DATE AS date_start
	,date_end :: DATE
	,group_by
FROM reports.cdr_custom_report AS rccr
WHERE 
	EXTRACT ('year' FROM date_start)=2023
	AND EXTRACT('month' FROM date_start) = 2  --CHANGE MONTH HERE
	AND EXTRACT('day' FROM date_start) BETWEEN 11 AND 15
	AND (created_at::date - date_start::date) <= 2
ORDER BY EXTRACT('day' FROM date_start)
),

--MTN, AIRTEL, 9MOBILE, AND GLO as Customers
 selected_customer AS (
SELECT rccrd.id, rccrd.report_id, customer_id, customer_auth_id, COALESCE (agg_calls_count,  0) AS agg_calls_count, agg_customer_calls_duration, agg_calls_duration, agg_calls_acd, COALESCE (agg_asr_origination,  0) as agg_asr_origination, agg_asr_termination 	
FROM reports.cdr_custom_report_data AS rccrd

LEFT JOIN selected_range
	ON selected_range.id = rccrd.report_id
WHERE 
	customer_id IN (1,2,3,7,26) 
	AND cast(selected_range.group_by as varchar) LIKE  '%customer%customer_auth%'
),

--MTN, AIRTEL, 9MOBILE, AND GLO FOR LAGOS 
MTN_LAG AS (
SELECT report_id,customer_id, customer_auth_id, agg_calls_count AS MTN_LAG_calls_count, (agg_asr_origination * 100) AS MTN_LAG_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20092
),
AIRTEL_LAG AS (
SELECT report_id,customer_id, customer_auth_id, agg_calls_count AS AIRTEL_LAG_calls_count, (agg_asr_origination * 100) AS AIRTEL_LAG_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20095
),
NINE_MOBILE_LAG AS (
SELECT report_id,customer_id, customer_auth_id, agg_calls_count AS NINE_MOBILE_LAG_calls_count, (agg_asr_origination * 100) AS NINE_MOBILE_LAG_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20087
),
GLO_LAG AS (
SELECT report_id, customer_id, customer_auth_id, agg_calls_count AS GLO_LAG_calls_count, (agg_asr_origination * 100) AS GLO_LAG_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20132
),
--MTN AND AIRTEL FOR ABUJA

MTN_ABJ AS (
SELECT report_id,customer_id, customer_auth_id, agg_calls_count AS MTN_ABJ_calls_count, (agg_asr_origination * 100) AS MTN_ABJ_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20101
),
AIRTEL_ABJ AS (
SELECT report_id, customer_id, customer_auth_id, agg_calls_count AS AIRTEL_ABJ_calls_count, agg_asr_origination * 100 AS AIRTEL_ABJ_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20102
),
--MTN AND AIRTEL FOR ASABA
MTN_ASABA_ANON AS (
SELECT report_id, COALESCE (agg_calls_count,  0) AS MTN_ASABA_ANON_calls_count, COALESCE ((agg_asr_origination * 100),0)  AS MTN_ASABA_ANON_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20151
),

MTN_ASABA AS (
SELECT report_id,customer_id, customer_auth_id, COALESCE (agg_calls_count,  0) AS MTN_ASABA_calls_count, COALESCE (agg_asr_origination,  0) * 100 AS MTN_ASABA_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20141
),
AIRTEL_ASABA_ANON AS (
SELECT report_id, COALESCE (agg_calls_count,  0) AS AIRTEL_ASABA_ANON_calls_count, COALESCE (agg_asr_origination,  0) * 100 AS AIRTEL_ASABA_ANON_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20150
),
AIRTEL_ASABA AS (
SELECT report_id,customer_id, customer_auth_id, COALESCE (agg_calls_count,  0) AS AIRTEL_ASABA_calls_count, COALESCE (agg_asr_origination,  0) * 100 AS AIRTEL_ASABA_ASR_IN
FROM selected_customer
WHERE customer_auth_id = 20142
),

--FINAL RESULT/OUTPUT. OUTPUT CAN BE MODIFIED TO CONFIRM JOINS
ASR_IN AS (
SELECT DISTINCT on (date_start)
	ROUND(COALESCE (MTN_ASABA_ANON_ASR_IN,  0),2) AS MTN_ASABA_ANON_ASR_IN, ROUND(MTN_ASABA_ASR_IN,2) AS MTN_ASABA_ASR_IN,
	ROUND(COALESCE (AIRTEL_ASABA_ANON_ASR_IN,  0),2) AS AIRTEL_ASABA_ANON_ASR_IN, ROUND(AIRTEL_ASABA_ASR_IN,2) AS AIRTEL_ASABA_ASR_IN,
	date_start, selected_range.id AS ID,
	MTN_LAG_calls_count, AIRTEL_LAG_calls_count, NINE_MOBILE_LAG_calls_count, GLO_LAG_calls_count,
	MTN_ABJ_calls_count, AIRTEL_ABJ_calls_count,
	MTN_ASABA_ANON_calls_count, MTN_ASABA_calls_count, AIRTEL_ASABA_ANON_calls_count, AIRTEL_ASABA_calls_count,
	ROUND(MTN_LAG_ASR_IN,2) AS MTN_LAG_ASR_IN, ROUND(AIRTEL_LAG_ASR_IN,2) AS AIRTEL_LAG_ASR_IN, ROUND(NINE_MOBILE_LAG_ASR_IN,2) AS NINE_MOBILE_LAG_ASR_IN, ROUND(GLO_LAG_ASR_IN,2) AS GLO_LAG_ASR_IN, 
	ROUND(MTN_ABJ_ASR_IN,2) AS MTN_ABJ_ASR_IN, ROUND(AIRTEL_ABJ_ASR_IN,2) AS AIRTEL_ABJ_ASR_IN,
	ROUND((((COALESCE (MTN_ASABA_ASR_IN,  0))*(COALESCE (MTN_ASABA_calls_count,  0))) + ((COALESCE (MTN_ASABA_ANON_ASR_IN,  0)) * (COALESCE (MTN_ASABA_ANON_calls_count))))/((COALESCE (MTN_ASABA_calls_count,  0)) + (COALESCE (MTN_ASABA_ANON_calls_count,  0))),2)  AS AVG_MTN_ASABA_ASR_IN,
	ROUND(((AIRTEL_ASABA_ASR_IN*AIRTEL_ASABA_calls_count) + (AIRTEL_ASABA_ANON_ASR_IN * AIRTEL_ASABA_ANON_calls_count))/(AIRTEL_ASABA_calls_count + AIRTEL_ASABA_ANON_calls_count),2)  AS AVG_AIRTEL_ASABA_ASR_IN 
FROM MTN_LAG
	LEFT JOIN AIRTEL_LAG ON AIRTEL_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN NINE_MOBILE_LAG ON NINE_MOBILE_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN GLO_LAG ON GLO_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN MTN_ABJ ON MTN_ABJ.report_id = MTN_LAG.report_id
	LEFT JOIN AIRTEL_ABJ ON AIRTEL_ABJ.report_id = MTN_LAG.report_id
	LEFT JOIN MTN_ASABA ON MTN_ASABA.report_id = MTN_LAG.report_id
		LEFT JOIN MTN_ASABA_ANON ON MTN_ASABA_ANON.report_id = MTN_LAG.report_id
	LEFT JOIN AIRTEL_ASABA ON AIRTEL_ASABA.report_id = MTN_LAG.report_id
		LEFT JOIN AIRTEL_ASABA_ANON ON AIRTEL_ASABA_ANON.report_id = MTN_LAG.report_id
	LEFT JOIN selected_range ON selected_range.id = MTN_LAG.report_id
ORDER BY date_start
)

SELECT DISTINCT ON (date_start) date_start, id, 
	MTN_LAG_ASR_IN, AIRTEL_LAG_ASR_IN,NINE_MOBILE_LAG_ASR_IN, GLO_LAG_ASR_IN, 
	MTN_ABJ_ASR_IN,AIRTEL_ABJ_ASR_IN,
	MTN_ASABA_ASR_IN, AIRTEL_ASABA_ASR_IN
FROM ASR_IN

