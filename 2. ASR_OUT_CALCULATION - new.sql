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
	AND EXTRACT('month' FROM date_start) = 2  --CHANGE MONTH HERE										----------CHANGE MONTH
	AND (created_at::date - date_start::date) <= 2
ORDER BY EXTRACT('day' FROM date_start)
),
--MTN, AIRTEL 9MOBILE AND GLO as vendors
 selected_vendor AS (
SELECT rccrd.id, rccrd.report_id, vendor_id, pop_id, COALESCE (agg_calls_count,0) AS agg_calls_count, agg_vendor_calls_duration, agg_calls_duration, agg_calls_acd, COALESCE (agg_asr_origination,0) AS agg_asr_origination, COALESCE (agg_asr_termination,0) AS agg_asr_termination	
FROM reports.cdr_custom_report_data AS rccrd

LEFT JOIN selected_range
	ON selected_range.id = rccrd.report_id

WHERE 
	vendor_id IN (1,2,3,26) 
	AND cast(selected_range.group_by as varchar) LIKE  '%vendor%pop%'
	OR cast(selected_range.group_by as varchar) LIKE  '%pop%vendor%'
),
--MTN, AIRTEL, 9MOBILE, AND GLO FOR LAGOS 
MTN_LAG AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS MTN_LAG_ASR_OUT
FROM selected_vendor
WHERE pop_id = 4 AND vendor_id = 1
),
AIRTEL_LAG AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS AIRTEL_LAG_ASR_OUT
FROM selected_vendor
WHERE pop_id = 4 AND vendor_id = 2
),
NINE_MOBILE_LAG AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS NINE_MOBILE_LAG_ASR_OUT
FROM selected_vendor
WHERE pop_id = 4 AND vendor_id = 3
),
GLO_LAG AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS GLO_LAG_ASR_OUT
FROM selected_vendor
WHERE pop_id = 4 AND vendor_id = 26
),
--MTN AND AIRTEL FOR ABUJA

MTN_ABJ AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS MTN_ABJ_ASR_OUT
FROM selected_vendor
WHERE pop_id = 5 AND vendor_id = 1
),
AIRTEL_ABJ AS (
SELECT report_id, vendor_id, pop_id, agg_asr_origination * 100 AS AIRTEL_ABJ_ASR_OUT
FROM selected_vendor
WHERE pop_id = 5 AND vendor_id = 2
),
--MTN AND AIRTEL FOR ASABA
MTN_ASABA AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS MTN_ASABA_ASR_OUT
FROM selected_vendor
WHERE pop_id = 7 AND vendor_id = 1
),
AIRTEL_ASABA AS (
SELECT report_id,vendor_id, pop_id, (agg_asr_origination * 100) AS AIRTEL_ASABA_ASR_OUT
FROM selected_vendor
WHERE pop_id = 7 AND vendor_id = 2
)

--FINAL RESULT/OUTPUT. OUTPUT CAN BE MODIFIED TO CONFIRM JOINS
SELECT DISTINCT on (date_start)
	date_start, selected_range.id, 
	ROUND(MTN_LAG_ASR_OUT, 2) AS MTN_LAG_ASR_OUT, ROUND(AIRTEL_LAG_ASR_OUT,2) AS AIRTEL_LAG_ASR_OUT, ROUND(NINE_MOBILE_LAG_ASR_OUT,2) AS NINE_MOBILE_LAG_ASR_OUT, ROUND(GLO_LAG_ASR_OUT,2) AS GLO_LAG_ASR_OUT, --LAGOS
	ROUND(MTN_ABJ_ASR_OUT,2) AS MTN_ABJ_ASR_OUT, ROUND(AIRTEL_ABJ_ASR_OUT,2) AS AIRTEL_ABJ_ASR_OUT, --ABUJA
	ROUND(MTN_ASABA_ASR_OUT,2) AS MTN_ASABA_ASR_OUT, ROUND(AIRTEL_ASABA_ASR_OUT,2) AS AIRTEL_ASABA_ASR_OUT  --ASABA
FROM MTN_LAG
	LEFT JOIN AIRTEL_LAG ON AIRTEL_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN NINE_MOBILE_LAG ON NINE_MOBILE_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN GLO_LAG ON GLO_LAG.report_id = MTN_LAG.report_id
	LEFT JOIN MTN_ABJ ON MTN_ABJ.report_id = MTN_LAG.report_id
	LEFT JOIN AIRTEL_ABJ ON AIRTEL_ABJ.report_id = MTN_LAG.report_id
	LEFT JOIN MTN_ASABA ON MTN_ASABA.report_id = MTN_LAG.report_id
	LEFT JOIN AIRTEL_ASABA ON AIRTEL_ASABA.report_id = MTN_LAG.report_id
	LEFT JOIN selected_range ON selected_range.id = MTN_LAG.report_id
ORDER BY date_start

