/*  total available capacity used for calculation 

mtn_lag = 10,016
airtel_lag = 10,016

mtn_abj = 6,000
airtel_abj = 7,232

mtn_asb = 6,000
airtel_asb = 8,000      */

With orig_table_range as(
select * from stats.active_call_orig_gateways_hourly 
WHERE EXTRACT ('year' FROM calls_time) = 2023
	AND EXTRACT('month' FROM calls_time) = 2
	AND EXTRACT('day' FROM calls_time)  BETWEEN 11 AND 13		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by calls_time 
),

term_table_range as(
select * from stats.active_call_term_gateways_hourly  
WHERE EXTRACT ('year' FROM calls_time) = 2023
	AND EXTRACT('month' FROM calls_time) = 2
	AND EXTRACT('day' FROM calls_time) BETWEEN 11 AND 13		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by calls_time 
),

mtn_lag as(
	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as mtn_lag_max_orig_count, max(term_table_range.max_count) as mtn_lag_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 10016 :: decimal, 2) as mtn_lag_util
	from orig_table_range
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 29 and term_table_range.gateway_id = 30
	group by orig_date, term_date
	order by orig_date 
),

airtel_lag as(

	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as airtel_lag_max_orig_count, max(term_table_range.max_count) as airtel_lag_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 10016 :: decimal, 2) as airtel_lag_util
	from orig_table_range 
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 35 and term_table_range.gateway_id = 36
	group by orig_date, term_date
	order by orig_date 
),

mtn_abj as(
	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as mtn_abj_max_orig_count, max(term_table_range.max_count) as mtn_abj_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 6000 :: decimal, 2) as mtn_abj_util
	from orig_table_range 
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 45 and term_table_range.gateway_id = 48
	group by orig_date, term_date
	order by orig_date 
),

airtel_abj as(
	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as airtel_abj_max_orig_count, max(term_table_range.max_count) as airtel_abj_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 7232 :: decimal, 2) as airtel_abj_util
	from orig_table_range
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 47 and term_table_range.gateway_id = 46
	group by orig_date, term_date
	order by orig_date 
),

mtn_asb as(
	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as mtn_asb_max_orig_count, max(term_table_range.max_count) as mtn_asb_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 6000 :: decimal, 2) as mtn_asb_util
	from orig_table_range 
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 111 and term_table_range.gateway_id = 112
	group by orig_date, term_date
	order by orig_date 
),

airtel_asb as(
	select orig_table_range.calls_time :: date as orig_date, term_table_range.calls_time :: date as term_date,
		max(orig_table_range.max_count) as airtel_asb_max_orig_count, max(term_table_range.max_count) as airtel_asb_max_term_count, 
		ROUND(( max(orig_table_range.max_count) + max(term_table_range.max_count) ) * 100/ 8000 :: decimal, 2) as airtel_asb_util
	from orig_table_range
	join term_table_range on term_table_range.calls_time :: date = orig_table_range.calls_time :: date
	where orig_table_range.gateway_id = 114 and term_table_range.gateway_id = 115
	group by orig_date, term_date
	order by orig_date 
),

utilization_multi as (
	select mtn_lag.orig_date AS date, mtn_lag_util, airtel_lag_util, mtn_abj_util, airtel_abj_util, mtn_asb_util, airtel_asb_util
	from mtn_lag
	join airtel_lag on mtn_lag.orig_date = airtel_lag.orig_date
	
	join mtn_abj on mtn_lag.orig_date = mtn_abj.orig_date
	join airtel_abj on mtn_lag.orig_date = airtel_abj.orig_date
	
	join mtn_asb on mtn_lag.orig_date = mtn_asb.orig_date
	join airtel_asb on mtn_lag.orig_date = airtel_asb.orig_date
),


--select * from utilization_multi
--#############################################################################################################################################
--#############################################################################################################################################
--#############################################################################################################################################


 orig_table_range_ as(
select * from stats.active_call_orig_gateways  
WHERE EXTRACT ('year' FROM created_at) = 2023 
	AND EXTRACT('month' FROM created_at) = 2
	AND EXTRACT('day' FROM created_at) = 14		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by created_at 
),
term_table_range_ as(
select * from stats.active_call_term_gateways  
WHERE EXTRACT ('year' FROM created_at) = 2023 
	AND EXTRACT('month' FROM created_at) = 2
	AND EXTRACT('day' FROM created_at) = 14		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by created_at 
),

count_orig_term_join as (
select orig_table_range_.gateway_id as orig_id, term_table_range_.gateway_id as term_id
		,max(orig_table_range_.count) as orig_max_count, max(term_table_range_.count) as term_max_count
from orig_table_range_
inner join term_table_range_ on orig_table_range_.gateway_id = term_table_range_.gateway_id
group by orig_table_range_.gateway_id, term_table_range_.gateway_id
),

utilization as (
	select  
	/*(
		select created_at :: date from orig_table_range_ limit 1
	) as created_date,*/
	
	(
		select created_at :: date from term_table_range_ limit 1
	) as confirm_created_date,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =29) 
		+ (select term_max_count from count_orig_term_join where term_id =30)) * 100/ 10016 ::decimal, 2) 
	) as mtn_lag_UT,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =35) 
		+ (select term_max_count from count_orig_term_join where term_id =36)) * 100/ 10016 ::decimal, 2)
	) as airtel_lag_UT,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =45) 
		+ (select term_max_count from count_orig_term_join where term_id =48)) * 100/ 6000 ::decimal, 2)
	) as mtn_abj_UT,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =47) 
		+ (select term_max_count from count_orig_term_join where term_id =46)) * 100/ 7232 ::decimal, 2)
	) as airtel_abj_UT,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =111) 
		+ (select term_max_count from count_orig_term_join where term_id =112)) * 100/ 6000 ::decimal, 2)
	) as mtn_asb_UT,
	
	(
		ROUND(((select orig_max_count from count_orig_term_join where orig_id =114) 
		+ (select term_max_count from count_orig_term_join where term_id =115)) * 100/ 8000 ::decimal, 2)
	) as airtel_asb_UT
)


select * from utilization_multi

UNION ALL

select * from utilization;


