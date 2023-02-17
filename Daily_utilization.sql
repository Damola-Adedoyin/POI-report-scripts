
With orig_table_range as(
select * from stats.active_call_orig_gateways  
WHERE EXTRACT ('year' FROM created_at) = 2023 
	AND EXTRACT('month' FROM created_at) = 
	AND EXTRACT('day' FROM created_at) = 14		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by created_at 
),
term_table_range as(
select * from stats.active_call_term_gateways  
WHERE EXTRACT ('year' FROM created_at) = 2023 
	AND EXTRACT('month' FROM created_at) = 2
	AND EXTRACT('day' FROM created_at) = 14		--REMEMBER TO CHANGE DATE HERE
	AND gateway_id in (29,30, 35,36, 45,48, 47,46, 111,112, 114,115)
order by created_at 
),

count_orig_term_join as (
select orig_table_range.gateway_id as orig_id, term_table_range.gateway_id as term_id
		,max(orig_table_range.count) as orig_max_count, max(term_table_range.count) as term_max_count
from orig_table_range
inner join term_table_range on orig_table_range.gateway_id = term_table_range.gateway_id
group by orig_table_range.gateway_id, term_table_range.gateway_id
),

utilization as (
	select  
	(
		select created_at :: date from orig_table_range limit 1
	) as created_date,
	
	(
		select created_at :: date from term_table_range limit 1
	) as confirm_created_date,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =29) 
		+ (select term_max_count from count_orig_term_join where term_id =30)) * 100/ 10016 ::decimal 
	) as mtn_lag_UT,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =35) 
		+ (select term_max_count from count_orig_term_join where term_id =36)) * 100/ 10016 ::decimal 
	) as airtel_lag_UT,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =45) 
		+ (select term_max_count from count_orig_term_join where term_id =48)) * 100/ 6000 ::decimal
	) as mtn_abj_UT,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =47) 
		+ (select term_max_count from count_orig_term_join where term_id =46)) * 100/ 7232 ::decimal
	) as airtel_abj_UT,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =111) 
		+ (select term_max_count from count_orig_term_join where term_id =112)) * 100/ 6000 ::decimal
	) as mtn_asb_UT,
	
	(
		((select orig_max_count from count_orig_term_join where orig_id =114) 
		+ (select term_max_count from count_orig_term_join where term_id =115)) * 100/ 8000 ::decimal
	) as airtel_asb_UT
)

select * from utilization;


