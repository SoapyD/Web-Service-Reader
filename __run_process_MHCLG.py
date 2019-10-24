import os

exec(open("_main2.py").read())


delete_staging = True
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 7



#start_date = datetime.datetime(2019, 10, 22)
#end_date = datetime.datetime(2019, 10, 25)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



process_list = [
	['MHCLG','sys_user',False, None],
	['MHCLG','incident_task',False, None],
	['MHCLG','sc_request',False, None],
	['MHCLG','sc_task',False, None],
	['MHCLG','change_request',False, None],
	['MHCLG','change_task',False, None],
	['MHCLG','problem',False, None],
	['MHCLG','problem_task',False, None],
	['MHCLG','task_sla',False, None],
	['MHCLG','sc_cat_item',False, None],	
	['MHCLG','incident',True, "mhclg_incident"],
	['MHCLG','sc_req_item',True, "mhclg_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("MHCLG", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)

