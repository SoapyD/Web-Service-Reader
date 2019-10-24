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
	['HE','sys_user',False, None],
	['HE','incident_task',False, None],
	['HE','incident_alert',False, None],
	['HE','sc_request',False, None],
	['HE','sc_task',False, None],
	['HE','change_request',False, None],
	['HE','change_task',False, None],
	['HE','problem',False, None],
	['HE','problem_task',False, None],
	['HE','task_sla',False, None],
	['HE','sc_cat_item',False, None],
	['HE_KB','kb_knowledge',False, None],	
	['HE','incident',True, "he_incident"],
	['HE','sc_req_item',True, "he_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)

