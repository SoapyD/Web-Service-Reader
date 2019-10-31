import os

exec(open("_main2.py").read())

db = 1 #REPORTING DATABASE
database = ''

delete_staging = True
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 7



#start_date = datetime.datetime(2019, 10, 22)
#end_date = datetime.datetime(2019, 10, 25)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
end_date = now.replace(microsecond=0) + datetime.timedelta(hours=1.0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS


process_list = [
	ws_process_class('HE','sys_user'),
	ws_process_class('HE','incident_task'),
	ws_process_class('HE','incident_alert'),
	ws_process_class('HE','sc_request'),
	ws_process_class('HE','sc_task'),
	ws_process_class('HE','change_request'),
	ws_process_class('HE','change_task'),
	ws_process_class('HE','problem'),
	ws_process_class('HE','problem_task'),
	ws_process_class('HE','task_sla'),
	ws_process_class('HE','sc_cat_item'),
	ws_process_class('HE_KB','kb_knowledge'),	
	ws_process_class('HE','incident',True, "he_incident"),
	ws_process_class('HE','sc_req_item',True, "he_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

