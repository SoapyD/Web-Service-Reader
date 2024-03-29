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
	ws_process_class('FSA','sys_user'),
	ws_process_class('FSA','incident_task'),
	ws_process_class('FSA','sc_request'),
	ws_process_class('FSA','sc_task'),
	ws_process_class('FSA','change_request'),
	ws_process_class('FSA','change_task'),
	###['FSA','task_sla',False, None],
	ws_process_class('FSA','sc_cat_item'),	
	ws_process_class('FSA','problem',True, "fsa_problem"),
	ws_process_class('FSA','problem_task', True, "fsa_problem_task"),
	ws_process_class('FSA','incident',True, "fsa_incident"),
	ws_process_class('FSA','sc_req_item',True, "fsa_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("FSA", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

