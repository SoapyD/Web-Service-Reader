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
	ws_process_class('ENWL','Employee'),
	#['ENWL','pir',False, None],
	#['ENWL','change',False, None],
	ws_process_class('ENWL','Frs_data_escalation_watch'),
	ws_process_class('ENWL','Task'),
	ws_process_class('ENWL','Problem', True, "enwl_problem"),
	ws_process_class('ENWL','ServiceReq',True, "enwl_request"),
	ws_process_class('ENWL','Incident',True, "enwl_incident"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("ENWL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

