import os

exec(open("_main2.py").read())

#db = 3 #REPORTING DATABASE
#database = 'LF-SQL-DEV'
db = 1 #REPORTING DATABASE
database = ''

delete_staging = False
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 7


start_date = datetime.datetime(2019, 11, 6)
end_date = datetime.datetime(2019, 11, 8)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



##############################################################################################################################
##############################################################################################################################
########################################################TEST DATA
##############################################################################################################################
##############################################################################################################################

"""
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



process_list = [
	ws_process_class('LFLIVEEXTRACT','sessionincident'),
	ws_process_class('LFLIVEEXTRACT','he_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','fsa_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','mhclg_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','croydon_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','sessionpostback'),
	ws_process_class('LFLIVEEXTRACT','completedsurveyresponse'),
	ws_process_class('LFLIVEEXTRACT','session',True, "lfliveextract_session"),
	ws_process_class('LFLIVEEXTRACT','completedsurvey',True, "lfliveextract_nps"),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACT", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


"""


start_date = datetime.datetime(2019, 11, 11)
end_date = datetime.datetime(2019, 11, 23)

"""
start_date = now.replace(hour=0)
start_date = start_date.replace(minute=0)
start_date = start_date.replace(second=0)
start_date = start_date.replace(microsecond=0)

end_date = start_date + datetime.timedelta(days=1.0)
"""


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################RINGCENTRAL
##############################################################################################################################################################
##############################################################################################################################################################


process_list = [
	ws_process_class('RINGCENTRAL','agents'),
	ws_process_class('RINGCENTRAL','completedcontacts'),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

"""
generate_creation_query('RINGCENTRAL', 'completedcontacts')
"""
