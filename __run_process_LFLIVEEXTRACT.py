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
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=4.0)
end_date = now.replace(microsecond=0) + datetime.timedelta(hours=1.0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################LFLIVEEXTRACT
##############################################################################################################################################################
##############################################################################################################################################################

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

