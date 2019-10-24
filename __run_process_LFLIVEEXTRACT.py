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


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################LFLIVEEXTRACT
##############################################################################################################################################################
##############################################################################################################################################################

process_list = [
	['LFLIVEEXTRACT','sessionincident',False, None],
	['LFLIVEEXTRACT','he_sessionincident',False, None],
	['LFLIVEEXTRACT','fsa_sessionincident',False, None],
	['LFLIVEEXTRACT','mhclg_sessionincident',False, None],
	['LFLIVEEXTRACT','croydon_sessionincident',False, None],
	['LFLIVEEXTRACT','sessionpostback',False, None],
	['LFLIVEEXTRACT','completedsurveyresponse',False, None],
	['LFLIVEEXTRACT','session',True, "lfliveextract_session"],
	['LFLIVEEXTRACT','completedsurvey',True, "lfliveextract_nps"],
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACT", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)

