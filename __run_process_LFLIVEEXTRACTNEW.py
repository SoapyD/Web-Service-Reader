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
	['LFLIVEEXTRACTNEW','sessionincident',False, None],
	['LFLIVEEXTRACTNEW','he_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','fsa_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','mhclg_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','croydon_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','sessionpostback',False, None],
	['LFLIVEEXTRACTNEW','completedsurveyresponse',False, None],
	['LFLIVEEXTRACTNEW','session',True, "lfliveextract_session"],
	['LFLIVEEXTRACTNEW','completedsurvey',True, "lfliveextract_nps"],
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACTNEW", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)
