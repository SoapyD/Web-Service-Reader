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



#start_date = datetime.datetime(2019, 11, 18)
#end_date = datetime.datetime(2019, 11, 27)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=4.0)
end_date = now.replace(microsecond=0) + datetime.timedelta(hours=1.0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################TELEPHONY
##############################################################################################################################################################
##############################################################################################################################################################

process_list = [
	ws_process_class('TELEPHONYEXTRACT','call', True,'MYCALLS_telephony'),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("TELEPHONYEXTRACT", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

