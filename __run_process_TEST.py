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


start_date = datetime.datetime(2019, 12, 1)
end_date = datetime.datetime(2019, 12, 21)
#start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
#end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS




##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################RINGCENTRAL
##############################################################################################################################################################
##############################################################################################################################################################


start_date = now.replace(hour=0)
start_date = start_date.replace(minute=0)
start_date = start_date.replace(second=0)
start_date = start_date.replace(microsecond=0)

end_date = start_date + datetime.timedelta(days=1.0)
""""""

#start_date = datetime.datetime(2019, 1, 1)
#end_date = datetime.datetime(2019, 12, 3)


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################RINGCENTRAL
##############################################################################################################################################################
##############################################################################################################################################################


process_list = [
	#ws_process_class('RINGCENTRAL','agents'),
	#ws_process_class('RINGCENTRAL','completedcontacts', True,'RINGCENTRAL_telephony'),
	ws_process_class('RINGCENTRAL','skills'),
	ws_process_class('RINGCENTRAL','campaigns'),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)



#generate_creation_query('RINGCENTRAL', 'campaigns')
#generate_creation_query('RINGCENTRAL', 'skills')
