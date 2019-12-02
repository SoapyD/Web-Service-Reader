import os

exec(open("_main2.py").read())

db = 1 #REPORTING DATABASE
database = ''


delete_staging = True
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 30


#CAN ONLY SEND DATES TO RINGCENTRAL, SO THE TIME COMPONENT NEEDS STRIPPING OUT

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
	ws_process_class('RINGCENTRAL','agents'),
	ws_process_class('RINGCENTRAL','completedcontacts', True,'RINGCENTRAL_telephony'),
]

"""
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

"""

"""
generate_creation_query('RINGCENTRAL', 'agents')
"""



##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################NOW ADD TO REPORTING_TEMP TO COVER SOME EXISTING REPORTING
##############################################################################################################################################################
##############################################################################################################################################################


run_warehousing = False

db = 0 #reporting_temp
database = 'reporting_temp'


run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)
