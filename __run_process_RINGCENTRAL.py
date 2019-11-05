import os

exec(open("_main2.py").read())

db = 1 #REPORTING DATABASE
database = ''


delete_staging = True
print_internal = True
print_details = False
run_warehousing = False
time_type = 'days'
time_unit = 7



#start_date = datetime.datetime(2019, 11, 5)
#end_date = datetime.datetime(2019, 11, 6)

#CAN ONLY SEND DATES TO RINGCENTRAL, SO THE TIME COMPONENT NEEDS STRIPPING OUT
start_date = now.replace(hour=0)
start_date = start_date.replace(minute=0)
start_date = start_date.replace(second=0)
start_date = start_date.replace(microsecond=0)

end_date = start_date + datetime.timedelta(days=1.0)



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