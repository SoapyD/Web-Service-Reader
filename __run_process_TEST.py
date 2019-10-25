import os

exec(open("_main2.py").read())

"""
delete_staging = True
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 7
"""
delete_staging = False
print_internal = True
print_details = False
run_warehousing = False
time_type = 'days'
time_unit = 7


start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2019, 10, 26)
#start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
#end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



##############################################################################################################################
##############################################################################################################################
########################################################TEST DATA
##############################################################################################################################
##############################################################################################################################

"""
generate_creation_query('RINGCENTRAL', 'agents')


process_list = [
	ws_process_class('RINGCENTRAL','completedcontacts'),
	ws_process_class('RINGCENTRAL','agents'),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)
"""

process_list = [
	ws_process_class('LFLIVEEXTRACT','sessionpostback',user_picked_fields=['recid','techname']),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACT", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)
