import os

exec(open("_main2.py").read())

#db = 3 #REPORTING DATABASE
#database = 'LF-SQL-DEV'
db = 1 #REPORTING DATABASE
database = ''

delete_staging = False
print_internal = True
print_details = False
run_warehousing = False
time_type = 'days'
time_unit = 7


start_date = datetime.datetime(2019, 9, 1)
end_date = datetime.datetime(2019, 12, 4)
#start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
#end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS




process_list = [
	ws_process_class('RINGCENTRAL','agents'),
	ws_process_class('RINGCENTRAL','completedcontacts', True,'RINGCENTRAL_telephony'),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

