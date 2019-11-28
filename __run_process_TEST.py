import os

exec(open("_main2.py").read())

#db = 3 #REPORTING DATABASE
#database = 'LF-SQL-DEV'
db = 1 #REPORTING DATABASE
database = ''

delete_staging = False
print_internal = True
print_details = True
run_warehousing = True
time_type = 'days'
time_unit = 7


start_date = datetime.datetime(2019, 11, 25)
end_date = datetime.datetime(2019, 11, 28)
#start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
#end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



process_list = [
	ws_process_class('HE','sys_user'),
	ws_process_class('HE','problem', True, "he_problem"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)
