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


start_date = datetime.datetime(2020, 1, 22)
end_date = datetime.datetime(2020, 1, 23)
#start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
#end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS




#start_date = datetime.datetime(2019, 10, 22)
#end_date = datetime.datetime(2019, 10, 25)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
end_date = now.replace(microsecond=0) + datetime.timedelta(hours=1.0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



### #     #    #    #     # ####### ###             ### #     #  #####  ####### 
 #  #     #   # #   ##    #    #     #               #  ##    # #     #    #    
 #  #     #  #   #  # #   #    #     #               #  # #   # #          #    
 #  #     # #     # #  #  #    #     #     #####     #  #  #  #  #####     #    
 #   #   #  ####### #   # #    #     #               #  #   # #       #    #    
 #    # #   #     # #    ##    #     #               #  #    ## #     #    #    
###    #    #     # #     #    #    ###             ### #     #  #####     #    

##############################################################################################################################
##############################################################################################################################
########################################################IVANTI INSTANCE
##############################################################################################################################
##############################################################################################################################


"""
process_list = [
	ws_process_class('HEATSM','task', True, "heatsm_problem_task"),
]
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HEATSM", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)



process_list = [
	ws_process_class('MHCLG','problem_task', True, "mhclg_problem_task"),
]
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("MHCLG", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

process_list = [
	ws_process_class('HE','problem_task', True, "he_problem_task"),
]
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	ws_process_class('FSA','problem_task', True, "fsa_problem_task"),
]
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("FSA", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)

process_list = [
	ws_process_class('CROYDON','problem_task', True, "croydon_problem_task"),
]
#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("CROYDON", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)
"""


process_list = [
	ws_process_class('ENWL','Task', True, "enwl_problem_task"),	
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("ENWL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)
