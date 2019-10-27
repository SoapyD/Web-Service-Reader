
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file

warehousing_path = base_path+"Warehousing-Process"
exec(open(warehousing_path+"\\_main.py").read()) #load the warehousing process


exec(open("run_process_stack_2.py").read())
exec(open("test_script.py").read())
exec(open("generate_queries.py").read())


global output_array
output_array = ''
global error_count
now = d.now()


def run_main(process_group, process_list, start_date, end_date, 
	time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details):


	start_time = datetime.datetime.now() #need for process time u_printing


	#db = 1 #REPORTING DATABASE
	#db = 2 #DEV DATABASE
	#database = ''

	###############################################################################MAIN PROCESS

	#time_type = 'days'
	#time_unit = 1
	global output_array
	output_array = ''
	global error_count
	error_count = 0
	global output_string
	output_string = ''


	u_print('########################################')
	u_print('RUNNING SERVICE EXTRACT PROCESS')
	u_print('########################################')


	#POPULATE THE BASE AND TEMP WAREHOUSE TABLES
	run_process_stack_2(
		start_date, end_date, time_type, time_unit, process_list, 
		db, database, delete_staging, run_warehousing, print_internal, print_details)

	u_print(output_array)


	finish_time = datetime.datetime.now()
	u_print('')
	u_print('########################################')
	u_print('PROCESS COMPLETE')

	u_print('Number of Errors: '+str(error_count))
	u_print('Start: '+str(start_time))
	u_print('End: '+str(finish_time))
	u_print('Time Taken: '+str(finish_time - start_time))
	u_print('########################################')

	if delete_staging == True:
		save_process(start_time, finish_time, str(finish_time - start_time), "Web-Service-Reader "+process_group, 'hourly')