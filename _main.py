
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file


exec(open("run_process_stack.py").read())
exec(open("ready_process.py").read())
exec(open("query_source_data.py").read())
exec(open("update_reporting_tables.py").read())
exec(open("test_script.py").read())
exec(open("generate_queries.py").read())
#exec(open("web_service_reader.py").read()) #this has now been moved to the functions folder
#exec(open("sql_database_reader.py").read())
#exec(open("return_field_list.py").read())

global output_array
output_array = ''

global error_count

u_print('########################################')
u_print('RUNNING SERVICE EXTRACT PROCESS')
u_print('########################################')

start_time = datetime.datetime.now() #need for process time u_printing


db = 1
database = ''

###############################################################################MAIN PROCESS

staging_tablename='stg_web_service'
delete_staging = True
print_internal = True
print_details = False

now = d.now()
start_date = now - datetime.timedelta(hours=2.0)
end_date = now

time_type = 'days'
time_unit = 1

run_process_stack_2(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal, print_details)

############################################################################### PROCESS

staging_tablename = staging_tablename + '_TEST'
delete_staging = False
print_internal = True
print_details = False


start_date = datetime.datetime(2019, 7, 29)
end_date = datetime.datetime(2019, 8, 1)
#start_date = datetime.datetime(2018, 12, 12, 8, 50, 0)
#end_date = datetime.datetime(2018, 12, 12, 9, 0, 0)
#now = d.now()
#start_date = now - datetime.timedelta(hours=2.0)
#end_date = now + datetime.timedelta(hours=2.0)


time_type = 'days'
time_unit = 1

#run_test_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal, print_details)
#run_process_stack_2(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal, print_details)

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

save_process(start_time, finish_time, str(finish_time - start_time), "Web-Service-Reader", 'hourly')