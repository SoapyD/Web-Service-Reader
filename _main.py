
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file


exec(open("run_process_stack.py").read())
exec(open("ready_process.py").read())
exec(open("query_source_data.py").read())
exec(open("update_reporting_tables.py").read())
#exec(open("web_service_reader.py").read()) #this has now been moved to the functions folder
#exec(open("sql_database_reader.py").read())
#exec(open("return_field_list.py").read())

global error_count

u_print('########################################')
u_print('RUNNING SERVICE EXTRACT PROCESS')
u_print('########################################')

start_time = datetime.datetime.now() #need for process time u_printing


db = 1
database = ''

###############################################################################MAIN PROCESS

staging_tablename='stg_web_service'
delete_staging=True

now = d.now()
start_date = now - datetime.timedelta(hours=2.0)
end_date = now
#start_date = datetime.datetime(2019, 6, 1)
#end_date = datetime.datetime(2019, 6, 8)

time_type = 'days'
time_unit = 28

run_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)


############################################################################### PROCESS

staging_tablename = staging_tablename + '_TEST'
delete_staging=False

#start_date = datetime.datetime(2017, 1, 1)
#end_date = datetime.datetime(2019, 6, 6)
start_date = datetime.datetime(2019, 5, 1)
end_date = datetime.datetime(2019, 6, 7)

time_type = 'weeks'
time_unit = 4

#run_test_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
#run_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HEAT
##############################################################################################################################################################
##############################################################################################################################################################

###HEAT
#update_tables('HEAT', 'incident', None, None, start_date, end_date, end_database)
#update_tables('HEAT', 'organizationalunit', None, None, start_date, end_date, end_database)
#update_tables('HEAT', 'change', None, None, start_date, end_date, end_database)
#update_tables('HEAT', 'problem', None, None, start_date, end_date, end_database)
#update_tables('HEAT', 'task', None, None, start_date, end_date, end_database)
#update_tables('HEAT', 'servicerequest', None, None, start_date, end_date, end_database)

###LFLIVEEXTRACT
#update_tables('LFLIVEEXTRACT', 'session', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'completedsurvey', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'completedsurveyresponse', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'sessionincident', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'fsa_sessionincident', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'he_sessionincident', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'mhclg_sessionincident', None, None, start_date, end_date, end_database)
#update_tables('LFLIVEEXTRACT', 'sessionpostback', None, None, start_date, end_date, end_database)


###TELEPHONYEXTRACT
#NEED A DELETION METHOD HERE TOO
#update_tables('TELEPHONYEXTRACT', 'telephony', None, None, start_date, end_date, end_database)



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