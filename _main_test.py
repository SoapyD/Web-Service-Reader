
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file


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


end_database = 1
now = d.now()
start_date = now - datetime.timedelta(hours=4000.0)
end_date = now

#start_date = datetime.datetime(2019, 3, 16, 12, 00, 00)
#end_date = datetime.datetime(2019, 3, 16, 14, 00, 00)
#start_date = datetime.datetime(2018, 1, 1)
#end_date = datetime.datetime(2019, 4, 1)

u_print(str(start_date) + " to " + str(end_date))
#errors = 0



##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################SERVICE NOW
##############################################################################################################################################################
##############################################################################################################################################################



###############################################################################KNOWLEDGE
#GET THE TABLE FIELDS
tablename = 'kb_knowledge'
return_info = return_servicenow_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
update_tables('HE_TEST', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('DEV', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################INCIDENT
#GET THE TABLE FIELDS
tablename = 'incident'
return_info = return_servicenow_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('HE_TEST', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('DEV', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################CHANGE REQUEST
#GET THE TABLE FIELDS
tablename = 'change_request'
return_info = return_servicenow_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)


#update_tables('HEAT', 'incident', None, None, start_date, end_date, end_database)


finish_time = datetime.datetime.now()
u_print('')
u_print('########################################')
u_print('PROCESS COMPLETE')

u_print('Number of Errors: '+str(error_count))
u_print('Start: '+str(start_time))
u_print('End: '+str(finish_time))
u_print('Time Taken: '+str(finish_time - start_time))
u_print('########################################')

#save_process(start_time, finish_time, str(finish_time - start_time), "Web-Service-Reader", 'hourly')