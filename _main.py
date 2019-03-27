
exec(open("update_reporting_tables.py").read())
exec(open("web_service_reader.py").read())
exec(open("sql_database_reader.py").read())
exec(open("return_field_list.py").read())

global error_count

u_print('########################################')
u_print('RUNNING SERVICE EXTRACT PROCESS')
u_print('########################################')

start_time = datetime.datetime.now() #need for process time u_printing


end_database = 1
now = d.now()
start_date = now - datetime.timedelta(hours=2.0)
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



###############################################################################SYS USER
#GET THE TABLE FIELDS
tablename = 'sys_user'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################INCIDENTS
#GET THE TABLE FIELDS
tablename = 'incident'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################INCIDENTS TASK
#GET THE TABLE FIELDS
tablename = 'incident_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################INCIDENTS ALERT
#GET THE TABLE FIELDS
tablename = 'incident_alert'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################SC_TASK
#GET THE TABLE FIELDS
tablename = 'sc_request'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################SC_REQ_ITEMS
#GET THE TABLE FIELDS
tablename = 'sc_req_item'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################SC_TASK
#GET THE TABLE FIELDS
tablename = 'sc_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################CHANGE REQUEST
#GET THE TABLE FIELDS
tablename = 'change_request'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################CHANGE TASK
#GET THE TABLE FIELDS
tablename = 'change_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################PROBLEM
#GET THE TABLE FIELDS
tablename = 'problem'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################PROBLEM
#GET THE TABLE FIELDS
tablename = 'problem_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)

###############################################################################TASK_SLAS
#GET THE TABLE FIELDS
tablename = 'task_sla'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_tables('HE', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('FSA', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date, end_database)
#update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date, end_database)


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

save_process(start_time, finish_time, str(finish_time - start_time), "Web-Service-Reader")