
exec(open("update_reporting_tables.py").read())
exec(open("web_service_reader.py").read())
exec(open("sql_database_reader.py").read())
exec(open("return_field_list.py").read())

print('########################################')
print('RUNNING SERVICE EXTRACT PROCESS')
print('########################################')

start_time = datetime.datetime.now() #need for process time printing

now = d.now()
start_date = now - datetime.timedelta(hours=4.0)
end_date = now

start_date = datetime.datetime(2019, 3, 16, 12, 00, 00)
end_date = datetime.datetime(2019, 3, 16, 14, 00, 00)
#start_date = datetime.datetime(2019, 3, 14)
#end_date = datetime.datetime(2019, 3, 15)

print(str(start_date) + " to " + str(end_date))
errors = 0


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
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################INCIDENTS
#GET THE TABLE FIELDS
tablename = 'incident'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################SC_TASK
#GET THE TABLE FIELDS
tablename = 'sc_request'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################SC_REQ_ITEMS
#GET THE TABLE FIELDS
tablename = 'sc_req_item'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################SC_TASK
#GET THE TABLE FIELDS
tablename = 'sc_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################PROBLEM
#GET THE TABLE FIELDS
tablename = 'problem'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)

###############################################################################PROBLEM
#GET THE TABLE FIELDS
tablename = 'problem_task'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)



###############################################################################TASK_SLAS
#GET THE TABLE FIELDS
tablename = 'task_sla'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
errors += update_tables('HE', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)
errors += update_tables('CROYDON', tablename, fields, filter_fields, start_date, end_date)





############Service Now
#DONE #incident
#DONE #sc_request
#DONE #sc req item
#DONE #task sla
#DONE #sc task
#DONE #problem
#DONE #problem task

#incident task


##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HEAT
##############################################################################################################################################################
##############################################################################################################################################################

###HEAT
errors += update_tables('HEAT', 'incident', None, None, start_date, end_date)
errors += update_tables('HEAT', 'organizationalunit', None, None, start_date, end_date)
errors += update_tables('HEAT', 'change', None, None, start_date, end_date)
errors += update_tables('HEAT', 'problem', None, None, start_date, end_date)
errors += update_tables('HEAT', 'task', None, None, start_date, end_date)
errors += update_tables('HEAT', 'servicerequest', None, None, start_date, end_date)

###LFLIVEEXTRACT
errors += update_tables('LFLIVEEXTRACT', 'session', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'completedsurvey', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'completedsurveyresponse', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'sessionincident', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'fsa_sessionincident', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'he_sessionincident', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'mhclg_sessionincident', None, None, start_date, end_date)
errors += update_tables('LFLIVEEXTRACT', 'sessionpostback', None, None, start_date, end_date)


###TELEPHONYEXTRACT
#errors += update_tables('TELEPHONYEXTRACT', 'telephony', None, None, start_date, end_date)





finish_time = datetime.datetime.now()
print('')
print('########################################')
print('PROCESS COMPLETE')
print('Number of Errors: '+str(errors))
print('Start: '+str(start_time))
print('End: '+str(finish_time))
print('Time Taken: '+str(finish_time - start_time))
print('########################################')