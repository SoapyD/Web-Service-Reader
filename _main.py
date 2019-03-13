
exec(open("web_service_reader.py").read())

print('########################################')
print('RUNNING SERVICE EXTRACT PROCESS')
print('########################################')

start_time = datetime.datetime.now() #need for process time printing

now = d.now()
start_date = now - datetime.timedelta(hours=2)
end_date = now

#start_date = datetime.datetime(2019, 1, 1, 00, 00, 00)
#end_date = datetime.datetime(2019, 2, 1, 23, 59, 59)
#start_date = datetime.datetime(2018, 11, 1)
#end_date = datetime.datetime(2019, 1, 1)
print(str(start_date) + " to " + str(end_date))
###############################################################################INCIDENTS
#GET THE TABLE FIELDS
tablename = 'incident'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
update_webservice_tables('HE', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)

###############################################################################SC_REQ_ITEMS
#GET THE TABLE FIELDS
tablename = 'sc_req_item'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_webservice_tables('HE', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)

###############################################################################TASK_SLAS
#GET THE TABLE FIELDS
tablename = 'task_sla'
return_info = return_field_list(tablename)
fields = return_info[0]
filter_fields = return_info[1]
#QUERY THE TABLE DATA
#update_webservice_tables('HE', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('FSA', tablename, fields, filter_fields, start_date, end_date)
#update_webservice_tables('MHCLG', tablename, fields, filter_fields, start_date, end_date)

#NEED TO ADD A MEANS OF SENDING AN ARRAY OF DATE FIELD NAMES THAT'LL GET TURNED INTO THE DATE FILTER STRING


finish_time = datetime.datetime.now()
print('')
print('########################################')
print('PROCESS COMPLETE')
print('Start: '+str(start_time))
print('End: '+str(finish_time))
print('Time Taken: '+str(finish_time - start_time))
print('########################################')