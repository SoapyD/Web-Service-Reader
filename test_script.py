
#####  ######   ##   #####  #   #    #####  #####   ####   ####  ######  ####   ####  
#    # #       #  #  #    #  # #     #    # #    # #    # #    # #      #      #      
#    # #####  #    # #    #   #      #    # #    # #    # #      #####   ####   ####  
#####  #      ###### #    #   #      #####  #####  #    # #      #           #      # 
#   #  #      #    # #    #   #      #      #   #  #    # #    # #      #    # #    # 
#    # ###### #    # #####    #      #      #    #  ####   ####  ######  ####   #### 

def TEST_ready_process(
	source, tablename, start_date, end_date, time_type, time_unit, 
	db, database, staging_tablename, delete_staging, 
	user_picked_fields=None,print_internal=False, print_details=False,
	wh_output_table=''):

	global output_array

	if time_type == 'days':
		time_add = datetime.timedelta(days=time_unit)
	if time_type == 'weeks':
		time_add = datetime.timedelta(weeks=time_unit)


	temp_start = start_date
	temp_end = start_date + time_add

	#if print_internal == True:
	#	u_print('-------------------------------------------')
	
	u_print("UPDATING: "+source+"_"+tablename) #need to at least print whats being updated

	#RUN THE DATE LOOP
	run_loop = True
	total_rows = 0
	total_time = datetime.datetime.now() - datetime.datetime.now()

	while run_loop == True:
		start_time = datetime.datetime.now() #need for process time u_printing

		#STOP THE LOOP IF THE TEMP END IF OVER THE ACTUAL END DATE
		if temp_end >= end_date:
			run_loop = False

		if temp_end > end_date:
			temp_end = end_date

		if print_internal == True:
			u_print("QUERYING BETWEEN: "+str(temp_start)+" AND "+str(temp_end))
		
		query_rows = TEST_update_tables(source, tablename, temp_start, temp_end, 
			db, database, staging_tablename, delete_staging, 
			user_picked_fields=user_picked_fields, print_details=print_details,
			wh_output_table=wh_output_table)

		total_rows += query_rows

		if print_internal == True:
			finish_time = datetime.datetime.now()
			time_taken = finish_time - start_time
			total_time += time_taken
			u_print(str(query_rows) + ' Rows | Time Taken: '+str(time_taken))	

		#ITTERATE THE DATE RANGE
		temp_start = temp_end
		temp_end = temp_end + time_add

	if print_internal == True:
		u_print('')



	output_array += source + "_" + tablename + ": " +str(total_rows) + " rows | total time: " + str(total_time) + "\n"


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

#    # #####  #####    ##   ##### ######    #####   ##   #####  #      ######  ####  
#    # #    # #    #  #  #    #   #           #    #  #  #    # #      #      #      
#    # #    # #    # #    #   #   #####       #   #    # #####  #      #####   ####  
#    # #####  #    # ######   #   #           #   ###### #    # #      #           # 
#    # #      #    # #    #   #   #           #   #    # #    # #      #      #    # 
 ####  #      #####  #    #   #   ######      #   #    # #####  ###### ######  ####  

def TEST_update_tables(source, tablename, start_date, end_date, 
	db, database, staging_tablename, delete_staging, 
	user_picked_fields=None, print_details=False,
	wh_output_table=''):

    global error_count
    errors = error_count

    return_info = TEST_query_source_data(source, tablename, start_date, end_date, 
    	user_picked_fields=user_picked_fields, print_details=print_details)

    source_type = return_info[0]
    output_df = return_info[1]
    fields = return_info[2]
    filter_fields = return_info[3]
    filter_mirror_fields = return_info[4]
    merge_sql = return_info[5]
    output_table_override = return_info[6]

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:
        TEST_merge_data(output_df, source_type, source, tablename, fields, 
        	db, database, staging_tablename, delete_staging, merge_sql, output_table_override,
        	print_details=print_details, wh_output_table=wh_output_table)

    #u_print('') #ADD GAP TO PROCESS

    row_count = output_df.shape[0]

    return row_count

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

#    # ###### #####   ####  ######    #####    ##   #####   ##   
##  ## #      #    # #    # #         #    #  #  #    #    #  #  
# ## # #####  #    # #      #####     #    # #    #   #   #    # 
#    # #      #####  #  ### #         #    # ######   #   ###### 
#    # #      #   #  #    # #         #    # #    #   #   #    # 
#    # ###### #    #  ####  ######    #####  #    #   #   #    # 

def TEST_merge_data(output_df, source_type, source, tablename, fields, 
	db, database, staging_tablename, delete_staging, merge_sql, output_table_override,
	print_details=False, wh_output_table=''):

	sql_filepath = this_dir+'\\sql\\'+source_type+'_import\\'
	sql_filename = source + '_' + tablename

	merge_with_database(output_df, sql_filepath, sql_filename, tablename, fields, db, database, 
	staging_tablename, delete_staging, print_details=print_details, merge_sql=merge_sql) #in FUNCTIONS_sql

	if wh_output_table != '':

		original_table = source + "_" + tablename
		if output_table_override != '':
			original_table = output_table_override
		merge_sql = merge_sql.replace(original_table, wh_output_table)
		#print(merge_sql)

		merge_with_database(output_df, sql_filepath, sql_filename, 
			wh_output_table, fields, db, database, staging_tablename, delete_staging, 
			print_details=print_details, merge_sql=merge_sql) #in FUNCTIONS_sql    	

    #merge_with_db_powershell(output_df, sql_filepath, sql_filename, tablename, fields, db, database, 
    #staging_tablename, delete_staging, print_details=print_details, merge_sql=merge_sql) #in FUNCTIONS_sql

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  

 ####  ###### #####    ###### # ###### #      #####     # #    # ######  ####  
#    # #        #      #      # #      #      #    #    # ##   # #      #    # 
#      #####    #      #####  # #####  #      #    #    # # #  # #####  #    # 
#  ### #        #      #      # #      #      #    #    # #  # # #      #    # 
#    # #        #      #      # #      #      #    #    # #   ## #      #    # 
 ####  ######   #      #      # ###### ###### #####     # #    # #       #### 

def get_field_info(source, table_info, user_picked_fields=None):
	#create an array of fields in the query tag array
	selected_fields = []
	selected_source_fields = []
	selected_mirror_fields = []
	for field in table_info.fields:

		if field.tag == source or field.tag == '': #check to see if the tag is unique to source or global

			check = False

			if user_picked_fields == None: #check to see if there's any user picked fields we're filtering by, if not, pass the check
				check = True
			else:
				if (field.mirror_name in user_picked_fields): #check to see if the mirror field is in the filtered field list
					check = True

			if check == True:
				if field.mirror_name not in selected_mirror_fields: #the check to see its not already been added
					selected_fields.append(field)
					selected_mirror_fields.append(field.mirror_name) #keep record of mirror fields as source names can differ

					if field.source_name != '': #create a list of non-blank source fields, which'll be sent to the web service query
						selected_source_fields.append(field.source_name)


	return_info = {}
	return_info[0] = selected_fields
	return_info[1] = selected_source_fields
	return_info[2] = selected_mirror_fields

	return return_info

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  

 ####  ###### #####     ####   ####  #    # #####   ####  ######    ##### #   # #####  ###### 
#    # #        #      #      #    # #    # #    # #    # #           #    # #  #    # #      
#      #####    #       ####  #    # #    # #    # #      #####       #     #   #    # #####  
#  ### #        #           # #    # #    # #####  #      #           #     #   #####  #      
#    # #        #      #    # #    # #    # #   #  #    # #           #     #   #      #      
 ####  ######   #       ####   ####   ####  #    #  ####  ######      #     #   #      ###### 

def get_source_type(source):

	source_type = ''
	instancename = ''
	username = ''
	password = ''

	#GET DATA FROM SOURCE
	if source == 'HE':
		source_type = 'service_now'
		instancename = he_instancename
		username = he_username
		password = he_password
	if source == 'HE_KB':
		source = "HE" #reset source
		source_type = 'service_now'
		instancename = he_instancename
		username = he_kb_username
		password = he_kb_password
	#if source == 'HE_TEST':
	#	source_type = 'service_now'
	#	instancename = he_test_instancename
	#	username = he_test_username
	#	password = he_test_password
	if source == 'FSA':    
		source_type = 'service_now'
		instancename = fsa_instancename
		username = fsa_username
		password = fsa_password
	if source == 'MHCLG':    
		source_type = 'service_now'
		instancename = mhclg_instancename
		username = mhclg_username
		password = mhclg_password
	if source == 'CROYDON':    
		source_type = 'service_now'
		instancename = croydon_instancename
		username = croydon_username
		password = croydon_password

	#HEAT MSSQL CONNECTION
	if source == 'HEATSM':
		source_type = 'live'

	if source == 'LFLIVEEXTRACT':
		source_type = 'live'

	if source == 'LFLIVEEXTRACTNEW':
		source_type = 'live'

	if source == 'TELEPHONYEXTRACT':
		source_type = 'live'

	#IVANTI API
	if source == 'ENWL':
		source_type = 'ivanti_api'

	#RINGCENTRAL API
	if source == 'RINGCENTRAL':
		source_type = 'ringcentral_api'	

	return_info = {}
	return_info[0] = source_type
	return_info[1] = instancename
	return_info[2] = username
	return_info[3] = password
	return_info[4] = source #needed when source updates sometimes

	return return_info


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  

 ####  #    # ###### #####  #   #     ####   ####  #    # #####   ####  ######    #####    ##   #####   ##   
#    # #    # #      #    #  # #     #      #    # #    # #    # #    # #         #    #  #  #    #    #  #  
#    # #    # #####  #    #   #       ####  #    # #    # #    # #      #####     #    # #    #   #   #    # 
#  # # #    # #      #####    #           # #    # #    # #####  #      #         #    # ######   #   ###### 
#   #  #    # #      #   #    #      #    # #    # #    # #   #  #    # #         #    # #    #   #   #    # 
 ### #  ####  ###### #    #   #       ####   ####   ####  #    #  ####  ######    #####  #    #   #   #    # 

def TEST_query_source_data(source, tablename, start_date, end_date, user_picked_fields=None, run_extract=True, reference_list=None, print_details=False):

	import numpy as np
	global error_count

	return_info = {}
	fields = []
	filter_fields = []
	filter_mirror_fields = []
	merge_sql = ''

	output_df = pd.DataFrame()

	source_type = ''

    #try:
	run = True
	if run == True:
		instancename = ''
		username = ''
		password = ''
		source_type = ''

		return_info = get_source_type(source)
		source_type = return_info[0] 
		instancename = return_info[1]
		username = return_info[2]
		password = return_info[3]
		source = return_info[4] #needed when source is updated, multi accounts need this

		if source_type == 'service_now':

			table_info = return_servicenow_field_list_2(tablename)

			return_info = get_field_info(source, table_info, user_picked_fields)
			selected_fields = return_info[0]
			selected_source_fields = return_info[1]
			selected_mirror_fields = return_info[2]

			fields = selected_source_fields
			filter_fields = table_info.filter_fields
			filter_mirror_fields = table_info.filter_mirror_fields


			output_df = get_servicenow_webservice_data(source, instancename, username, password, tablename, fields, filter_fields, start_date, end_date, 
				run_extract=run_extract, reference_list=reference_list, print_details=print_details)

			#output_df.to_csv('exports\\TEST.csv')

			#create a temp lists we'll be running
			merge_sql = generate_merge_sql(source, tablename, table_info, selected_fields)


		if source_type == 'live':

			table_info = return_heat_field_list_2(tablename)

			return_info = get_field_info(source, table_info, user_picked_fields)
			selected_fields = return_info[0]
			selected_source_fields = return_info[1]
			selected_mirror_fields = return_info[2]

			fields = selected_source_fields
			filter_fields = table_info.filter_fields
			filter_mirror_fields = table_info.filter_mirror_fields         

			merge_sql = generate_merge_sql(source, tablename, table_info, selected_fields)

			#output_df = sql_query_parser_3(source, tablename, table_info, fields, filter_fields, start_date, end_date, 
			#    run_extract=run_extract, print_details=print_details)      

			output_df = sql_query_parser_3_PYTHON(source, tablename, table_info, fields, filter_fields, start_date, end_date, 
			    run_extract=run_extract, print_details=print_details)     


		if source_type == 'ivanti_api':

			table_info = return_ivanti_api_field_list_2(tablename, start_date, end_date)

			connection_info = ivanti_connection_class(
				url = enwl_url,
				username = enwl_username,
				password = enwl_password,
				role = enwl_role,
				tenant_id = enwl_tenant_id
				)
			fields = table_info.fields
			table = tablename
			filters = table_info.filter_fields

			output_df = query_ivanti_2(connection_info, fields, table, filters)	

			#NEED TO GENERATE A MERGE SCRIPT
			merge_sql = generate_merge_sql(source, tablename, table_info, fields)


		if source_type == 'ringcentral_api':

			table_info = return_ringcentral_api_field_list(tablename)
			return_info = get_field_info(source, table_info, user_picked_fields)
			selected_fields = return_info[0]
			
			field_string = ''
			for field in selected_fields:
				if field_string != '':
					field_string += ", "
				field_string += field.source_name 

			fields = table_info.fields
			table = tablename

			#print(fields)
			output_df = query_ringcentral(table, field_string, start_date, end_date, print_details)

			#NEED TO GENERATE A MERGE SCRIPT
			merge_sql = generate_merge_sql(source, table, table_info, fields)
			#print(merge_sql)
	

	    #IF A ENTIRE DATASETS FIELDS ARE BLANK FOR A CERTAIN FIELD, THAT FIELD ISN'T RETURNED.
	    #IF THIS IS THE CASE THEN THAT FIELD AND IT'S RELATED VALUE FIELD NEED ADDING TO THE DATASET


		if source_type == 'service_now' and run_extract == True:
			for field in fields:
				if field not in output_df.columns:
					output_df[field] = np.nan #add the missing field
					output_df[field+'_value'] = np.nan #add a corresponding value field  

		return_info[0] = source_type
		return_info[1] = output_df
		return_info[2] = fields
		return_info[3] = filter_fields
		return_info[4] = filter_mirror_fields
		return_info[5] = merge_sql
		return_info[6] = table_info.output_table_override
    #except:
    #	print("ERROR: Data Query Didn't Work")
    #	error_count += 1

	return return_info