
def TEST_ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields=None,
	print_internal=False, print_details=False):

	if time_type == 'days':
		time_add = datetime.timedelta(days=time_unit)
	if time_type == 'weeks':
		time_add = datetime.timedelta(weeks=time_unit)


	temp_start = start_date
	temp_end = start_date + time_add

	if print_internal == True:
		u_print('-------------------------------------------')
	
	u_print("UPDATING: "+source+"_"+tablename) #need to at least print whats being updated
	
	if print_internal == True:
		u_print('')

	run_loop = True

	while run_loop == True:


		#STOP THE LOOP IF THE TEMP END IF OVER THE ACTUAL END DATE
		if temp_end >= end_date:
			run_loop = False

		if temp_end > end_date:
			temp_end = end_date

		if print_internal == True:
			u_print("QUERYING BETWEEN: "+str(temp_start)+" AND "+str(temp_end))
		
		TEST_update_tables(source, tablename, temp_start, temp_end, db, database, staging_tablename, delete_staging, user_picked_fields=user_picked_fields, print_details=print_details)

		#ITTERATE THE DATE RANGE
		temp_start = temp_end
		temp_end = temp_end + time_add

	if print_internal == True:
		u_print('-------------------------------------------')

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

def TEST_update_tables(source, tablename, start_date, end_date, db, database, staging_tablename, delete_staging, user_picked_fields=None, print_details=False):

    global error_count
    errors = error_count

    return_info = TEST_query_source_data(source, tablename, start_date, end_date, user_picked_fields=user_picked_fields, print_details=print_details)

    source_type = return_info[0]
    output_df = return_info[1]
    fields = return_info[2]
    filter_fields = return_info[3]
    filter_mirror_fields = return_info[4]
    merge_sql = return_info[5]

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:
        TEST_merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging, merge_sql, print_details=print_details)

    #u_print('') #ADD GAP TO PROCESS

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

def TEST_merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging, merge_sql, print_details=False):

    sql_filepath = this_dir+'\\sql\\'+source_type+'_import\\'
    sql_filename = source + '_' + tablename

    #print(merge_sql)
    merge_with_database(output_df, sql_filepath, sql_filename, tablename, fields, db, database, 
    staging_tablename, delete_staging, print_details=print_details, merge_sql=merge_sql) #in FUNCTIONS_sql


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  

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

	if source == 'HEATSM':
		source_type = 'live'

	if source == 'LFLIVEEXTRACT':
		source_type = 'live'

	if source == 'LFLIVEEXTRACTNEW':
		source_type = 'live'

	if source == 'TELEPHONYEXTRACT':
		source_type = 'live'

	return_info = {}
	return_info[0] = source_type
	return_info[1] = instancename
	return_info[2] = username
	return_info[3] = password

	return return_info


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  

def TEST_query_source_data(source, tablename, start_date, end_date, user_picked_fields=None, run_extract=True, reference_list=None, print_details=False):

	import numpy as np
	global error_count

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

			output_df = sql_query_parser_3(source, tablename, table_info, fields, filter_fields, start_date, end_date, 
			    run_extract=run_extract, print_details=print_details)      


    #IF A ENTIRE DATASETS FIELDS ARE BLANK FOR A CERTAIN FIELD, THAT FIELD ISN'T RETURNED.
    #IF THIS IS THE CASE THEN THAT FIELD AND IT'S RELATED VALUE FIELD NEED ADDING TO THE DATASET
	if source_type == 'service_now' and run_extract == True:
		for field in fields:
			if field not in output_df.columns:
				output_df[field] = np.nan #add the missing field
				output_df[field+'_value'] = np.nan #add a corresponding value field  

	return_info = {}
	return_info[0] = source_type
	return_info[1] = output_df
	return_info[2] = fields
	return_info[3] = filter_fields
	return_info[4] = filter_mirror_fields
	return_info[5] = merge_sql

	return return_info
    #except:
   # 	print("didn't work")