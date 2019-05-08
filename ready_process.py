
def ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging):

	return_info = return_servicenow_field_list(tablename)
	fields = return_info[0]
	filter_fields = return_info[1]
	filter_mirror_fields = return_info[2]

	if time_type == 'days':
		time_add = datetime.timedelta(days=time_unit)
	if time_type == 'weeks':
		time_add = datetime.timedelta(weeks=time_unit)


	temp_start = start_date
	temp_end = start_date + time_add


	run_loop = True

	while run_loop == True:


		#STOP THE LOOP IF THE TEMP END IF OVER THE ACTUAL END DATE
		if temp_end >= end_date:
			run_loop = False

		if temp_end > end_date:
			temp_end = end_date

		u_print('-------------------------------------------')
		u_print("QUERYING BETWEEN: "+str(temp_start)+" AND "+str(temp_end))
		
		update_tables(source, tablename, fields, filter_fields, temp_start, temp_end, db, database, staging_tablename, delete_staging)

		#ITTERATE THE DATE RANGE
		temp_start = temp_end
		temp_end = temp_end + time_add


		u_print('-------------------------------------------')
