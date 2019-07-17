
def ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging,
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
		
		update_tables(source, tablename, temp_start, temp_end, db, database, staging_tablename, delete_staging, print_details=print_details)

		#ITTERATE THE DATE RANGE
		temp_start = temp_end
		temp_end = temp_end + time_add

	if print_internal == True:
		u_print('-------------------------------------------')
