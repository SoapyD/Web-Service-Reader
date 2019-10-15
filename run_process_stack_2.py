

def process_stack_subprocess(source, tablename, start_date, end_date, 
	time_type, time_unit, 
	db, database, staging_tablename, delete_staging, user_picked_fields, 
	print_internal, print_details, run_creation=True, wh_output_table=''):

	#THIS HAS BEEN REMOVED AS IT'S NOT NEEDED HERE AT ALL
	#if run_creation == True:
	#	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	global error_count
	
	
	total_rows = 0
	#run = True
	#if run == True:
	try:
		TEST_ready_process(source, tablename, start_date, end_date, 
			time_type, time_unit, db, database, staging_tablename, delete_staging, 
			user_picked_fields, print_internal=print_internal, print_details=print_details,
			wh_output_table=wh_output_table)
	except Exception as e:
		try:
			u_print(e) #seems to error out completely sometimes
			u_print('')	
		except:
			u_print("Couldn't print error reason")
			u_print('')	
		error_count += 1


####### #######  #####  #######             ######  ######  #######  #####  #######  #####   #####  
   #    #       #     #    #                #     # #     # #     # #     # #       #     # #     # 
   #    #       #          #                #     # #     # #     # #       #       #       #       
   #    #####    #####     #       #####    ######  ######  #     # #       #####    #####   #####  
   #    #             #    #                #       #   #   #     # #       #             #       # 
   #    #       #     #    #                #       #    #  #     # #     # #       #     # #     # 
   #    #######  #####     #                #       #     # #######  #####  #######  #####   #####  


def run_test_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=False, print_details=False):

	if print_internal == True:
		u_print(str(start_date) + " to " + str(end_date))

	#generate_merge_query(source, tablename, user_picked_fields)	DO NOT DELETE!!!!!!!!
	#generate_creation_query(source, tablename)


	user_picked_fields = None

	source = 'HE'
	#tablename = 'sys_user'
	#process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



#     # ####### #     #             ######  ######  #######  #####  #######  #####   #####  
##    # #       #  #  #             #     # #     # #     # #     # #       #     # #     # 
# #   # #       #  #  #             #     # #     # #     # #       #       #       #       
#  #  # #####   #  #  #    #####    ######  ######  #     # #       #####    #####   #####  
#   # # #       #  #  #             #       #   #   #     # #       #             #       # 
#    ## #       #  #  #             #       #    #  #     # #     # #       #     # #     # 
#     # #######  ## ##              #       #     # #######  #####  #######  #####   #####  

def run_process_stack_2(
	start_date, end_date, time_type, time_unit, process_list, 
	db, database, delete_staging, print_internal=False, print_details=False):


	global error_count

	if print_internal == True:
		u_print(str(start_date) + " to " + str(end_date))

	#generate_merge_query(source, tablename, user_picked_fields)	#DO NOT DELETE!!!!!!!!
	user_picked_fields = None
	#user_picked_fields = ['sys_id','number','causecode','lastmoddatetime']

	wh_table_prefix = 'WH_'+now.strftime('%d_%m_%Y_%H_%M_%S') + '_'


	for process in process_list:

		source = process[0]
		tablename = process[1]
		make_wh_table = process[2]
		wh_query = process[3]

		#WE WANT A UNIQUE INSTANCE OF A STAGING TABLE TO AVOID MULTIPLE INSTANCE PROBLEMS
		staging_tablename='stg_web_service_'+now.strftime('%d_%m_%Y_%H_%M_%S')+"_"+source+"_"+tablename

		##############################################################################################################################################################
		##############################################################################################################################################################
		#######################################ADDITIONAL FUNCTIONS
		##############################################################################################################################################################
		##############################################################################################################################################################

		#CHECK TO SEE IF THE PROCESS IS TELEPHONY, IF SO, DELETE THE OLD CALL DATA
		if source == 'TELEPHONYEXTRACT' and tablename == 'call':
			#DELETE ANY TELEPHONY DATA FOR THE GIVEN RANGE,
			#THIS NEEDS TO BE DONE AS CALLS ARE PASSED BETWEEN AGENTS IN ODD WAYS WHICH GET REFLECTED IN THE TELEPHONY DATA
			#THIS WOULD PRODUCE DUPLICATES IN MY DATA WHICH ARE EFFECTIVELY REMOVED FROM THE MAIN DATASOURCE
			sqlfile = "DELETE FROM TELEPHONYEXTRACT_call WHERE datetime BETWEEN CONVERT(DATETIME,'@start_date') AND CONVERT(DATETIME,'@end_date')"
			sqlfile = sqlfile.replace("@start_date", str(start_date.replace(microsecond=0)))
			sqlfile = sqlfile.replace("@end_date", str(end_date.replace(microsecond=0)))		

			#query_db_powershell(sqlfile, db, database)
			query_database2('deleting telephony data',sqlfile, db, database)

			#DELETE THE DATA FROM THE WAREHOUSE AND AGGREGATION TABLES

		##############################################################################################################################################################
		##############################################################################################################################################################
		#######################################OUTPUT TABLE CREATION
		##############################################################################################################################################################
		##############################################################################################################################################################

		#CREATE THE OUTPUT TABLE WE'LL LOAD THE DATA INTO, ONLY NEED TO DO THIS WITH SOME TABLES
		wh_output_table = ''
		wh_combined_table = ''
		table_name = source + "_" + tablename
		if make_wh_table == True:
			wh_output_table = wh_table_prefix
			wh_output_table += table_name
			wh_combined_table = "COMBINED_" + wh_output_table

			#CREATE THE DATA WAREHOUSE TABLE
			if wh_output_table != '' and wh_output_table != table_name:
				generate_creation_query(
					source, tablename, 
					db=db, database=database, output_table=wh_output_table)

		##############################################################################################################################################################
		##############################################################################################################################################################
		#######################################RUN EXTRACT AND MERGE
		##############################################################################################################################################################
		##############################################################################################################################################################

		#THIS PROCESS UPDATES THE BASE TABLES
		#IT ALSO POPULATES THE TEMPORARY WAREHOUSE TABLES
		process_stack_subprocess(source, tablename, start_date, end_date, 
			time_type, time_unit, db, database, 
			staging_tablename, delete_staging, 
			user_picked_fields=user_picked_fields, 
			print_internal=print_internal, print_details=print_details,
			wh_output_table=wh_output_table)


		##############################################################################################################################################################
		##############################################################################################################################################################
		#######################################WAREHOUSING SCRIPT
		##############################################################################################################################################################
		##############################################################################################################################################################

		if make_wh_table == True:
			try:
				update_warehouse(table_name, wh_output_table, wh_query, wh_combined_table,
					print_internal=print_internal, print_details=print_details)
			except:
				error_count += 1
				u_print("There was an error updating the warehouse")

			drop_sql = "DROP TABLE "+wh_output_table
			if wh_output_table != table_name: #DON'T DROP THE TABLE IF IT'S JUST THE NORMAL TABLE
				query_database2('Drop Temp WH Table',drop_sql, db, database, print_details=print_details)
			
			if print_internal == True:
				u_print("Warehousing Complete")
				u_print("")