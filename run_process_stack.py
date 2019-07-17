

def run_test_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=False, print_details=False):

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################SERVICE NOW
##############################################################################################################################################################
##############################################################################################################################################################

	if print_internal == True:
		u_print(str(start_date) + " to " + str(end_date))

	
	source = 'HE'
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'change_task'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_req_item'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)	

	source = 'FSA'
	tablename = 'incident'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	
	source = 'MHCLG'
	tablename = 'sys_user'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem_task'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)

	source = 'CROYDON'
	tablename = 'task_sla'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)


	#update_tables('HEATSM', 'incident', None, None, start_date, end_date, end_database)
	source = 'HEATSM'
	tablename = 'Incident'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'servicereq'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'organizationalunit'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'change'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)


def run_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=False, print_details=False):

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################SERVICE NOW
##############################################################################################################################################################
##############################################################################################################################################################

	if print_internal == True:
		u_print(str(start_date) + " to " + str(end_date))

	source = 'HE'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident_alert'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'change_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'change_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	#tablename = 'kb_knowledge'


	source = 'CROYDON'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'change_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)

	source = 'MHCLG'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	##tablename = 'change_request'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)

	source = 'FSA'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	##tablename = 'change_request'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	#tablename = 'task_sla'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=print_internal, print_details=print_details)