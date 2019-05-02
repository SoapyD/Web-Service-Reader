

def run_test_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging):

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################SERVICE NOW
##############################################################################################################################################################
##############################################################################################################################################################

	staging_tablename = staging_tablename + '_TEST'
	delete_staging=False

	tablename = 'problem'
	
	source = 'HE'
	#ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)

	source = 'FSA'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	
	source = 'MHCLG'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)

	source = 'CROYDON'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)


def run_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging):

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################SERVICE NOW
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'HE'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident_alert'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'change_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'change_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	#tablename = 'kb_knowledge'


	source = 'CROYDON'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'change_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)

	source = 'MHCLG'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	##tablename = 'change_request'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)

	source = 'FSA'
	tablename = 'sys_user'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'incident'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_request'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_req_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	##tablename = 'change_request'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	##tablename = 'change_task'
	##ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'problem_task'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'task_sla'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)
	tablename = 'sc_cat_item'
	ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging)