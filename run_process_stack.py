

def process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=True):

	#THIS HAS BEEN REMOVED AS IT'S NOT NEEDED HERE AT ALL
	#if run_creation == True:
	#	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)

	
	#run = True
	#if run == True:
	try:
		TEST_ready_process(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal=print_internal, print_details=print_details)
	except:
		u_print("ERROR: Data import could not be run")


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
	user_picked_fields = None
	#user_picked_fields = ['sys_id','number','causecode','lastmoddatetime']

#     # ####### 
#     # #       
#     # #       
####### #####   
#     # #       
#     # #       
#     # ####### 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HE
##############################################################################################################################################################
##############################################################################################################################################################
	
	source = 'HE'
	tablename = 'sys_user'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident_alert'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_req_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'task_sla'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_cat_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)



#######  #####     #    
#       #     #   # #   
#       #        #   #  
#####    #####  #     # 
#             # ####### 
#       #     # #     # 
#        #####  #     # 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################FSA
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'FSA'
	tablename = 'sys_user'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_req_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'task_sla'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_cat_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)



#     # #     #  #####  #        #####  
##   ## #     # #     # #       #     # 
# # # # #     # #       #       #       
#  #  # ####### #       #       #  #### 
#     # #     # #       #       #     # 
#     # #     # #     # #       #     # 
#     # #     #  #####  #######  #####  

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################MHCLG
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'MHCLG'
	tablename = 'sys_user'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_req_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'task_sla'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_cat_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)


 #####  ######  ####### #     # ######  ####### #     # 
#     # #     # #     #  #   #  #     # #     # ##    # 
#       #     # #     #   # #   #     # #     # # #   # 
#       ######  #     #    #    #     # #     # #  #  # 
#       #   #   #     #    #    #     # #     # #   # # 
#     # #    #  #     #    #    #     # #     # #    ## 
 #####  #     # #######    #    ######  ####### #     # 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################CROYDON
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'CROYDON'
	tablename = 'sys_user'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'incident_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_req_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_request'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem_task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'task_sla'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sc_cat_item'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)


#     # #######    #    ####### 
#     # #         # #      #    
#     # #        #   #     #    
####### #####   #     #    #    
#     # #       #######    #    
#     # #       #     #    #    
#     # ####### #     #    #    

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HEAT
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'HEATSM'
	tablename = 'incident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'servicereq'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'problem'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'change'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'pir'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'organizationalunit'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'employee'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'task'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)



#       ####### #       ### #     # ####### 
#       #       #        #  #     # #       
#       #       #        #  #     # #       
#       #####   #        #  #     # #####   
#       #       #        #   #   #  #       
#       #       #        #    # #   #       
####### #       ####### ###    #    ####### 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################LFLIVEEXTRACT
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'LFLIVEEXTRACT'
	tablename = 'session'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sessionincident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'he_sessionincident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'fsa_sessionincident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'mhclg_sessionincident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'croydon_sessionincident'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'sessionpostback'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'completedsurvey'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)
	tablename = 'completedsurveyresponse'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)



####### ####### #       ####### ######  #     # ####### #     # #     # 
   #    #       #       #       #     # #     # #     # ##    #  #   #  
   #    #       #       #       #     # #     # #     # # #   #   # #   
   #    #####   #       #####   ######  ####### #     # #  #  #    #    
   #    #       #       #       #       #     # #     # #   # #    #    
   #    #       #       #       #       #     # #     # #    ##    #    
   #    ####### ####### ####### #       #     # ####### #     #    #  

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################TELEPHONY
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'TELEPHONYEXTRACT'
	tablename = 'call'
	generate_creation_query(source, tablename, user_picked_fields, db, database, print_details=print_details)






#     # ####### #     #             ######  ######  #######  #####  #######  #####   #####  
##    # #       #  #  #             #     # #     # #     # #     # #       #     # #     # 
# #   # #       #  #  #             #     # #     # #     # #       #       #       #       
#  #  # #####   #  #  #    #####    ######  ######  #     # #       #####    #####   #####  
#   # # #       #  #  #             #       #   #   #     # #       #             #       # 
#    ## #       #  #  #             #       #    #  #     # #     # #       #     # #     # 
#     # #######  ## ##              #       #     # #######  #####  #######  #####   #####  


def run_process_stack_2(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=False, print_details=False):

	if print_internal == True:
		u_print(str(start_date) + " to " + str(end_date))

	#generate_merge_query(source, tablename, user_picked_fields)	DO NOT DELETE!!!!!!!!
	user_picked_fields = None
	#user_picked_fields = ['sys_id','number','causecode','lastmoddatetime']

#     # ####### 
#     # #       
#     # #       
####### #####   
#     # #       
#     # #       
#     # ####### 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HE
##############################################################################################################################################################
##############################################################################################################################################################
	
	source = 'HE'
	tablename = 'sys_user'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident_alert'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_req_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'task_sla'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_cat_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



#######  #####     #    
#       #     #   # #   
#       #        #   #  
#####    #####  #     # 
#             # ####### 
#       #     # #     # 
#        #####  #     # 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################FSA
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'FSA'
	tablename = 'sys_user'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_req_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'task_sla'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_cat_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



#     # #     #  #####  #        #####  
##   ## #     # #     # #       #     # 
# # # # #     # #       #       #       
#  #  # ####### #       #       #  #### 
#     # #     # #       #       #     # 
#     # #     # #     # #       #     # 
#     # #     #  #####  #######  #####  

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################MHCLG
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'MHCLG'
	tablename = 'sys_user'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_req_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'task_sla'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_cat_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



 #####  ######  ####### #     # ######  ####### #     # 
#     # #     # #     #  #   #  #     # #     # ##    # 
#       #     # #     #   # #   #     # #     # # #   # 
#       ######  #     #    #    #     # #     # #  #  # 
#       #   #   #     #    #    #     # #     # #   # # 
#     # #    #  #     #    #    #     # #     # #    ## 
 #####  #     # #######    #    ######  ####### #     # 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################CROYDON
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'CROYDON'
	tablename = 'sys_user'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'incident_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_req_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_request'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem_task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'task_sla'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sc_cat_item'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



#     # #######    #    ####### 
#     # #         # #      #    
#     # #        #   #     #    
####### #####   #     #    #    
#     # #       #######    #    
#     # #       #     #    #    
#     # ####### #     #    #    

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################HEAT
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'HEATSM'
	tablename = 'incident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'servicereq'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'problem'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'change'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'pir'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'organizationalunit'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	#tablename = 'frs_data_escalation_watch'
	tablename = 'employee'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'task'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



#       ####### #       ### #     # ####### 
#       #       #        #  #     # #       
#       #       #        #  #     # #       
#       #####   #        #  #     # #####   
#       #       #        #   #   #  #       
#       #       #        #    # #   #       
####### #       ####### ###    #    ####### 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################LFLIVEEXTRACT
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'LFLIVEEXTRACT'
	tablename = 'session'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'he_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'fsa_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'mhclg_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'croydon_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'sessionpostback'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'completedsurvey'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)
	tablename = 'completedsurveyresponse'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)



	source = 'LFLIVEEXTRACTNEW'
	tablename = 'session'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'he_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'fsa_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'mhclg_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'croydon_sessionincident'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'sessionpostback'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'completedsurvey'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)
	tablename = 'completedsurveyresponse'
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details, run_creation=False)



####### ####### #       ####### ######  #     # ####### #     # #     # 
   #    #       #       #       #     # #     # #     # ##    #  #   #  
   #    #       #       #       #     # #     # #     # # #   #   # #   
   #    #####   #       #####   ######  ####### #     # #  #  #    #    
   #    #       #       #       #       #     # #     # #   # #    #    
   #    #       #       #       #       #     # #     # #    ##    #    
   #    ####### ####### ####### #       #     # ####### #     #    #  

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################TELEPHONY
##############################################################################################################################################################
##############################################################################################################################################################

	source = 'TELEPHONYEXTRACT'
	tablename = 'call'

	#DELETE ANY TELEPHONY DATA FOR THE GIVEN RANGE,
	#THIS NEEDS TO BE DONE AS CALLS ARE PASSED BETWEEN AGENTS IN ODD WAYS WHICH GET REFLECTED IN THE TELEPHONY DATA
	#THIS WOULD PRODUCE DUPLICATES IN MY DATA WHICH ARE EFFECTIVELY REMOVED FROM THE MAIN DATASOURCE
	sqlfile = "DELETE FROM TELEPHONYEXTRACT_call WHERE datetime BETWEEN CONVERT(DATE,'@start_date') AND CONVERT(DATE,'@end_date')"
	sqlfile = sqlfile.replace("@start_date", str(start_date))
	sqlfile = sqlfile.replace("@end_date", str(end_date))		
	#print(sqlfile)
	query_database2('deleting telephony data',sqlfile, db, database)

	#THEN RUN THE UPDATE PROCESS
	process_stack_subprocess(source, tablename, start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, user_picked_fields, print_internal, print_details)







####### #       ######              ######  ######  #######  #####  #######  #####   #####  
#     # #       #     #             #     # #     # #     # #     # #       #     # #     # 
#     # #       #     #             #     # #     # #     # #       #       #       #       
#     # #       #     #    #####    ######  ######  #     # #       #####    #####   #####  
#     # #       #     #             #       #   #   #     # #       #             #       # 
#     # #       #     #             #       #    #  #     # #     # #       #     # #     # 
####### ####### ######              #       #     # #######  #####  #######  #####   #####  


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