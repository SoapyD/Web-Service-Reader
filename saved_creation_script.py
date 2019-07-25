def run_CREATION_process_stack(start_date, end_date, time_type, time_unit, db, database, staging_tablename, delete_staging, print_internal=False, print_details=False):

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

