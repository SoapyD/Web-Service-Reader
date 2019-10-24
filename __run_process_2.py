import os

exec(open("_main2.py").read())


delete_staging = True
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 7



#start_date = datetime.datetime(2019, 10, 22)
#end_date = datetime.datetime(2019, 10, 25)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=2.0)
end_date = now.replace(microsecond=0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS



### #     #    #    #     # ####### ###             ### #     #  #####  ####### 
 #  #     #   # #   ##    #    #     #               #  ##    # #     #    #    
 #  #     #  #   #  # #   #    #     #               #  # #   # #          #    
 #  #     # #     # #  #  #    #     #     #####     #  #  #  #  #####     #    
 #   #   #  ####### #   # #    #     #               #  #   # #       #    #    
 #    # #   #     # #    ##    #     #               #  #    ## #     #    #    
###    #    #     # #     #    #    ###             ### #     #  #####     #    

##############################################################################################################################
##############################################################################################################################
########################################################IVANTI INSTANCE
##############################################################################################################################
##############################################################################################################################


process_list = [
	['HEATSM','employee',False, None],
	['HEATSM','organizationalunit',False, None],
	['HEATSM','pir',False, None],
	['HEATSM','change',False, None],
	['HEATSM','problem',False, None],
	['HEATSM','task',False, None],
	['HEATSM','servicereq',True, "heatsm_request"],
	['HEATSM','incident',True, "heatsm_incident"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HEATSM", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)



process_list = [
	['ENWL','Employee',False, None],
	#['ENWL','pir',False, None],
	#['ENWL','change',False, None],
	['ENWL','Problem',False, None],
	['ENWL','Frs_data_escalation_watch',False, None],
	['ENWL','Task',False, None],
	['ENWL','ServiceReq',True, "enwl_request"],
	['ENWL','Incident',True, "enwl_incident"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("ENWL", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)



 #####  ####### ######  #     # ###  #####  ####### #     # ####### #     #             ### #     #  #####  ####### 
#     # #       #     # #     #  #  #     # #       ##    # #     # #  #  #              #  ##    # #     #    #    
#       #       #     # #     #  #  #       #       # #   # #     # #  #  #              #  # #   # #          #    
 #####  #####   ######  #     #  #  #       #####   #  #  # #     # #  #  #    #####     #  #  #  #  #####     #    
      # #       #   #    #   #   #  #       #       #   # # #     # #  #  #              #  #   # #       #    #    
#     # #       #    #    # #    #  #     # #       #    ## #     # #  #  #              #  #    ## #     #    #    
 #####  ####### #     #    #    ###  #####  ####### #     # #######  ## ##              ### #     #  #####     #   

##############################################################################################################################
##############################################################################################################################
########################################################SERVICENOW INSTANCE
##############################################################################################################################
##############################################################################################################################

process_list = [
	['HE','sys_user',False, None],
	['HE','incident_task',False, None],
	['HE','incident_alert',False, None],
	['HE','sc_request',False, None],
	['HE','sc_task',False, None],
	['HE','change_request',False, None],
	['HE','change_task',False, None],
	['HE','problem',False, None],
	['HE','problem_task',False, None],
	['HE','task_sla',False, None],
	['HE','sc_cat_item',False, None],
	['HE_KB','kb_knowledge',False, None],	
	['HE','incident',True, "he_incident"],
	['HE','sc_req_item',True, "he_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	['FSA','sys_user',False, None],
	['FSA','incident_task',False, None],
	['FSA','sc_request',False, None],
	['FSA','sc_task',False, None],
	['FSA','change_request',False, None],
	['FSA','change_task',False, None],
	['FSA','problem',False, None],
	['FSA','problem_task',False, None],
	###['FSA','task_sla',False, None],
	['FSA','sc_cat_item',False, None],	
	['FSA','incident',True, "fsa_incident"],
	['FSA','sc_req_item',True, "fsa_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("FSA", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	['MHCLG','sys_user',False, None],
	['MHCLG','incident_task',False, None],
	['MHCLG','sc_request',False, None],
	['MHCLG','sc_task',False, None],
	['MHCLG','change_request',False, None],
	['MHCLG','change_task',False, None],
	['MHCLG','problem',False, None],
	['MHCLG','problem_task',False, None],
	['MHCLG','task_sla',False, None],
	['MHCLG','sc_cat_item',False, None],	
	['MHCLG','incident',True, "mhclg_incident"],
	['MHCLG','sc_req_item',True, "mhclg_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("MHCLG", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	['CROYDON','sys_user',False, None],
	['CROYDON','incident_task',False, None],
	['CROYDON','sc_request',False, None],
	['CROYDON','sc_task',False, None],
	['CROYDON','change_request',False, None],
	['CROYDON','change_task',False, None],
	['CROYDON','problem',False, None],
	['CROYDON','problem_task',False, None],
	['CROYDON','task_sla',False, None],
	['CROYDON','sc_cat_item',False, None],	
	['CROYDON','incident',True, "croydon_incident"],
	['CROYDON','sc_req_item',True, "croydon_request"],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("CROYDON", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)





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

process_list = [
	['LFLIVEEXTRACT','sessionincident',False, None],
	['LFLIVEEXTRACT','he_sessionincident',False, None],
	['LFLIVEEXTRACT','fsa_sessionincident',False, None],
	['LFLIVEEXTRACT','mhclg_sessionincident',False, None],
	['LFLIVEEXTRACT','croydon_sessionincident',False, None],
	['LFLIVEEXTRACT','sessionpostback',False, None],
	['LFLIVEEXTRACT','completedsurveyresponse',False, None],
	['LFLIVEEXTRACT','session',True, "lfliveextract_session"],
	['LFLIVEEXTRACT','completedsurvey',True, "lfliveextract_nps"],
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACT", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)



process_list = [
	['LFLIVEEXTRACTNEW','sessionincident',False, None],
	['LFLIVEEXTRACTNEW','he_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','fsa_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','mhclg_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','croydon_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','enwl_sessionincident',False, None],
	['LFLIVEEXTRACTNEW','sessionpostback',False, None],
	['LFLIVEEXTRACTNEW','completedsurveyresponse',False, None],
	['LFLIVEEXTRACTNEW','session',True, "lfliveextract_session"],
	['LFLIVEEXTRACTNEW','completedsurvey',True, "lfliveextract_nps"],
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACTNEW", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)


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

process_list = [
	['TELEPHONYEXTRACT','call',False, None],
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("TELEPHONYEXTRACT", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)



######  ### #     #  #####               #####  ####### #     # ####### ######     #    #       
#     #  #  ##    # #     #             #     # #       ##    #    #    #     #   # #   #       
#     #  #  # #   # #                   #       #       # #   #    #    #     #  #   #  #       
######   #  #  #  # #  ####    #####    #       #####   #  #  #    #    ######  #     # #       
#   #    #  #   # # #     #             #       #       #   # #    #    #   #   ####### #       
#    #   #  #    ## #     #             #     # #       #    ##    #    #    #  #     # #       
#     # ### #     #  #####               #####  ####### #     #    #    #     # #     # ####### 

##############################################################################################################################################################
##############################################################################################################################################################
###############################################################################RINGCENTRAL
##############################################################################################################################################################
##############################################################################################################################################################

process_list = [
	['RINGCENTRAL','completedcontacts',False, ""],
	['RINGCENTRAL','agents',False, ""],
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)
