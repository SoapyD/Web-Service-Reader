import os

exec(open("_main2.py").read())

db = 1 #REPORTING DATABASE
database = ''


delete_staging = False
print_internal = True
print_details = False
run_warehousing = True
time_type = 'days'
time_unit = 1



start_date = datetime.datetime(2019, 11, 16)
end_date = datetime.datetime(2019, 11, 17)
start_date = now.replace(microsecond=0) - datetime.timedelta(hours=6.0)
end_date = now.replace(microsecond=0) + datetime.timedelta(hours=1.0)

#CREATE TEMP WAREHOUSING TABLE NAMES HERE VIA A CLASS








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
	ws_process_class('HE','sys_user'),
	ws_process_class('HE','incident_task'),
	ws_process_class('HE','incident_alert'),
	ws_process_class('HE','sc_request'),
	ws_process_class('HE','sc_task'),
	ws_process_class('HE','change_request'),
	ws_process_class('HE','change_task'),
	ws_process_class('HE','problem'),
	ws_process_class('HE','problem_task'),
	ws_process_class('HE','task_sla'),
	ws_process_class('HE','sc_cat_item'),
	ws_process_class('HE_KB','kb_knowledge'),	
	ws_process_class('HE','incident',True, "he_incident"),
	ws_process_class('HE','sc_req_item',True, "he_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HE", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	ws_process_class('FSA','sys_user'),
	ws_process_class('FSA','incident_task'),
	ws_process_class('FSA','sc_request'),
	ws_process_class('FSA','sc_task'),
	ws_process_class('FSA','change_request'),
	ws_process_class('FSA','change_task'),
	ws_process_class('FSA','problem'),
	ws_process_class('FSA','problem_task'),
	###['FSA','task_sla',False, None],
	ws_process_class('FSA','sc_cat_item'),	
	ws_process_class('FSA','incident',True, "fsa_incident"),
	ws_process_class('FSA','sc_req_item',True, "fsa_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("FSA", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	ws_process_class('MHCLG','sys_user'),
	ws_process_class('MHCLG','incident_task'),
	ws_process_class('MHCLG','sc_request'),
	ws_process_class('MHCLG','sc_task'),
	ws_process_class('MHCLG','change_request'),
	ws_process_class('MHCLG','change_task'),
	ws_process_class('MHCLG','problem'),
	ws_process_class('MHCLG','problem_task'),
	ws_process_class('MHCLG','task_sla'),
	ws_process_class('MHCLG','sc_cat_item'),	
	ws_process_class('MHCLG','incident',True, "mhclg_incident"),
	ws_process_class('MHCLG','sc_req_item',True, "mhclg_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("MHCLG", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	ws_process_class('CROYDON','sys_user'),
	ws_process_class('CROYDON','incident_task'),
	ws_process_class('CROYDON','sc_request'),
	ws_process_class('CROYDON','sc_task'),
	ws_process_class('CROYDON','change_request'),
	ws_process_class('CROYDON','change_task'),
	ws_process_class('CROYDON','problem'),
	ws_process_class('CROYDON','problem_task'),
	ws_process_class('CROYDON','task_sla'),
	ws_process_class('CROYDON','sc_cat_item'),	
	ws_process_class('CROYDON','incident',True, "croydon_incident"),
	ws_process_class('CROYDON','sc_req_item',True, "croydon_request"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("CROYDON", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
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
	ws_process_class('LFLIVEEXTRACT','sessionincident'),
	ws_process_class('LFLIVEEXTRACT','he_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','fsa_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','mhclg_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','croydon_sessionincident'),
	ws_process_class('LFLIVEEXTRACT','sessionpostback'),
	ws_process_class('LFLIVEEXTRACT','completedsurveyresponse'),
	ws_process_class('LFLIVEEXTRACT','session',True, "lfliveextract_session"),
	ws_process_class('LFLIVEEXTRACT','completedsurvey',True, "lfliveextract_nps"),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACT", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)



process_list = [
	ws_process_class('LFLIVEEXTRACTNEW','sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','he_sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','fsa_sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','mhclg_sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','croydon_sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','enwl_sessionincident'),
	ws_process_class('LFLIVEEXTRACTNEW','sessionpostback'),
	ws_process_class('LFLIVEEXTRACTNEW','completedsurveyresponse'),
	ws_process_class('LFLIVEEXTRACTNEW','session',True, "lfliveextract_session"),
	ws_process_class('LFLIVEEXTRACTNEW','completedsurvey',True, "lfliveextract_nps"),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("LFLIVEEXTRACTNEW", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
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
	ws_process_class('TELEPHONYEXTRACT','call'),
]

#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("TELEPHONYEXTRACT", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
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
	ws_process_class('RINGCENTRAL','completedcontacts'),
	ws_process_class('RINGCENTRAL','agents'),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("RINGCENTRAL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)
""""""




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
	ws_process_class('ENWL','Employee'),
	#['ENWL','pir',False, None],
	#['ENWL','change',False, None],
	ws_process_class('ENWL','Problem'),
	ws_process_class('ENWL','Frs_data_escalation_watch'),
	ws_process_class('ENWL','Task'),
	ws_process_class('ENWL','ServiceReq',True, "enwl_request"),
	ws_process_class('ENWL','Incident',True, "enwl_incident"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("ENWL", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)


process_list = [
	ws_process_class('HEATSM','employee'),
	ws_process_class('HEATSM','organizationalunit'),
	ws_process_class('HEATSM','pir'),
	ws_process_class('HEATSM','change'),
	ws_process_class('HEATSM','problem'),
	ws_process_class('HEATSM','task'),
	ws_process_class('HEATSM','servicereq',True, "heatsm_request"),
	ws_process_class('HEATSM','incident',True, "heatsm_incident"),
]


#QUERY DATA AND MERGE IT INTO THE BASE TABLES AND TEMP WAREHOUSING TABLES
run_main("HEATSM", process_list, start_date, end_date, time_type, time_unit, db, database, run_warehousing,
	delete_staging, print_internal, print_details)