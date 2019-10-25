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
run_main("HEATSM", process_list, start_date, end_date, time_type, time_unit, run_warehousing,
	delete_staging, print_internal, print_details)
