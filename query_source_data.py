import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string

this_dir = base_path+"Web-Service-Reader\\"


def query_source_data(source, tablename, fields, filter_fields, start_date, end_date, run_extract=True, reference_list=None):

    import numpy as np
    global error_count

    output_df = pd.DataFrame()

    source_type = ''

    try:
        instancename = ''
        username = ''
        password = ''

        #GET DATA FROM SOURCE
        if source == 'HE':
            source_type = 'service_now'
            instancename = he_instancename
            username = he_username
            password = he_password
        if source == 'HE_TEST':
            source_type = 'service_now'
            instancename = he_test_instancename
            username = he_test_username
            password = he_test_password
        if source == 'FSA':    
            source_type = 'service_now'
            instancename = fsa_instancename
            username = fsa_username
            password = fsa_password
        if source == 'MHCLG':    
            source_type = 'service_now'
            instancename = mhclg_instancename
            username = mhclg_username
            password = mhclg_password
        if source == 'CROYDON':    
            source_type = 'service_now'
            instancename = croydon_instancename
            username = croydon_username
            password = croydon_password
        if source == 'DEV':    
            source_type = 'service_now'
            instancename = dev_instancename
            username = dev_username
            password = dev_password

        if source_type == 'service_now':
            output_df = get_servicenow_webservice_data(source, instancename, username, password, tablename, fields, filter_fields, start_date, end_date, 
                run_extract=run_extract, reference_list=reference_list)


        #THESE ARE ALL OUT OF DATE
        ##############################################################################################
        ##############################################################################################
        ##############################################################################################
        if source == 'HEAT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = this_dir+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)
        if source == 'LFLIVEEXTRACT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = this_dir+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)
        if source == 'TELEPHONYEXTRACT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = this_dir+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)

        ##############################################################################################
        ##############################################################################################
        ##############################################################################################

    except ValueError as e:
        u_print("###ERROR TRYING TO EXTRACT DATA###")
        u_print(str(e))
        error_count += 1

    #RELOCATED TO MERGE FUNCTION AS IT MAKES THINGS MORE UNIVERSAL
    if source_type == 'service_now' and run_extract == True:
        for field in fields:
            if field not in output_df.columns:
                output_df[field] = np.nan #add the missing field
                output_df[field+'_value'] = np.nan #add a corresponding value field  

    return_info = {}
    return_info[0] = source_type
    return_info[1] = output_df

    return return_info  

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################