
def query_source_data(source, tablename, fields, filter_fields, start_date, end_date, run_extract=True):

    import numpy as np
    global error_count

    output_df = pd.DataFrame()

    source_type = ''

    try:
        #GET DATA FROM SOURCE
        if source == 'HE':
            output_df = get_servicenow_webservice_data(source, he_instancename, he_username, he_password, tablename, fields, filter_fields, start_date, end_date, run_extract=run_extract)
            source_type = 'service_now'
        if source == 'FSA':    
            output_df = get_servicenow_webservice_data(source, fsa_instancename, fsa_username, fsa_password, tablename, fields, filter_fields, start_date, end_date, run_extract=run_extract)
            source_type = 'service_now'
        if source == 'MHCLG':    
            output_df = get_servicenow_webservice_data(source, mhclg_instancename, mhclg_username, mhclg_password, tablename, fields, filter_fields, start_date, end_date, run_extract=run_extract)
            source_type = 'service_now'
        if source == 'CROYDON':    
            output_df = get_servicenow_webservice_data(source, croydon_instancename, croydon_username, croydon_password, tablename, fields, filter_fields, start_date, end_date, run_extract=run_extract)
            source_type = 'service_now'

        if source == 'HEAT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = path+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)
        if source == 'LFLIVEEXTRACT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = path+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)
        if source == 'TELEPHONYEXTRACT':
            source_type = 'live'
            #output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
            sql_filepath = path+'\\sql\\'+source_type+'_extract\\'
            sql_filename = source + '_' + tablename
            output_df = sql_query_parser(sql_filepath, sql_filename, tablename, start_date, end_date)

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