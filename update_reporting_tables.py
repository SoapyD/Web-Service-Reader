

def update_tables(source, tablename, fields, filter_fields, start_date, end_date, end_database):

    output_df = pd.DataFrame()

    source_type = ''

    global error_count
    errors = error_count

    try:
        #GET DATA FROM SOURCE
        if source == 'HE':
            output_df = get_servicenow_webservice_data(source, he_instancename, he_username, he_password, tablename, fields, filter_fields, start_date, end_date)
            source_type = 'service_now'
        if source == 'FSA':    
            output_df = get_servicenow_webservice_data(source, fsa_instancename, fsa_username, fsa_password, tablename, fields, filter_fields, start_date, end_date)
            source_type = 'service_now'
        if source == 'MHCLG':    
            output_df = get_servicenow_webservice_data(source, mhclg_instancename, mhclg_username, mhclg_password, tablename, fields, filter_fields, start_date, end_date)
            source_type = 'service_now'
        if source == 'CROYDON':    
            output_df = get_servicenow_webservice_data(source, croydon_instancename, croydon_username, croydon_password, tablename, fields, filter_fields, start_date, end_date)
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

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:
        #merge_with_database(output_df, source_type, source, tablename, fields, end_database)

        sql_filepath = path+'\\sql\\'+source_type+'_import\\'
        sql_filename = source + '_' + tablename

        merge_with_database(output_df, sql_filepath, sql_filename, tablename, fields, end_database) #in FUNCTIONS_sql

    u_print('') #ADD GAP TO PROCESS


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################