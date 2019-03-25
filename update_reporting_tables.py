

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
            output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
        if source == 'LFLIVEEXTRACT':
            source_type = 'live'
            output_df = get_heat_data(source_type, source, tablename, start_date, end_date)
        if source == 'TELEPHONYEXTRACT':
            source_type = 'live'
            output_df = get_heat_data(source_type, source, tablename, start_date, end_date)


    except ValueError as e:
        u_print("###ERROR TRYING TO EXTRACT DATA###")
        u_print(str(e))
        error_count += 1

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:
        merge_with_database(output_df, source_type, source, tablename, fields, end_database)

    u_print('') #ADD GAP TO PROCESS


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################


def merge_with_database(output_df, source_type, source, tablename, fields, end_database):

    max_rows = 500

    row_count = output_df.shape[0] #GET THE ROW COUNT
    #IF THERE#S BEEN ANY RESULTING DF RETURNED
    if output_df is not None and row_count > 0:

        u_print('slicing output...')
        start_time = datetime.datetime.now() #need for process time u_printing
        u_print('Start: '+str(start_time))

        run_loop = 1
        offset = 0
        
        u_print(str(row_count)+' rows to slice. max slice size is '+str(max_rows))
        u_print('--------------------------')

        while run_loop == 1:
            slice_end = offset+max_rows
            if slice_end >= row_count:
                slice_end = row_count
                run_loop = 0

            #SLICE THE DF
            df_slice = output_df.iloc[offset:slice_end]
            slice_range = str(offset+1)+' to '+str(slice_end)
            u_print('unloading slice '+slice_range)

            #SEND THE SLICE TO THE DEV DATABASE
            global error_count
            errors = error_count
            bulk_insert_to_database(df_slice, end_database, 'stg', filename='rows '+slice_range, runtype='replace')

            if (errors == error_count): #IF NO ERRORS HAVE BEEN ADDED, THEN PROCEEED
                #RUN CHECK SCRIPT, WHICH CHECKS FOR ALL QUERIED FIELDS AND ADDS THEM IF THEY'RE MISSING
                #NULL FIELDS IN A QUERY DON'T RETURN ANY DATA, THIS IS HERE TO ENSURE THE QUERIES RUN PROPERLY
                if source_type == 'service_now':
                    sqlfile = ""
                    for item in fields:
                        sqlfile += "IF COL_LENGTH('dbo.stg', '"+item+"') IS NULL "
                        sqlfile += "BEGIN "
                        sqlfile += "ALTER TABLE dbo.stg ADD "+item+" varchar(MAX) "
                        sqlfile += "END; "

                    query_database2('Add missing field', sqlfile, end_database)

                #RUN MERGE SCRIPT
                sql_filename = source + '_' + tablename
                sqlfile = get_sql_query(sql_filename, path+'\\sql\\'+source_type+'\\')
                query_database2('merge to main table', sqlfile, end_database)

                u_print('--------------------------')

                offset += max_rows

        finish_time = datetime.datetime.now()
        u_print('End: '+str(finish_time))
        u_print('Time Taken: '+str(finish_time - start_time))