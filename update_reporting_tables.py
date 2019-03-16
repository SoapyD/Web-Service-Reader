

def update_tables(source, tablename, fields, filter_fields, start_date, end_date):

    output_df = pd.DataFrame()

    source_type = ''
    errors = 0

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
        print("###ERROR TRYING TO EXTRACT DATA###")
        print(e)
        errors += 1

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == 0:
        try:
            merge_with_database(output_df, source_type, source, tablename, fields)
        except:
            print("###ERROR TRYING TO MERGE TO MAIN TABLE###")
            #print(ValueError)
            errors += 1
    else:
        errors += 1

    return errors

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################


def merge_with_database(output_df, source_type, source, tablename, fields):

    max_rows = 500

    row_count = output_df.shape[0] #GET THE ROW COUNT
    #IF THERE#S BEEN ANY RESULTING DF RETURNED
    if output_df is not None and row_count > 0:

        print('slicing output...')
        start_time = datetime.datetime.now() #need for process time printing
        print('Start: '+str(start_time))

        run_loop = 1
        offset = 0
        
        print(str(row_count)+' rows to slice. max slice size is '+str(max_rows))
        print('--------------------------')

        while run_loop == 1:
            slice_end = offset+max_rows
            if slice_end >= row_count:
                slice_end = row_count
                run_loop = 0

            #SLICE THE DF
            df_slice = output_df.iloc[offset:slice_end]
            slice_range = str(offset+1)+' to '+str(slice_end)
            print('unloading slice '+slice_range)

            #SEND THE SLICE TO THE DEV DATABASE
            bulk_insert_to_database(df_slice, 2, 'stg', filename='rows '+slice_range, runtype='replace')

            #RUN CHECK SCRIPT, WHICH CHECKS FOR ALL QUERIED FIELDS AND ADDS THEM IF THEY'RE MISSING
            #NULL FIELDS IN A QUERY DON'T RETURN ANY DATA, THIS IS HERE TO ENSURE THE QUERIES RUN PROPERLY
            
            if source_type == 'service_now':
                sqlfile = ""
                for item in fields:
                    sqlfile += "IF COL_LENGTH('dbo.stg', '"+item+"') IS NULL "
                    sqlfile += "BEGIN "
                    sqlfile += "ALTER TABLE dbo.stg ADD "+item+" varchar(MAX) "
                    sqlfile += "END; "

                query_database2('Add missing field', sqlfile, 2)

            #RUN MERGE SCRIPT
            sql_filename = source + '_' + tablename
            sqlfile = get_sql_query(sql_filename, path+'\\sql\\'+source_type+'\\')
            query_database2('merge to main table', sqlfile, 2)

            print('--------------------------')

            offset += max_rows

        finish_time = datetime.datetime.now()
        print('End: '+str(finish_time))
        print('Time Taken: '+str(finish_time - start_time))