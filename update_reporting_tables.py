
def update_tables(source, tablename, fields, filter_fields, start_date, end_date, end_database):

    global error_count
    errors = error_count

    return_info = query_source_data(source, tablename, fields, filter_fields, start_date, end_date)

    source_type = return_info[0]
    output_df = return_info[1]

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