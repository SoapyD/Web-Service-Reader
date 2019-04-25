import os
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string

this_dir = base_path+"Web-Service-Reader\\"


def update_tables(source, tablename, fields, filter_fields, start_date, end_date, db, database, staging_tablename, delete_staging):

    global error_count
    errors = error_count

    return_info = query_source_data(source, tablename, fields, filter_fields, start_date, end_date)

    source_type = return_info[0]
    output_df = return_info[1]

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:

        merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging)

    u_print('') #ADD GAP TO PROCESS


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

def merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging):

    sql_filepath = this_dir+'\\sql\\'+source_type+'_import\\'
    sql_filename = source + '_' + tablename

    merge_with_database(output_df, sql_filepath, sql_filename, tablename, fields, db, database, staging_tablename, delete_staging) #in FUNCTIONS_sql