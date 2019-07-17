import os
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string

this_dir = base_path+"Web-Service-Reader\\"


def update_tables(source, tablename, start_date, end_date, db, database, staging_tablename, delete_staging, print_details=False):

    global error_count
    errors = error_count

    return_info = query_source_data(source, tablename, start_date, end_date, print_details=print_details)

    source_type = return_info[0]
    output_df = return_info[1]
    fields = return_info[2]
    filter_fields = return_info[3]
    filter_mirror_fields = return_info[4]

    #MERGE DATA WITH MAIN DATABASE TABLE
    if errors == error_count:

        merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging, print_details=print_details)

    #u_print('') #ADD GAP TO PROCESS


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

def merge_data(output_df, source_type, source, tablename, fields, db, database, staging_tablename, delete_staging, print_details=False):

    sql_filepath = this_dir+'\\sql\\'+source_type+'_import\\'
    sql_filename = source + '_' + tablename

    merge_with_database(output_df, sql_filepath, sql_filename, tablename, fields, db, database, staging_tablename, delete_staging, print_details=print_details) #in FUNCTIONS_sql