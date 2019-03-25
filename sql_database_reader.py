
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"_passwords\\_master.py").read()) #load the master password file
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file


def get_heat_data(source_type, source, tablename, start_date, end_date):

    start_time = datetime.datetime.now() #need for process time u_printing
    u_print('Start: '+str(start_time))

    output_df = pd.DataFrame()

    itt_start = start_date
    itt_end = start_date + datetime.timedelta(hours=24)
    if itt_end > end_date:
        itt_end = end_date

    u_print('--------------------------')

    run_loop = 1
    while run_loop == 1:

        if itt_end >= end_date: #check to see if the loop is over yet
            itt_end = end_date
            run_loop = 0  
  
        start_date_str = itt_start.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = itt_end.strftime('%Y-%m-%d %H:%M:%S')

        sql_filename = source + '_' + tablename
        
        u_print('running query: '+sql_filename+" between "+start_date_str+" and "+end_date_str)

        sqlfile = get_sql_query(sql_filename, path+'\\sql\\'+source_type+'_extract\\')
        sqlfile = sqlfile.replace("_@start", start_date_str)
        sqlfile = sqlfile.replace("_@end", end_date_str)
        #u_print(sqlfile)

        new_df = query_database(sqlfile, 0)
        u_print(str(new_df.shape[0])+" row returned")
        u_print('--------------------------')

        frames = [output_df, new_df]
        output_df = pd.concat(frames, sort=False)

        #ADVANCE THE DATE FILTER VALUES
        itt_start = itt_end
        itt_end = itt_end + datetime.timedelta(hours=24)

    output_df.to_csv('exports\\'+source+'_'+tablename+'.csv') 

    finish_time = datetime.datetime.now()
    u_print('End: '+str(finish_time))
    u_print('Time Taken: '+str(finish_time - start_time))

    return output_df

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################