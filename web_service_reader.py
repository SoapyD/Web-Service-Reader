
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"_passwords\\_master.py").read()) #load the master password file
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file

exec(open("return_field_list.py").read())

from datetime import datetime as d
import datetime
import time
import requests as r
import json
import pandas as pd
import sys


def get_servicenow_webservice_data(source, instance, username, password, tablename, fields, filter_fields, start_date, end_date):

    start_time = datetime.datetime.now() #need for process time printing

    limit = "&sysparm_limit=0"
    offset = "&sysparm_offset=0"
    query_limit = 500

    start_date = start_date.strftime('%Y%m%d%H%M%S')
    end_date = end_date.strftime('%Y%m%d%H%M%S')

    #THESE ARE THE CLOSED FILTERS
    filter_string = "sysparm_query="

    use_or = 0
    for item in filter_fields:
        
        if use_or == 1:
            #^NQ which I think means OR
            filter_string += "%5ENQ"
        #TRANSLATION: item>=start_date^item<=end_date
        filter_string += item+"%3E%3D"+start_date+"%5E"+item+"%3C%3D"+end_date
        use_or = 1

    #FORMAT THE URL TO RETURN THE NUMBER OF RECORDS THAT MATCH THE ABOVE FILTER CRITERIA
    url = instance+"/api/now/stats/"+tablename+"?sysparm_count=true&"+filter_string
    #RETURN THE STAT DATA FROM THE FORMATTED URL
    response = r.get(url, auth=(username, password))

    output_df = pd.DataFrame()

    #if the stats query worked, run the next part of the process 
    if response.status_code == 200:
        #PULL AND PRINT THE TICKET NUMBER FROM THE RETURNED DATA
        json_text = response.json()
        item_count = json_text['result']['stats']['count']
        item_count = int(item_count) #conver returned string to number

        print('########################################')
        print('pulling '+str(item_count)+' records from the '+tablename+' table in '+instance)
        #print('########################################')

        itt = 0
        total_itt = 0
        offset_itt = 0
        query_count = 0
        used = 0

        for i in range(item_count):
            itt+=1
            total_itt+=1

            if itt >= query_limit or total_itt == item_count:
                limit = "&sysparm_limit="+str(query_limit)
                offset = "&sysparm_offset="+str(offset_itt)        
                
                print('returning records '+str(offset_itt)+" to "+str(offset_itt+query_limit)) 

                #SEND OFF THE REQUEST
                url = instance+"/api/now/table/"+tablename+"?sysparm_display_value=all&"+filter_string+"&"+fields+limit+offset
                response = r.get(url, auth=(username, password))

                if response.status_code == 200:
                    json_text = response.json()

                    j_dump = json.dumps(json_text['result'], sort_keys=True,indent=4, separators=(',', ': ')) #pull out the value data from the json file, messages are stored in value
                    df = pd.read_json(j_dump) #read the json file into a dataframe
                    

                    #FLATTERN THE JSON INFORMATION SO WE NO LONGER HAVE NESTED DISPLAY_VALUE AND VALUE IN THE SAME CELL
                    for row in df.iterrows():
                        data = row[1] #GET ROW DATA

                        #CREATE TWO ARRAYS THAT'LL STORE THE NEW INDEX AND RELATED VALUES
                        my_list = []
                        index_list = []

                        #LOOP TROUGH EACH VALUE OF THE ROW
                        for index, item in data.items():
                            test = json.dumps(item, sort_keys=True,indent=4, separators=(',', ': ')) #
                            test = json.loads(test)

                            #LOOP THROUGH THE SUB VALUES IN THE ROW VALUES
                            saved_val = ''
                            for part in test:
                                value = str(test[part])
                                if value == "None":
                                    value = ""
                                #print(index + " -- " + part + " -- " + value)
                                #ONLY SAVE THE VALUE IF IT'S NEW AND ISN'T A LINK
                                if value.lower() != saved_val.lower() and part != 'link':
                                    index_list.append(index + "_" + part)
                                    my_list.append(value)
                                    saved_val = value

                        new_df = pd.DataFrame(my_list,index=index_list)
                        new_df = new_df.transpose() #TRANSPOSE THE VALUES SO THEY'RE COLUMNS AND NOW ROWS
                        frames = [output_df, new_df]
                        output_df = pd.concat(frames, sort=False)

                    #UPDATE OFFSET VALUES
                    offset_itt = offset_itt + itt
                    itt = 0
                    query_count+=1

                else:
                    print('Error while running table extract query')
                    print(response.text)

        output_df = output_df.reset_index(drop=True)
        output_df.to_csv('exports\\'+source+'_'+tablename+'.csv')

    else:
        print('Error while running stats query')
        print(response.text)

    finish_time = datetime.datetime.now()
    #print('########################################')
    print('REST QUERY COMPLETE')
    #print('Start: '+str(start_time))
    #print('End: '+str(finish_time))
    print('Time Taken: '+str(finish_time - start_time))
    print('########################################')
    #print('')

    return output_df

def update_webservice_tables(source, tablename, fields, filter_fields, start_date, end_date):

    output_df = pd.DataFrame()

    if source == 'HE':
        output_df = get_servicenow_webservice_data(source, he_instancename, he_username, he_password, tablename, fields, filter_fields, start_date, end_date)
    if source == 'FSA':    
        output_df = get_servicenow_webservice_data(source, fsa_instancename, fsa_username, fsa_password, tablename, fields, filter_fields, start_date, end_date)
    if source == 'MHCLG':    
        output_df = get_servicenow_webservice_data(source, mhclg_instancename, mhclg_username, mhclg_password, tablename, fields, filter_fields, start_date, end_date)

    #PASS TABLES TO DATABASE

    #BREAK DOWN ANY RETURNED DATA INTO SMALLER CHUNKS SO LARGE UPLOADS DON'T CHOKE THE PROCESS OUT

    #BULK UPLOAD DATA

    #RUN MERGE SCRIPT

