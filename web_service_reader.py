
import os
path = os.getcwd() #get the current path
string_pos = path.index('Python') #find the python folder
base_path = path[:string_pos]+'Python\\' #create a base filepath string
exec(open(base_path+"_passwords\\_master.py").read()) #load the master password file
exec(open(base_path+"Functions\\functions.py").read()) #load the master password file


from datetime import datetime as d
import datetime
import time
import requests as r
import json



def get_webservice_data(instance, username, password, tablename, fields, start_date, end_date):

    limit = "&sysparm_limit=0"
    offset = "&sysparm_offset=0"
    query_limit = 10

    start_date = start_date.strftime('%Y%m%d%H%M%S')
    end_date = end_date.strftime('%Y%m%d%H%M%S')

    #THESE ARE THE CLOSED FILTERS
    filter_string = "sysparm_query=closed_at%3E%3D"+start_date+"%5Eclosed_at%3C%3D"+end_date+"%5ENQsys_updated_on%3E%3D"
    filter_string = filter_string+start_date+"%5Esys_updated_on%3C%3D"+end_date #THIS IS THE FILTER FOR CLOSED TICKETS

    #FORMAT THE URL TO RETURN THE NUMBER OF RECORDS THAT MATCH THE ABOVE FILTER CRITERIA
    url = instance+"/api/now/stats/"+tablename+"?sysparm_count=true&"+filter_string
    #RETURN THE STAT DATA FROM THE FORMATTED URL
    response = r.get(url, auth=(username, password))

    #PULL AND PRINT THE TICKET NUMBER FROM THE RETURNED DATA
    json_text = response.json()
    item_count = json_text['result']['stats']['count']
    item_count = int(item_count) #conver returned string to number
    print('pulling '+str(item_count)+' records from the '+tablename+' table in '+fsa_instancename)

    itt = 0
    total_itt = 0
    offset_itt = 0
    query_count = 0
    for i in range(item_count):
        itt+=1
        total_itt+=1

        if itt >= query_limit or total_itt == item_count:
            limit = "&sysparm_limit="+str(query_limit)
            offset = "&sysparm_offset="+str(offset_itt)        

            #Write-Host "RUNNING MAIN QUERY: $($query_count)"
            print('returning records '+str(offset_itt)+" to "+str(offset_itt+query_limit))

            #SEND OFF THE REQUEST
            url = instance+"/api/now/table/"+tablename+"?sysparm_display_value=all&"+filter_string+"&"+fields+limit+offset
            response = r.get(url, auth=(username, password))

            #UPDATE OFFSET VALUES
            offset_itt = offset_itt + itt
            itt = 0
            query_count+=1



fields = "sysparm_fields="
fields += "number"+"%2C"
fields += "company"+"%2C"
fields += "short_description"+"%2C"
fields += "priority"+"%2C"
fields += "state"+"%2C"
fields += "contact_type"+"%2C"
fields += "category"+"%2C"
fields += "subcategory"+"%2C"
fields += "u_first_fix"+"%2C"
fields += "assignment_group"+"%2C"
fields += "assigned_to"+"%2C"
fields += "close_code"+"%2C"
fields += "sys_created_by"+"%2C"
fields += "sys_created_on"+"%2C"
fields += "resolved_by"+"%2C"
fields += "resolved_at"+"%2C"
fields += "closed_by"+"%2C"
fields += "closed_at"+"%2C"
fields += "sys_updated_by"+"%2C"
fields += "sys_updated_on"+"%2C"
fields += "caller_id"+"%2C"
fields += "location"+"%2C"
fields += "active"+"%2C"
fields += "u_psupplier"+"%2C"
fields += "u_3rd_party_reference"+"%2C"
fields += "u_resolving_team"+"%2C"
fields += "u_fix_code"+"%2C"
fields += "u_exception_y_n"+"%2C"
fields += "u_exception_reason"+"%2C"
fields += "u_lf_comments"+"%2C"
fields += "u_agreed"+"%2C"
fields += "u_he_comments"+"%2C"
fields += "parent_incident"+"%2C"
fields += "company"+"%2C"
fields += "u_fa"+"%2C"
fields += "u_ft"+"%2C"

now = d.now()
end_date = now
start_date = now - datetime.timedelta(hours=2)

get_webservice_data(fsa_instancename, fsa_username, fsa_password, 'incident', fields, start_date, end_date)