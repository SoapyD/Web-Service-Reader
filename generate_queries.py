

def generate_creation_query(source, tablename, user_picked_fields=None, db=None, database=None, print_details=False):

	table_info = None
	return_info = get_source_type(source)
	source_type = return_info[0] 

	if source_type == 'service_now':
		table_info = return_servicenow_field_list_2(tablename)
	if source_type == 'live':
		table_info = return_heat_field_list_2(tablename)	

	#print(table_info)

	return_info = get_field_info(source, table_info, user_picked_fields)
	selected_fields = return_info[0]
	selected_source_fields = return_info[1]
	selected_mirror_fields = return_info[2]
	create_sql = generate_create_sql(source, tablename, table_info, selected_fields)
	print(create_sql)

	drop_script = "DROP TABLE "+source+"_"+tablename

	if db:
		query_db_powershell(drop_script, db, database=database, print_details=print_details)
		query_db_powershell(create_sql, db, database=database, print_details=print_details)


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  


def generate_drop_query(source, tablename, user_picked_fields=None, db=None, database=None, print_details=False):
	table_info = None
	return_info = get_source_type(source)
	source_type = return_info[0] 

	if source_type == 'service_now':
		table_info = return_servicenow_field_list_2(tablename)
	if source_type == 'live':
		table_info = return_heat_field_list_2(tablename)	

	drop_script = "DROP TABLE "+source+"_"+tablename

	#with open('exports\\'+'drop_table.sql', 'w') as file:
	#  file.write(drop_script)
	#file.close()

	if db:
		query_db_powershell(drop_script, db, database=database, print_details=print_details)


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################  


def generate_merge_query(source, tablename, user_picked_fields=None):

	table_info = None
	return_info = get_source_type(source)
	source_type = return_info[0] 

	if source_type == 'service_now':
		table_info = return_servicenow_field_list_2(tablename)
	if source_type == 'live':
		table_info = return_heat_field_list_2(tablename)	

	return_info = get_field_info(source, table_info, user_picked_fields)
	selected_fields = return_info[0]
	selected_source_fields = return_info[1]
	selected_mirror_fields = return_info[2]
	create_sql = generate_merge_sql(source, tablename, table_info, selected_fields)
	#print(create_sql)

	with open('exports\\merge_query.sql', 'w') as file:
	  file.write(create_sql)
	file.close()


###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################    

def generate_create_sql(source, tablename, table_info, selected_fields):
	first_pass = True
	create_string = ''

	#used the selected fields to create the various listed fields in the merge script
	for field in selected_fields:
		if first_pass == False:
			create_string += ',\n'

		create_string += field.mirror_name+" "+field.mirror_type

		first_pass = False

	#OVERRIDE THE DESTINATION TABLE IF THERE'S AN OVERRIDE NAME PRESENT
	output_table = source+"_"+tablename
	if table_info.output_table_override != '':
		output_table = table_info.output_table_override

	#DEFINE THE STRINGS SO THEY HAVE A BEGINNING AND ENDING TO ANY FIELDS REFERENCED
	create_string = "CREATE TABLE "+output_table+"(\n"+create_string+"\n)\n"

	#COMBINE THE STRINGS TOGETHER INTO A FINAL MERGE SQL STRING AND RETURN IT
	create_sql = create_string

	return create_sql	

###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################   

def generate_merge_sql(source, tablename, table_info, selected_fields):

	first_pass = True
	create_string = ''
	import_string = ''  
	target_source_string = ''
	insert_string = ''
	source_string = ''
	join_string = ''
	output_table = source+"_"+tablename
	#OVERRIDE THE DESTINATION TABLE IF THERE'S AN OVERRIDE NAME PRESENT
	if table_info.output_table_override != '':
		output_table = table_info.output_table_override

	#used the selected fields to create the various listed fields in the merge script
	for field in selected_fields:
		if first_pass == False:
			create_string += ',\n'
			import_string += ',\n' 
			target_source_string += ',\n' 
			insert_string += ',\n' 
			source_string += ',\n'         

		create_string += field.mirror_name+" "+field.mirror_type
		import_string += field.import_script
		target_source_string += "TARGET."+field.mirror_name+" = SOURCE."+field.mirror_name
		insert_string += field.mirror_name
		source_string += "SOURCE."+field.mirror_name

		first_pass = False

	#build the string that'll define how the source and target tables should be joined
	first_pass = True
	for join in table_info.import_joins:
		if first_pass == False:
			join_string += ' AND\n'

		join_string += "TARGET."+join+" = SOURCE."+join
		first_pass = False

	#DEFINE THE STRINGS SO THEY HAVE A BEGINNING AND ENDING TO ANY FIELDS REFERENCED
	create_string = "SET NOCOUNT ON;\n\nDECLARE @Temp_Table TABLE(\n"+create_string+"\n)\n"
	import_string = "INSERT INTO @Temp_Table\nSELECT\n"+import_string+"\nFROM\n[dbo].[stg] stg\n"

	#format the message that'll display if the temp value is empty (which it shouldn't be if data was bulk uploaded)
	error_string = "\nDECLARE @table_count FLOAT;\nSET @table_count = (select COUNT(*) from @Temp_Table)"
	error_string += "\nIF @table_count = 0\nBEGIN\nTHROW 50000, 'TEMP TABLE EMPTY', 1;\nEND\n"

	#add an error to check for the target table
	error_string += "\nIF OBJECT_ID(N'"+output_table+"') IS NULL\nBEGIN\nTHROW 50001, 'TARGET TABLE "+output_table+" DOES NOT EXIST', 1;\nEND\n"

	#space after error messages
	error_string += '\n'



	join_string = "MERGE [dbo].["+output_table+"] target\nUsing @Temp_Table source\nON (\n"+join_string+"\n)\n"

	target_source_string = "WHEN MATCHED\nTHEN UPDATE\nSET\n"+target_source_string+"\n"
	insert_string = "WHEN NOT MATCHED BY TARGET\nTHEN INSERT\n(\n"+insert_string+"\n)\n"
	source_string = "VALUES (\n"+source_string+'\n);'

	#COMBINE THE STRINGS TOGETHER INTO A FINAL MERGE SQL STRING AND RETURN IT
	merge_sql = create_string + import_string + error_string
	merge_sql += join_string + target_source_string + insert_string + source_string

	return merge_sql	