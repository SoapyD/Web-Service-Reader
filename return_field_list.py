def return_field_list(tablename):
	fields = "sysparm_fields="

	##########################################################################
	##########################################################################
	################################INCIDENTS
	##########################################################################
	##########################################################################

	if tablename == 'incident':
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


	##########################################################################
	##########################################################################
	################################INCIDENTS
	##########################################################################
	##########################################################################

	if tablename == 'sc_req_item':

		fields += "number"+ "%2C"
		fields += "short_description"+"%2C"
		fields += "state"+"%2C"
		fields += "stage"+"%2C"
		fields += "contact_type"+"%2C"
		fields += "cat_item"+"%2C"
		fields += "assignment_group"+"%2C"
		fields += "assigned_to"+"%2C"
		fields += "approval"+"%2C"
		fields += "due_date"+"%2C"
		fields += "sys_created_by"+"%2C"
		fields += "sys_created_on"+"%2C"
		fields += "closed_by"+"%2C"
		fields += "closed_at"+"%2C"
		fields += "sys_updated_by"+"%2C"
		fields += "sys_updated_on"+"%2C"
		fields += "active"+"%2C"
		fields += "request"+"%2C"
		fields += "company"+"%2C"


	return fields