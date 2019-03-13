def return_field_list(tablename):
	fields = ''
	filter_fields = ''

	##########################################################################
	##########################################################################
	################################INCIDENTS
	##########################################################################
	##########################################################################

	if tablename == 'incident':
		fields = [
		"number",
		"company",
		"short_description",
		"priority",
		"state",
		"contact_type",
		"category",
		"subcategory",
		"u_first_fix",
		"assignment_group",
		"assigned_to",
		"close_code",
		"sys_created_by",
		"sys_created_on",
		"resolved_by",
		"resolved_at",
		"closed_by",
		"closed_at",
		"sys_updated_by",
		"sys_updated_on",
		"caller_id",
		"location",
		"active",
		"u_psupplier",
		"u_3rd_party_reference",
		"u_resolving_team",
		"u_fix_code",
		"u_exception_y_n",
		"u_exception_reason",
		"u_lf_comments",
		"u_agreed",
		"u_he_comments",
		"parent_incident",
		"company",
		"u_fa",
		"u_ft"
		]

		filter_fields = ['closed_at','sys_updated_on']

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

		filter_fields = ['closed_at','sys_updated_on']

	##########################################################################
	##########################################################################
	################################INCIDENTS
	##########################################################################
	##########################################################################

	if tablename == 'task_sla':

	    #SELECT THE FIELDS WE WANT TO SEE FROM INCIDENTS
	    fields += "sys_id"+"%2C"
	    fields += "task"+"%2C"
	    fields += "sla"+"%2C"
	    fields += "stage"+"%2C"
	    fields += "start_time"+"%2C"
	    fields += "planned_end_time"+"%2C"
	    fields += "end_time"+"%2C"
	    fields += "duration"+"%2C"
	    fields += "percentage"+"%2C"
	    fields += "time_left"+"%2C"
	    fields += "business_duration"+"%2C"
	    fields += "business_percentage"+"%2C"
	    fields += "business_time_left"+"%2C"
	    fields += "has_breached"+"%2C"
	    fields += "active"+"%2C"
	    fields += "sys_created_by"+"%2C"
	    fields += "sys_created_on"+"%2C"
	    fields += "sys_updated_by"+"%2C"
	    fields += "sys_updated_on"+"%2C"

	    filter_fields = ['sys_created_on','sys_updated_on']


	return_info = [fields, filter_fields]

	return return_info