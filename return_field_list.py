def return_field_list(tablename):
	fields = ''
	filter_fields = ''

	##########################################################################
	##########################################################################
	################################SYS USER
	##########################################################################
	##########################################################################

	if tablename == 'sys_user':
		fields = [
		"sys_id",
		"name",
		"email",
		'vip',
		"sys_created_on",
		"sys_updated_on"
		]

		filter_fields = ['sys_created_on','sys_updated_on']

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
		"u_first_fix", #HE
		"u_first_time_fix_nonsla", #FSA
		"u_first_time_fix", #MHCLG
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
		"u_ft",
		"u_project_related",
		"reassignment_count"
		]

		filter_fields = ['closed_at','sys_updated_on']

	##########################################################################
	##########################################################################
	################################SC REQ ITEM
	##########################################################################
	##########################################################################

	if tablename == 'sc_req_item':

		fields = [
		"sys_id",
		"number",
		"short_description",
		"state",
		"stage",
		"contact_type",
		"cat_item",
		"assignment_group",
		"assigned_to",
		"approval",
		"due_date",
		"sys_created_by",
		"sys_created_on",
		"closed_by",
		"closed_at",
		"sys_updated_by",
		"sys_updated_on",
		"active",
		"request",
		"company"
		]

		filter_fields = ['closed_at','sys_updated_on']

	##########################################################################
	##########################################################################
	################################TASK SLA
	##########################################################################
	##########################################################################

	if tablename == 'task_sla':

	    #SELECT THE FIELDS WE WANT TO SEE FROM INCIDENTS
	    fields = [
	    "sys_id",
	    "task",
	    "sla",
	    "stage",
	    "start_time",
	    "planned_end_time",
	    "end_time",
	    "duration",
	    "percentage",
	    "time_left",
	    "business_duration",
	    "business_percentage",
	    "business_time_left",
	    "has_breached",
	    "active",
	    "sys_created_by",
	    "sys_created_on",
	    "sys_updated_by",
	    "sys_updated_on"
	   	]

	    filter_fields = ['sys_created_on','sys_updated_on']


	return_info = [fields, filter_fields]

	return return_info