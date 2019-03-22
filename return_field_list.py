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
		"sys_id",
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
	################################INCIDENT TASK
	##########################################################################
	##########################################################################

	if tablename == 'incident_task':
		fields = [
			'sys_id',
		    "number",
		    "short_description",
		    "description",
		    "priority",
		    "state",
		    "assignment_group",
		    "assigned_to",
		    "location",
		    "business_duration",
		    "u_psupplier",
		    "u_pref",
		    "incident",
		    "sys_created_by",
		    "sys_created_on",
		    "closed_by",
		    "closed_at",
		    "sys_updated_by",
		    "sys_updated_on",
		    "company"
		]

		filter_fields = ['closed_at','sys_updated_on']

	##########################################################################
	##########################################################################
	################################INCIDENT ALERTS (HE SPECIFIC TABLE)
	##########################################################################
	##########################################################################

	if tablename == 'incident_alert':
		fields = [
			'sys_id',
			"number",
			"u_priority",
			"state",
			"short_description",
			"assignment_group",
			"assigned_to",
			"resolution_notes",
			"summary",
			"u_number_of_staff_affected",
			"u_root_cause",
			"lessons_learned",
		    "sys_created_by",
			"sys_created_on",
			"resolved_by",
			"resolved_at",
		    "closed_by",
			"closed_at",
			"sys_updated_by",
		    "sys_updated_on",
			"source_incident"
		]

		filter_fields = ['closed_at','sys_updated_on']


	##########################################################################
	##########################################################################
	################################REQUEST
	##########################################################################
	##########################################################################

	if tablename == 'sc_request':

		fields = [
		"sys_id",
		"number",
		"request_state",
		"stage",
		"contact_type",
		"sys_created_by",
		"sys_created_on",
		"closed_by",
		"closed_at",
		"due_date",
		"requested_for",
		"sys_updated_by", #LastModBy
		"sys_updated_on", #LastModDateTime
		"active",
		"company"
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
	################################SC_TASK
	##########################################################################
	##########################################################################

	if tablename == 'sc_task':

	    #SELECT THE FIELDS WE WANT TO SEE FROM INCIDENTS
	    fields = [
		"sys_id",
	    "number",
	    "company",
	    "short_description",
	    "priority",
	    "state",
	    "assignment_group",
	    "assigned_to",
		"sys_created_by",
	    "sys_created_on",
	    "closed_by",
	    "closed_at",
	    "sys_updated_by", #LastModBy
	    "sys_updated_on", #LastModDateTime
		"active",
	    "request",
	    "request_item"
	   	]

	    filter_fields = ['sys_created_on','sys_updated_on']


	##########################################################################
	##########################################################################
	################################CHANGE REQUEST
	##########################################################################
	##########################################################################

	if tablename == 'change_request':
		fields = [
	    	"sys_id",
			"number",
			"company",
			"short_description",
			"priority",
			"state",
			"contact_type",
			"category",
			"u_subcategory",
			"approval",
			"assignment_group",
			"assigned_to",
			"requested_by",
			"location",
			"due_date",
		    "sys_created_by",
			"sys_created_on",
		    "closed_by",
			"closed_at",
			"sys_updated_by",
		    "sys_updated_on",
		    "active"
		]

		filter_fields = ['closed_at','sys_updated_on']


	##########################################################################
	##########################################################################
	################################CHANGE TASK
	##########################################################################
	##########################################################################

	if tablename == 'change_task':
		fields = [
	    	"sys_id",
			"number",
			"parent",
			"change_request",
			"priority",
			"state",
			"short_description",
			"assigned_to",
			"assignment_group",
			"due_date",
		    "sys_created_by",
			"sys_created_on",
		    "closed_by",
			"closed_at",
			"sys_updated_by",
		    "sys_updated_on",
		    "active"
		]


		filter_fields = ['closed_at','sys_updated_on']



	##########################################################################
	##########################################################################
	################################PROBLEM
	##########################################################################
	##########################################################################

	if tablename == 'problem':

	    #SELECT THE FIELDS WE WANT TO SEE FROM INCIDENTS
	    fields = [
	    "sys_id",
	    "number",
	    "company",
	    "location",
	    "short_description",
	    "priority",
	    "state",
	    "contact_type",
	    "u_category",
	    "u_subcategory",
	    "assignment_group",
	    "assigned_to",
	    #ASSIGNED TO EMAIL
	    "u_date_problem_closed", #DATE PROBLEM RESOLVED
	    "sys_created_by",
	    "sys_created_on",
	    "closed_by",
	    "closed_at",
	    "sys_updated_by", #LastModBy
	    "sys_updated_on", #LastModDateTime
	    "active",
	    "work_notes",
	    "u_exception_y_n",
	    "u_exception_reason", #CODE LOOKUP
	    "u_lf_comments",
	    "u_exception_agreed",
	    "u_he_comments",
	    "u_problem_source"
	    ]

	    filter_fields = ['closed_at','sys_updated_on']

	##########################################################################
	##########################################################################
	################################PROBLEM TASK
	##########################################################################
	##########################################################################

	if tablename == 'problem_task':

	    #SELECT THE FIELDS WE WANT TO SEE FROM INCIDENTS
	    fields = [
	    "sys_id",
	    "number",
	    "short_description",
	    "priority",
	    "state",
	    "assignment_group",
	    "assigned_to",
	    #ASSIGNED TO EMAIL
	    "sys_created_by",
	    "sys_created_on",
	    "closed_by",
	    "closed_at", 
	    "sys_updated_by", #LastModBy
	    "sys_updated_on", #LastModDateTime
	    "active",
	    "problem"
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