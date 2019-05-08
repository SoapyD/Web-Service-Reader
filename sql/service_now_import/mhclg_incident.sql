set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE mhclg_incident (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY, 
	number NVARCHAR(100),
	system NVARCHAR(100),
	company NVARCHAR(150),
	businessunit NVARCHAR(200),
	location NVARCHAR(100),
	--customer NVARCHAR(100),
	customer_id CHAR(32),
	subject NVARCHAR(255),
	priority NVARCHAR(10), 
	status NVARCHAR(40), 
	source NVARCHAR(40),
	category NVARCHAR(60),
	subcategory NVARCHAR(60),
	fcr NVARCHAR(10),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	resolvedby NVARCHAR(100),
	resolveddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	typeofincident NVARCHAR(20),
	active NVARCHAR(20),

	parentincident_id CHAR(32),
	changerequest_id CHAR(32),
	problem_id CHAR(32),
	reassignmentcount INT
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	number,
	'mhclg' AS system,
	'MHCLG' AS company,
	company AS businessunit,
	location,
	--caller_id AS customer,
	caller_id_value AS customer_id,
	short_description AS subject,
	priority_value AS priority,
	state AS status,
	contact_type AS source,
	category,
	subcategory,
	CASE WHEN LOWER([u_first_time_fix]) = 'true' THEN 1 ELSE 0 END as fcr,
	assignment_group AS ownerteam,
	assigned_to AS owner,
	sys_created_by AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	resolved_by AS resolvedby,
	CASE WHEN ISNULL([resolved_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[resolved_at]) END as resolveddatetime,
	closed_by AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	sys_updated_by AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as lastmoddatetime,
	'Failure',
	active,

	CASE WHEN parent_incident_value = '' THEN NULL ELSE parent_incident_value END as parentincident_id,
	CASE WHEN rfc_value = '' THEN NULL ELSE rfc_value END as changerequest_id,
	CASE WHEN problem_id_value = '' THEN NULL ELSE problem_id_value END as problem_id,
	reassignment_count AS reassignmentcount
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[mhclg_incident] target
Using @Temp_Table source
ON (
target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
target.sys_id = source.sys_id,
target.number = source.number,
target.system = source.system,
target.company = source.company,
target.businessunit = source.businessunit,
target.location = source.location,
--target.customer = source.customer,
target.customer_id = source.customer_id,
target.subject = source.subject,
target.priority  = source.priority ,
target.status  = source.status ,
target.source = source.source,
target.category = source.category,
target.subcategory = source.subcategory,
target.fcr = source.fcr,
target.ownerteam = source.ownerteam,
target.owner = source.owner,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.resolvedby = source.resolvedby,
target.resolveddatetime = source.resolveddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.lastmodby = source.lastmodby,
target.lastmoddatetime = source.lastmoddatetime,
target.typeofincident = source.typeofincident,
target.active = source.active,

target.parentincident_id = source.parentincident_id,
target.changerequest_id = source.changerequest_id,
target.problem_id = source.problem_id,
target.reassignmentcount = source.reassignmentcount
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
company,
businessunit,
location,
--customer,
customer_id,
subject,
priority, 
status, 
source,
category,
subcategory,
fcr,
ownerteam,
owner,
createdby,
createddatetime,
resolvedby,
resolveddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
typeofincident,
active,

parentincident_id,
changerequest_id,
problem_id,
reassignmentcount
)
VALUES (
source.sys_id,
source.number,
source.system,
source.company,
source.businessunit,
source.location,
--source.customer,
source.customer_id,
source.subject,
source.priority,
source.status,
source.source,
source.category,
source.subcategory,
source.fcr,
source.ownerteam,
source.owner,
source.createdby,
source.createddatetime,
source.resolvedby,
source.resolveddatetime,
source.closedby,
source.closeddatetime,
source.lastmodby,
source.lastmoddatetime,
source.typeofincident,
source.active,

source.parentincident_id,
source.changerequest_id,
source.problem_id,
source.reassignmentcount
);
