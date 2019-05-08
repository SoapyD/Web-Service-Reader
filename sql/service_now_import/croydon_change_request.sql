set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE croydon_change_request (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,	
	number NVARCHAR(20),
	system NVARCHAR(100),
	company NVARCHAR(100),
	location NVARCHAR(100),
	customer NVARCHAR(100),
	customer_id CHAR(32),
	subject NVARCHAR(255),
	priority INT,
	status NVARCHAR(50),
	source NVARCHAR(40),
	category NVARCHAR(60),
	subcategory NVARCHAR(60),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	approval NVARCHAR(30),
	duedate DATETIME,
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	active NVARCHAR(20),

	closecode NVARCHAR(30),
	closenotes NVARCHAR(500),
	type NVARCHAR(40),
	conflictstatus NVARCHAR(40),
	workstart DATETIME,
	workend DATETIME,
	startdate DATETIME,
	enddate DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'croydon' AS system,
	'Croydon' AS company,
	location,
	requested_by,
	requested_by_value,
	short_description AS subject,
	ISNULL(priority_value, NULL) AS priority,
	state,
	contact_type,
	category,
	u_subcategory,
	assignment_group AS ownerteam,
	assigned_to AS owner,
	approval,
	CASE WHEN ISNULL([due_date],'') = '' THEN NULL ELSE CONVERT(DATETIME,[due_date]) END as duedate,
	sys_created_by AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	closed_by AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	sys_updated_by AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	active,

	close_code,
	LEFT(close_notes,500),
	type,
	conflict_status,
	CASE WHEN ISNULL(work_start,'') = '' THEN NULL ELSE CONVERT(DATETIME,work_start) END as workstart,
	CASE WHEN ISNULL(work_end,'') = '' THEN NULL ELSE CONVERT(DATETIME,work_end) END as workend,
	CASE WHEN ISNULL(start_date,'') = '' THEN NULL ELSE CONVERT(DATETIME,start_date) END as startdate,
	CASE WHEN ISNULL(end_date,'') = '' THEN NULL ELSE CONVERT(DATETIME,end_date) END as enddate

FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[croydon_change_request] target
Using @Temp_Table source
ON (
target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.sys_id = SOURCE.sys_id,
TARGET.number = SOURCE.number,
TARGET.system = SOURCE.system,
TARGET.company = SOURCE.company,
TARGET.location = SOURCE.location,
TARGET.customer = SOURCE.customer,
TARGET.customer_id = SOURCE.customer_id,
TARGET.subject = SOURCE.subject,
TARGET.priority = SOURCE.priority,
TARGET.status = SOURCE.status,
TARGET.source = SOURCE.source,
TARGET.category = SOURCE.category,
TARGET.subcategory = SOURCE.subcategory,
TARGET.ownerteam = SOURCE.ownerteam,
TARGET.owner = SOURCE.owner,
TARGET.approval = SOURCE.approval,
TARGET.duedate = SOURCE.duedate,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.lastmoddatetime = SOURCE.lastmoddatetime,
TARGET.active = SOURCE.active,

TARGET.closecode = SOURCE.closecode,
TARGET.closenotes = SOURCE.closenotes,
TARGET.type = SOURCE.type,
TARGET.conflictstatus = SOURCE.conflictstatus,
TARGET.workstart = SOURCE.workstart,
TARGET.workend = SOURCE.workend,
TARGET.startdate = SOURCE.startdate,
TARGET.enddate = SOURCE.enddate
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
company,
location,
customer,
customer_id,
subject,
priority,
status,
source,
category,
subcategory,
ownerteam,
owner,
approval,
duedate,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
active,

closecode,
closenotes,
type,
conflictstatus,
workstart,
workend,
startdate,
enddate
)
VALUES (
SOURCE.sys_id,
SOURCE.number,
SOURCE.system,
SOURCE.company,
SOURCE.location,
SOURCE.customer,
SOURCE.customer_id,
SOURCE.subject,
SOURCE.priority,
SOURCE.status,
SOURCE.source,
SOURCE.category,
SOURCE.subcategory,
SOURCE.ownerteam,
SOURCE.owner,
SOURCE.approval,
SOURCE.duedate,
SOURCE.createdby,
SOURCE.createddatetime,
SOURCE.closedby,
SOURCE.closeddatetime,
SOURCE.lastmodby,
SOURCE.lastmoddatetime,
SOURCE.active,

SOURCE.closecode,
SOURCE.closenotes,
SOURCE.type,
SOURCE.conflictstatus,
SOURCE.workstart,
SOURCE.workend,
SOURCE.startdate,
SOURCE.enddate
);
