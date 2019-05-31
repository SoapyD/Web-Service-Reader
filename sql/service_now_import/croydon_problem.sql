set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE croydon_problem (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY, 
	number NVARCHAR(100),
	system NVARCHAR(100),
	subject NVARCHAR(500),
	priority INT,
	status NVARCHAR(40),
	source NVARCHAR(50),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	active NVARCHAR(20),
	problemsource NVARCHAR(40),
	worknotes NVARCHAR(MAX),

	approval NVARCHAR(20),
	--businessduration FLOAT,
	knownerror NVARCHAR(10),
	urgency NVARCHAR(30),
	workaround NVARCHAR(MAX),
	expectedstart DATETIME,
	workstart DATETIME,
	workend DATETIME,
	duedate DATETIME,
	reassignmentcount INT,
	changerequest_id CHAR(32)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'croydon' AS system,
	LEFT(short_description,500) AS subject,
	CONVERT(INT,priority_value) AS priority,
	LEFT(state,40) AS status,
	LEFT(contact_type,50) AS source,
	LEFT(assignment_group,100) AS ownerteam,
	LEFT(assigned_to,100) AS owner,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	active,
	LEFT(u_problem_source,40) AS problemsource,
	work_notes AS worknotes,

	approval,
	--business_duration,
	LEFT(known_error,10),
	LEFT(urgency,30),
	CASE WHEN ISNULL(work_around,'') = '' THEN NULL ELSE work_around END AS work_around,
	CASE WHEN ISNULL(expected_start,'') = '' THEN NULL ELSE CONVERT(DATETIME,expected_start) END as expected_start,
	CASE WHEN ISNULL(work_start,'') = '' THEN NULL ELSE CONVERT(DATETIME,work_start) END as work_start,
	CASE WHEN ISNULL(work_end,'') = '' THEN NULL ELSE CONVERT(DATETIME,work_end) END as work_end,
	CASE WHEN ISNULL(due_date,'') = '' THEN NULL ELSE CONVERT(DATETIME,due_date) END as due_date,
	reassignment_count,
	CASE WHEN rfc_value = '' THEN NULL ELSE rfc_value END as changerequest_id
FROM 
	[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[croydon_problem] target
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
TARGET.subject = SOURCE.subject,
TARGET.priority = SOURCE.priority,
TARGET.status = SOURCE.status,
TARGET.source = SOURCE.source,
TARGET.ownerteam = SOURCE.ownerteam,
TARGET.owner = SOURCE.owner,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.lastmoddatetime = SOURCE.lastmoddatetime,
TARGET.active = SOURCE.active,
TARGET.problemsource = SOURCE.problemsource,
TARGET.worknotes = SOURCE.worknotes,

TARGET.approval = SOURCE.approval,
--TARGET.businessduration = SOURCE.businessduration,
TARGET.knownerror = SOURCE.knownerror,
TARGET.urgency = SOURCE.urgency,
TARGET.workaround = SOURCE.workaround,
TARGET.expectedstart = SOURCE.expectedstart,
TARGET.workstart = SOURCE.workstart,
TARGET.workend = SOURCE.workend,
TARGET.duedate = SOURCE.duedate,
TARGET.reassignmentcount = SOURCE.reassignmentcount,
TARGET.changerequest_id = SOURCE.changerequest_id
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
subject,
priority,
status,
source,
ownerteam,
owner,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
active,
problemsource,
worknotes,

approval,
--businessduration,
knownerror,
urgency,
workaround,
expectedstart,
workstart,
workend,
duedate,
reassignmentcount,
changerequest_id
)
VALUES (
SOURCE.sys_id,
SOURCE.number,
SOURCE.system,
--SOURCE.businessunit,
SOURCE.subject,
SOURCE.priority,
SOURCE.status,
SOURCE.source,
SOURCE.ownerteam,
SOURCE.owner,
SOURCE.createdby,
SOURCE.createddatetime,
SOURCE.closedby,
SOURCE.closeddatetime,
SOURCE.lastmodby,
SOURCE.lastmoddatetime,
SOURCE.active,
SOURCE.problemsource,
SOURCE.worknotes,

SOURCE.approval,
--SOURCE.businessduration,
SOURCE.knownerror,
SOURCE.urgency,
SOURCE.workaround,
SOURCE.expectedstart,
SOURCE.workstart,
SOURCE.workend,
SOURCE.duedate,
SOURCE.reassignmentcount,
SOURCE.changerequest_id
);
