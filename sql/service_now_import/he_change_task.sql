set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE he_change_task (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,	
	number NVARCHAR(20),
	system NVARCHAR(100),
	company NVARCHAR(100),
	--businessunit NVARCHAR(200),
	subject NVARCHAR(255),
	priority INT,
	status NVARCHAR(50),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	duedate DATETIME,
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	active NVARCHAR(20),
	changerequest NVARCHAR(20),
	changerequest_id CHAR(32)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'he' AS system,
	'Historic England' AS company,
	--CASE
	--WHEN CHARINDEX('english-heritage', requestoremail) > 0 THEN 'English Heritage'
	--WHEN CHARINDEX('historicengland', requestoremail) > 0 THEN 'Historic England'
	--ELSE 'Other'
	--END AS businessunit,
	LEFT(short_description,255) AS subject,
	ISNULL(priority_value, NULL) AS priority,
	LEFT(state,50),
	LEFT(assignment_group,100) AS ownerteam,
	LEFT(assigned_to,100) AS owner,
	CASE WHEN ISNULL([due_date],'') = '' THEN NULL ELSE CONVERT(DATETIME,[due_date]) END as duedate,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	active,
	change_request,
	change_request_value
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[he_change_task] target
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
TARGET.subject = SOURCE.subject,
TARGET.priority = SOURCE.priority,
TARGET.status = SOURCE.status,
TARGET.ownerteam = SOURCE.ownerteam,
TARGET.owner = SOURCE.owner,
TARGET.duedate = SOURCE.duedate,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.lastmoddatetime = SOURCE.lastmoddatetime,
TARGET.active = SOURCE.active,
TARGET.changerequest = SOURCE.changerequest,
TARGET.changerequest_id = SOURCE.changerequest_id
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
company,
subject,
priority,
status,
ownerteam,
owner,
duedate,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
active,
changerequest,
changerequest_id
)
VALUES (
SOURCE.sys_id,
SOURCE.number,
SOURCE.system,
SOURCE.company,
SOURCE.subject,
SOURCE.priority,
SOURCE.status,
SOURCE.ownerteam,
SOURCE.owner,
SOURCE.duedate,
SOURCE.createdby,
SOURCE.createddatetime,
SOURCE.closedby,
SOURCE.closeddatetime,
SOURCE.lastmodby,
SOURCE.lastmoddatetime,
SOURCE.active,
SOURCE.changerequest,
SOURCE.changerequest_id
);
