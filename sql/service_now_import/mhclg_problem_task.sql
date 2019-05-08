set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE mhclg_problem_task (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,
	number NVARCHAR(100),
	system NVARCHAR(100),
	--businessunit NVARCHAR(200),
	subject NVARCHAR(500),
	--isvip NVARCHAR(10),
	priority INT,
	status NVARCHAR(40),
	--source NVARCHAR(50),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	--owneremail NVARCHAR(100),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	active NVARCHAR(20),
	problemnumber NVARCHAR(30),
	problem_id CHAR(32)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'mhclg' AS system,
	--businessunit,
	LEFT(short_description,500) AS subject,
	--isvip,
	CONVERT(INT,priority_value) AS priority,
	state AS status,
	--source,
	assignment_group AS ownerteam,
	assigned_to AS owner,
	--owneremail,
	sys_created_by AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	closed_by AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	sys_updated_by AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	active,
	problem AS problemnumber,
	problem_value AS problem_id
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[mhclg_problem_task] target
Using @Temp_Table source
ON (
target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.[sys_id] = SOURCE.[sys_id],
TARGET.[number] = SOURCE.[number],
TARGET.system = SOURCE.system,
--TARGET.businessunit = SOURCE.businessunit,
TARGET.subject = SOURCE.subject,
--TARGET.isvip = SOURCE.isvip,
TARGET.priority = SOURCE.priority,
TARGET.status = SOURCE.status,
--TARGET.source = SOURCE.source,
TARGET.ownerteam = SOURCE.ownerteam,
TARGET.owner = SOURCE.owner,
--TARGET.owneremail = SOURCE.owneremail,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.LastModDateTime = SOURCE.LastModDateTime,
TARGET.active = SOURCE.active,
TARGET.problemnumber = SOURCE.problemnumber,
TARGET.problem_id = SOURCE.problem_id
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
[number],
system,
--businessunit,
subject,
--isvip,
priority,
status,
--source,
ownerteam,
owner,
--owneremail,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
LastModDateTime,
active,
problemnumber,
problem_id
)
VALUES (
SOURCE.[sys_id],
SOURCE.[number],
SOURCE.system,
--SOURCE.businessunit,
SOURCE.subject,
--SOURCE.isvip,
SOURCE.priority,
SOURCE.status,
--SOURCE.source,
SOURCE.ownerteam,
SOURCE.owner,
--SOURCE.owneremail,
SOURCE.createdby,
SOURCE.createddatetime,
SOURCE.closedby,
SOURCE.closeddatetime,
SOURCE.lastmodby,
SOURCE.LastModDateTime,
SOURCE.active,
SOURCE.problemnumber,
SOURCE.problem_id
);
