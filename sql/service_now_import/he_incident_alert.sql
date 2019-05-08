set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE he_incident_alert (*/
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

	lessonslearned NVARCHAR(250),
	resolutionnotes NVARCHAR(250),
	summary NVARCHAR(250),
	numberofstaffaffected NVARCHAR(250),
	rootcause NVARCHAR(250),

	createdby NVARCHAR(100),
	createddatetime DATETIME,
	resolvedby NVARCHAR(100),
	resolveddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,

	incident NVARCHAR(20),
	incident_id CHAR(32)
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
	ISNULL(u_priority_value, NULL) AS priority,
	LEFT(state,50),
	LEFT(assignment_group,100) AS ownerteam,
	LEFT(assigned_to,100) AS owner,

	LEFT(lessons_learned,250),
	LEFT(resolution_notes,250),
	LEFT(summary,250),
	LEFT(u_number_of_staff_affected,250),
	LEFT(u_root_cause,250),

	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(resolved_by,100) AS resolvedby,
	CASE WHEN ISNULL([resolved_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[resolved_at]) END as resolveddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,

	source_incident,
	source_incident_value
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[he_incident_alert] target
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
TARGET.lessonslearned = SOURCE.lessonslearned,
TARGET.resolutionnotes = SOURCE.resolutionnotes,
TARGET.summary = SOURCE.summary,
TARGET.numberofstaffaffected = SOURCE.numberofstaffaffected,
TARGET.rootcause = SOURCE.rootcause,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.resolvedby = SOURCE.resolvedby,
TARGET.resolveddatetime = SOURCE.resolveddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.lastmoddatetime = SOURCE.lastmoddatetime,
TARGET.incident = SOURCE.incident,
TARGET.incident_id = SOURCE.incident_id
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
lessonslearned,
resolutionnotes,
summary,
numberofstaffaffected,
rootcause,
createdby,
createddatetime,
resolvedby,
resolveddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
incident,
incident_id
)
VALUES (
source.sys_id,	
source.number,
source.system,
source.company,
source.subject,
source.priority,
source.status,
source.ownerteam,
source.owner,
source.lessonslearned,
source.resolutionnotes,
source.summary,
source.numberofstaffaffected,
source.rootcause,
source.createdby,
source.createddatetime,
source.resolvedby,
source.resolveddatetime,
source.closedby,
source.closeddatetime,
source.lastmodby,
source.lastmoddatetime,
source.incident,
source.incident_id
);
