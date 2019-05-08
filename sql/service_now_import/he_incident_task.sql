set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE he_incident_task (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,
	number NVARCHAR(150),
	system NVARCHAR(100),
	--businessunit NVARCHAR(200),
	shortdescription NVARCHAR(250),
	description NVARCHAR(250),
	priority NVARCHAR(60),
	status NVARCHAR(40),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	location NVARCHAR(100),
	businessduration FLOAT,
	thirdpartyname NVARCHAR(100),
	thirdpartyreference NVARCHAR(200),
	incident NVARCHAR(20),
	incident_id CHAR(32),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	number,
	'he' as system,
	--businessunit,
	LEFT(short_description,250) as shortdescription,
	LEFT(description,250) as description,
	priority_value,
	LEFT(state,40), 
	LEFT(assignment_group,100),
	LEFT(assigned_to,100),
	LEFT(location,100),
	CASE WHEN ISNULL([business_duration_value],'') = '' THEN NULL ELSE DATEDIFF(second,convert(date,'1970-01-01'),business_duration_value) END as business_duration,
	LEFT(u_psupplier,100) AS thirdpartyname,
	LEFT(u_pref,200) AS thirdpartyreference,
	incident,
	incident_value,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[he_incident_task] target
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
--target.businessunit = source.businessunit,
target.shortdescription = source.shortdescription,
target.description = source.description,
target.priority = source.priority,
target.status = source.status,
target.ownerteam = source.ownerteam,
target.owner = source.owner,
target.location = source.location,
target.businessduration = source.businessduration,
target.thirdpartyname = source.thirdpartyname,
target.thirdpartyreference = source.thirdpartyreference,
target.incident = source.incident,
target.incident_id = source.incident_id,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.lastmodby = source.lastmodby,
target.lastmoddatetime = source.lastmoddatetime
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
--businessunit,
shortdescription,
description,
priority,
status,
ownerteam,
owner,
location,
businessduration,
thirdpartyname,
thirdpartyreference,
incident,
incident_id,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime
)
VALUES (
source.sys_id,
source.number,
source.system,
--source.businessunit,
source.shortdescription,
source.description,
source.priority,
source.status,
source.ownerteam,
source.owner,
source.location,
source.businessduration,
source.thirdpartyname,
source.thirdpartyreference,
source.incident,
source.incident_id,
source.createdby,
source.createddatetime,
source.closedby,
source.closeddatetime,
source.lastmodby,
source.lastmoddatetime
);
