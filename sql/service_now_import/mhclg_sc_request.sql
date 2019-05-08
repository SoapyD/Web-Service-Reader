set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE mhclg_sc_request (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,	
	number NVARCHAR(100),
	system NVARCHAR(100),
	company NVARCHAR(100),
	--businessunit NVARCHAR(200),
	requeststate NVARCHAR(50),
	stage NVARCHAR(50),
	--source NVARCHAR(50),
	duedate DATETIME,
	requestor NVARCHAR(100),
	requestor_id CHAR(32),
	--requestoremail NVARCHAR(200),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	typeofincident NVARCHAR(20),
	active NVARCHAR(20)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'mhclg' AS system,
	'MHCLG' AS company,
	--CASE
	--WHEN CHARINDEX('english-heritage', requestoremail) > 0 THEN 'English Heritage'
	--WHEN CHARINDEX('historicengland', requestoremail) > 0 THEN 'Historic England'
	--ELSE 'Other'
	--END AS businessunit,
	LEFT(request_state,50) AS requeststate,
	LEFT(stage,50),
	--source,
	due_date AS duedate,
	LEFT(requested_for,100) AS requestor,
	requested_for_value AS requestor_id,
	--requestoremail,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	'Service Request',
	active
FROM 
[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[mhclg_sc_request] target
Using @Temp_Table source
ON (
target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
target.[sys_id] = source.[sys_id],
target.[number] = source.[number],
target.system = source.system,
target.company = source.company,
--target.businessunit = source.businessunit,
target.requeststate = source.requeststate,
target.stage = source.stage,
--target.source = source.source,
target.duedate = source.duedate,
target.requestor = source.requestor,
target.requestor_id = source.requestor_id,
--target.requestoremail = source.requestoremail,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.lastmodby = source.lastmodby,
target.lastmoddatetime = source.lastmoddatetime,
target.typeofincident = source.typeofincident,
target.active = source.active
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
[number],
system,
company,
--businessunit,
requeststate,
stage,
--source,
duedate,
requestor,
requestor_id,
--requestoremail,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
typeofincident,
active
)
VALUES (
source.[sys_id],
source.[number],
source.system,
source.company,
--source.businessunit,
source.requeststate,
source.stage,
--source.source,
source.duedate,
source.requestor,
source.requestor_id,
--source.requestoremail,
source.createdby,
source.createddatetime,
source.closedby,
source.closeddatetime,
source.lastmodby,
source.lastmoddatetime,
source.typeofincident,
source.active
);
