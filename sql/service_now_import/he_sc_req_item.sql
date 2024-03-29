set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE he_sc_req_item (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY, 
	number NVARCHAR(150),
	system NVARCHAR(100),
	--businessunit NVARCHAR(200),
	subject NVARCHAR(500),
	--isvip NVARCHAR(10),
	status NVARCHAR(40),
	stage NVARCHAR(50),
	source NVARCHAR(50),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	--owneremail NVARCHAR(100),
	duedatetime DATETIME,
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	--customer NVARCHAR(100),
	--location NVARCHAR(200),
	active NVARCHAR(20),
	approval NVARCHAR(50),
	--category NVARCHAR(200),
	subcategory NVARCHAR(200),
	subcategory_id CHAR(32),
	requestnumber NVARCHAR(30),
	requestnumber_id CHAR(32),
	failedtimeliness NVARCHAR(10)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	[number],
	'he' AS system,
	--businessunit,
	LEFT(short_description,500) AS subject,
	--isvip,
	LEFT(state,40) AS status,
	LEFT(stage,50),
	LEFT(contact_type,50) AS source,
	LEFT(assignment_group,100) AS ownerteam,
	LEFT(assigned_to,100) AS owner,
	--owneremail,
	case WHEN ISNULL([due_date],'') = '' THEN NULL WHEN ISDATE(due_date) = 1 THEN CONVERT(DATETIME,due_date) ELSE NULL END AS duedatetime,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	--caller_id AS customer,
	--location,
	active,
	approval,
	--category,
	LEFT(cat_item,200) AS subcategory,
	cat_item_value AS subcategory_id,
	request AS requestnumber,
	request_value AS requestnumber_id,
	LEFT(u_ft,10) AS failedtimeliness
FROM 
[dbo].[stg] stg;
/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

MERGE [dbo].[he_sc_req_item] target
Using @Temp_Table source
ON (
target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.sys_id = SOURCE.sys_id,
TARGET.[number] = SOURCE.[number],
TARGET.system = SOURCE.system,
--TARGET.businessunit = SOURCE.businessunit,
TARGET.subject = SOURCE.subject,
--TARGET.isvip = SOURCE.isvip,
TARGET.status = SOURCE.status,
TARGET.stage = SOURCE.stage,
TARGET.source = SOURCE.source,
TARGET.ownerteam = SOURCE.ownerteam,
TARGET.owner = SOURCE.owner,
--TARGET.owneremail = SOURCE.owneremail,
TARGET.duedatetime = SOURCE.duedatetime,
TARGET.createdby = SOURCE.createdby,
TARGET.createddatetime = SOURCE.createddatetime,
TARGET.closedby = SOURCE.closedby,
TARGET.closeddatetime = SOURCE.closeddatetime,
TARGET.lastmodby = SOURCE.lastmodby,
TARGET.LastModDateTime = SOURCE.LastModDateTime,
--TARGET.customer = SOURCE.customer,
--TARGET.location = SOURCE.location,
TARGET.active = SOURCE.active,
TARGET.approval = SOURCE.approval,
--TARGET.category = SOURCE.category,
TARGET.subcategory = SOURCE.subcategory,
TARGET.subcategory_id = SOURCE.subcategory_id,
TARGET.requestnumber = SOURCE.requestnumber,
TARGET.requestnumber_id = SOURCE.requestnumber_id,
TARGET.failedtimeliness = SOURCE.failedtimeliness
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
[number],
system,
--businessunit,
subject,
--isvip,
status,
stage,
source,
ownerteam,
owner,
--owneremail,
duedatetime,
createdby,
createddatetime,
closedby,
closeddatetime,
lastmodby,
LastModDateTime,
--customer,
--location,
active,
approval,
--category,
subcategory,
subcategory_id,
requestnumber,
requestnumber_id,
failedtimeliness
)
VALUES (
SOURCE.sys_id,
SOURCE.[number],
SOURCE.system,
--SOURCE.businessunit,
SOURCE.subject,
--SOURCE.isvip,
SOURCE.status,
SOURCE.stage,
SOURCE.source,
SOURCE.ownerteam,
SOURCE.owner,
--SOURCE.owneremail,
SOURCE.duedatetime,
SOURCE.createdby,
SOURCE.createddatetime,
SOURCE.closedby,
SOURCE.closeddatetime,
SOURCE.lastmodby,
SOURCE.LastModDateTime,
--SOURCE.customer,
--SOURCE.location,
SOURCE.active,
SOURCE.approval,
--SOURCE.category,
SOURCE.subcategory,
SOURCE.subcategory_id,
SOURCE.requestnumber,
SOURCE.requestnumber_id,
SOURCE.failedtimeliness
);
