set language british; /*DATE CONVERSIONS WONT WORK WITHOUT THIS*/

/*

contains business unit which is unique to this table

*/

/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE mhclg_sys_user (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,
	name NVARCHAR(100),
	email NVARCHAR(150),
	vip NVARCHAR(10),
	location NVARCHAR(100),
	businessunit NVARCHAR(200),
	createddatetime DATETIME,
	lastmoddatetime DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	LEFT(name,100),
	LEFT(email,150),
	LEFT(vip,10),
	LEFT(location,100),
	LEFT(businessunit,200),
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
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
MERGE [dbo].[mhclg_sys_user] target
Using @Temp_Table source
ON (
	target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
target.sys_id = source.sys_id,
target.name = source.name,
target.email = source.email,
target.vip = source.vip,
target.location = source.location,
target.businessunit = source.businessunit,
target.createddatetime = source.createddatetime,
target.lastmoddatetime = source.lastmoddatetime
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
name,
email,
vip,
location,
businessunit,
createddatetime,
lastmoddatetime
)
VALUES (
source.sys_id,
source.name,
source.email,
source.vip,
source.location,
source.businessunit,
source.createddatetime,
source.lastmoddatetime
);