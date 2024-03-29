set language british; /*DATE CONVERSIONS WONT WORK WITHOUT THIS*/

/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE croydon_sc_cat_item (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY,
	category NVARCHAR(100),
	createddatetime DATETIME,
	lastmoddatetime DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	LEFT(category,100),
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
MERGE [dbo].[croydon_sc_cat_item] target
Using @Temp_Table source
ON (
	target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
target.sys_id = source.sys_id,
target.category = source.category,
target.createddatetime = source.createddatetime,
target.lastmoddatetime = source.lastmoddatetime
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
category,
createddatetime,
lastmoddatetime
)
VALUES (
source.sys_id,
source.category,
source.createddatetime,
source.lastmoddatetime
);