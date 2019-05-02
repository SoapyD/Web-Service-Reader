set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE heat_organizationalunit (*/
DECLARE @Temp_Table TABLE(
	recid NVARCHAR(150) PRIMARY KEY,
	name NVARCHAR(100),
	aliasname NVARCHAR(100),
	parentlinkcategory NVARCHAR(100),
	parentlinkrecid NVARCHAR(150),
	depth INT,
	supportingsite NVARCHAR(100),
	accountmanagername NVARCHAR(100),
	sraccountmanager NVARCHAR(100),
	isdeleted NVARCHAR(20),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	recid,
	name,
	AA_AliasName AS aliasname,
	ParentLink_Category AS parentlinkcategory,
	ParentLink_RecID AS parentlinkrecid,
	CASE WHEN depth = '' THEN NULL ELSE CONVERT(INT,depth) END,
	AA_Supportingsite AS supportingsite,
	AA_AccountManagerName AS accountmanagername,
	SR_AccountManager AS sraccountmanager,
	isdeleted,
	createdby,
	CONVERT(DATETIME,[createddatetime]) as createddatetime,
	lastmodby,
	CASE WHEN ISNULL([LastModDateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[LastModDateTime]) END as LastModDateTime
FROM 
	[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[heat_organizationalunit] target
Using @Temp_Table source
ON (
target.recid = source.recid
)
WHEN MATCHED
THEN UPDATE
SET
target.recid = source.recid,
target.name = source.name,
target.aliasname = source.aliasname,
target.parentlinkcategory = source.parentlinkcategory,
target.parentlinkrecid = source.parentlinkrecid,
target.depth = source.depth,
target.supportingsite = source.supportingsite,
target.accountmanagername = source.accountmanagername,
target.sraccountmanager = source.sraccountmanager,
target.isdeleted = source.isdeleted,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.lastmodby = source.lastmodby,
target.lastmoddatetime = source.lastmoddatetime
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
recid,
name,
aliasname,
parentlinkcategory,
parentlinkrecid,
depth,
supportingsite,
accountmanagername,
sraccountmanager,
isdeleted,
createdby,
createddatetime,
lastmodby,
lastmoddatetime
)
VALUES (
source.recid,
source.name,
source.aliasname,
source.parentlinkcategory,
source.parentlinkrecid,
source.depth,
source.supportingsite,
source.accountmanagername,
source.sraccountmanager,
source.isdeleted,
source.createdby,
source.createddatetime,
source.lastmodby,
source.lastmoddatetime
);
