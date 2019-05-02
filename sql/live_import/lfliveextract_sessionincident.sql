set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE lfliveextract_sessionincident (*/
DECLARE @Temp_Table TABLE(
	[sessionid] BIGINT,
	[incidentrecid] NVARCHAR(MAX),
	[incidentnumber] NVARCHAR(MAX),
	[createdatetime] DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	CONVERT(BIGINT, sessionid),
	incidentrecid,
	CASE WHEN incidentnumber = '' THEN NULL ELSE incidentnumber END AS incidentnumber,
	CONVERT(DATETIME, createdatetime)
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[lfliveextract_sessionincident] target
Using @Temp_Table source
ON (
TARGET.[sessionid] = SOURCE.[sessionid] AND
TARGET.[incidentnumber] = SOURCE.[incidentnumber] AND
TARGET.[createdatetime] = SOURCE.[createdatetime]
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.[sessionid] = SOURCE.[sessionid],
TARGET.[incidentrecid] = SOURCE.[incidentrecid],
TARGET.[incidentnumber] = SOURCE.[incidentnumber],
TARGET.[createdatetime] = SOURCE.[createdatetime]
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
[sessionid],
[incidentrecid],
[incidentnumber],
[createdatetime]
)
VALUES (
SOURCE.[sessionid],
SOURCE.[incidentrecid],
SOURCE.[incidentnumber],
SOURCE.[createdatetime]
);
