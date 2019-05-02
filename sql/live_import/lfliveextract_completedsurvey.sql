--set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE lfliveextract_completedsurvey (*/
DECLARE @Temp_Table TABLE(
	id INT,
	[surveyid] INT,
	[rescuesessionid] BIGINT,
	[incidentnumber] NVARCHAR(20),
	[submittedat] DATETIME,
	[comments] NVARCHAR(MAX),
	[nps] TINYINT
);
INSERT INTO @Temp_Table
SELECT 
	CONVERT(INT,id),
	CONVERT(INT,[surveyid]),
	CASE WHEN rescuesessionid = '' THEN NULL ELSE CONVERT(FLOAT,[rescuesessionid]) END AS rescuesessionid,
	CASE WHEN [incidentnumber] = '' THEN NULL ELSE incidentnumber END AS incidentnumber,
	CASE WHEN [submittedat] = '' THEN NULL ELSE CONVERT(DATETIME,[submittedat]) END as [submittedat],
	[comments],
	CONVERT(TINYINT,[nps])
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[lfliveextract_completedsurvey] target
Using @Temp_Table source
ON (
TARGET.[id] = SOURCE.[id]
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.[id] = SOURCE.[id],
TARGET.[surveyid] = SOURCE.[surveyid],
TARGET.[rescuesessionid] = SOURCE.[rescuesessionid],
TARGET.[incidentnumber] = SOURCE.[incidentnumber],
TARGET.[submittedat] = SOURCE.[submittedat],
TARGET.[comments] = SOURCE.[comments],
TARGET.[nps] = SOURCE.[nps]
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
id,
[surveyid],
[rescuesessionid],
[incidentnumber],
[submittedat],
[comments],
[nps]
)
VALUES (
SOURCE.id,
SOURCE.[surveyid],
SOURCE.[rescuesessionid],
SOURCE.[incidentnumber],
SOURCE.[submittedat],
SOURCE.[comments],
SOURCE.[nps]      
);
