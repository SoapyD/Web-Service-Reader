--set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE lfliveextract_completedsurveyresponse (*/
DECLARE @Temp_Table TABLE(
	[id] INT,
	[completedsurveyid] INT,
	[questionid] INT,
	[answerid] INT,
	[submittedat] DATETIME
);
INSERT INTO @Temp_Table
SELECT 
	CONVERT(INT,id),
	CONVERT(INT,[completedsurveyid]),
	CONVERT(INT,[questionid]),
	CONVERT(INT,[answerid]),
	CONVERT(DATETIME,[submittedat])
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[lfliveextract_completedsurveyresponse] target
Using @Temp_Table source
ON (
TARGET.[id] = SOURCE.[id]
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.[id] = SOURCE.[id],
TARGET.[completedsurveyid] = SOURCE.[completedsurveyid],
TARGET.[questionid] = SOURCE.[questionid],
TARGET.[answerid] = SOURCE.[answerid],
TARGET.[submittedat] = SOURCE.[submittedat]
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
[id],
[completedsurveyid],
[questionid],
[answerid],
[submittedat]
)
VALUES (
SOURCE.id,
SOURCE.[completedsurveyid],
SOURCE.[questionid],
SOURCE.[answerid],
SOURCE.[submittedat]
);
