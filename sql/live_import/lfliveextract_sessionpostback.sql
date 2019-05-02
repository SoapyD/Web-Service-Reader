--set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE lfliveextract_sessionpostback (*/
DECLARE @Temp_Table TABLE(
	[sessionid] BIGINT,
	[cfield0] NVARCHAR(255),
	[cfield1] NVARCHAR(255),
	[cfield2] NVARCHAR(255),
	[cfield3] NVARCHAR(255),
	[cfield4] NVARCHAR(255),
	[cfield5] NVARCHAR(255),
	[chatlog] NVARCHAR(MAX),
	[techname] NVARCHAR(255),
	[he_session] TINYINT,
	[incidentcreated] TINYINT,
	[notecreated] TINYINT,
	[createdatetime] DATETIME,
	[lastmodifydatetime] DATETIME,
	[fsa_session] TINYINT,
	[mhclg_session] TINYINT
);
INSERT INTO @Temp_Table
SELECT 
	CONVERT(BIGINT,[sessionid]),
	[cfield0],
	[cfield1],
	[cfield2],
	[cfield3],
	CASE WHEN [cfield4] = '' THEN NULL ELSE [cfield4] END AS [cfield4],
	[cfield5],
	[chatlog],
	[techname],
	CONVERT(TINYINT,[he_session]),
	CONVERT(TINYINT,[incidentcreated]),
	CONVERT(TINYINT,[notecreated]),
	CONVERT(DATETIME,[createdatetime]),
	CONVERT(DATETIME,[lastmodifydatetime]),
	CONVERT(TINYINT,[fsa_session]),
	CONVERT(TINYINT,[mhclg_session])
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[lfliveextract_sessionpostback] target
Using @Temp_Table source
ON (
TARGET.[sessionid] = SOURCE.[sessionid]
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.[sessionid] = SOURCE.[sessionid],
TARGET.[cfield0] = SOURCE.[cfield0],
TARGET.[cfield1] = SOURCE.[cfield1],
TARGET.[cfield2] = SOURCE.[cfield2],
TARGET.[cfield3] = SOURCE.[cfield3],
TARGET.[cfield4] = SOURCE.[cfield4],
TARGET.[cfield5] = SOURCE.[cfield5],
TARGET.[chatlog] = SOURCE.[chatlog],
TARGET.[techname] = SOURCE.[techname],
TARGET.[he_session] = SOURCE.[he_session],
TARGET.[incidentcreated] = SOURCE.[incidentcreated],
TARGET.[notecreated] = SOURCE.[notecreated],
TARGET.[createdatetime] = SOURCE.[createdatetime],
TARGET.[lastmodifydatetime] = SOURCE.[lastmodifydatetime],
TARGET.[fsa_session] = SOURCE.[fsa_session],
TARGET.[mhclg_session] = SOURCE.[mhclg_session]
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
[sessionid],
[cfield0],
[cfield1],
[cfield2],
[cfield3],
[cfield4],
[cfield5],
[chatlog],
[techname],
[he_session],
[incidentcreated],
[notecreated],
[createdatetime],
[lastmodifydatetime],
[fsa_session],
[mhclg_session]
)
VALUES (
SOURCE.[sessionid],
SOURCE.[cfield0],
SOURCE.[cfield1],
SOURCE.[cfield2],
SOURCE.[cfield3],
SOURCE.[cfield4],
SOURCE.[cfield5],
SOURCE.[chatlog],
SOURCE.[techname],
SOURCE.[he_session],
SOURCE.[incidentcreated],
SOURCE.[notecreated],
SOURCE.[createdatetime],
SOURCE.[lastmodifydatetime],
SOURCE.[fsa_session],
SOURCE.[mhclg_session]
);
