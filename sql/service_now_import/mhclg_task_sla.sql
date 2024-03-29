set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE mhclg_task_sla (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY, 
	system NVARCHAR(100),
	company NVARCHAR(100),
	[sladefinition] NVARCHAR(50),
	sladefinition_id CHAR(32),
	stage NVARCHAR(50),
	[starttime] DATETIME,
	[breachtime] DATETIME,
	[stoptime] DATETIME,
	[duration] INT,
	[percentage] FLOAT,
	[timeleft] INT,
	[businessduration] INT,
	[businesspercentage] FLOAT,
	[businesstimeleft] INT,
	[hasbreached] NVARCHAR(50),
	[active] NVARCHAR(10),
	[createdby] NVARCHAR(100),
	[createddatetime] DATETIME,
	[lastmodby] NVARCHAR(100),
	[lastmoddatetime] DATETIME,
	[task] NVARCHAR(150),
	task_id CHAR(32)
	--[tasktype] NVARCHAR(50)
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	'mhclg' AS system,
	'MHCLG' AS company,
	LEFT(sla,50) AS [sladefinition],
	sla_value AS sladefinition_id,
	LEFT(stage,50),
	CASE WHEN [start_time] = '' THEN NULL ELSE CONVERT(DATETIME,[start_time]) END as starttime,
	CASE WHEN [planned_end_time] = '' THEN NULL ELSE CONVERT(DATETIME,[planned_end_time]) END as breachtime,
	CASE WHEN [end_time] = '' THEN NULL ELSE CONVERT(DATETIME,[end_time]) END as stoptime,
	CASE 
		WHEN duration_value = '' THEN ''
		WHEN ISDATE(CONVERT(varchar,CONVERT(DATE,duration_value),103)) = 1 THEN 
		DATEDIFF(minute,convert(date,'1970-01-01'),duration_value) * 60 
		ELSE NULL 
	END as duration,
	CONVERT(FLOAT,REPLACE([percentage], ',', '')) AS percentage,
	CASE 
		WHEN time_left_value = '' THEN ''
		WHEN ISDATE(CONVERT(varchar,CONVERT(DATE,time_left_value),103)) = 1 THEN 
		DATEDIFF(minute,convert(date,'1970-01-01'),time_left_value) * 60 
		ELSE NULL 
	END as time_left,	
	CASE 
		WHEN business_duration_value = '' THEN ''
		WHEN ISDATE(CONVERT(varchar,CONVERT(DATE,business_duration_value),103)) = 1 THEN 
		DATEDIFF(minute,convert(date,'1970-01-01'),business_duration_value) * 60 
		ELSE NULL 
	END as business_duration,
	CONVERT(FLOAT,REPLACE([business_percentage], ',', '')) AS business_percentage,
	CASE 
		WHEN business_time_left_value = '' THEN ''
		WHEN ISDATE(CONVERT(varchar,CONVERT(DATE,business_time_left_value),103)) = 1 THEN 
		DATEDIFF(minute,convert(date,'1970-01-01'),business_time_left_value) * 60 
		ELSE NULL 
	END as business_time_left,	
	[has_breached],
	active,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(sys_updated_by,100) AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as LastModDateTime,
	task,
	task_value AS task_id
FROM 
	[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[mhclg_task_sla] target
Using @Temp_Table source
ON (
target.[sys_id] = source.[sys_id]
)
WHEN MATCHED
THEN UPDATE
SET
TARGET.sys_id  = SOURCE.sys_id ,
TARGET.system = SOURCE.system,
TARGET.company = SOURCE.company,
TARGET.[sladefinition] = SOURCE.[sladefinition],
TARGET.sladefinition_id = SOURCE.sladefinition_id,
TARGET.stage = SOURCE.stage,
TARGET.[starttime] = SOURCE.[starttime],
TARGET.[breachtime] = SOURCE.[breachtime],
TARGET.[stoptime] = SOURCE.[stoptime],
TARGET.[duration] = SOURCE.[duration],
TARGET.[percentage] = SOURCE.[percentage],
TARGET.[timeleft] = SOURCE.[timeleft],
TARGET.[businessduration] = SOURCE.[businessduration],
TARGET.[businesspercentage] = SOURCE.[businesspercentage],
TARGET.[businesstimeleft] = SOURCE.[businesstimeleft],
TARGET.[hasbreached] = SOURCE.[hasbreached],
TARGET.[active] = SOURCE.[active],
TARGET.[createdby] = SOURCE.[createdby],
TARGET.[createddatetime] = SOURCE.[createddatetime],
TARGET.[lastmodby] = SOURCE.[lastmodby],
TARGET.[lastmoddatetime] = SOURCE.[lastmoddatetime],
TARGET.[task] = SOURCE.[task],
TARGET.task_id = SOURCE.task_id
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
system,
company,
[sladefinition],
sladefinition_id,
stage,
[starttime],
[breachtime],
[stoptime],
[duration],
[percentage],
[timeleft],
[businessduration],
[businesspercentage],
[businesstimeleft],
[hasbreached],
[active],
[createdby],
[createddatetime],
[lastmodby],
[lastmoddatetime],
[task],
task_id
)
VALUES (
SOURCE.sys_id, 
SOURCE.system,
SOURCE.company,
SOURCE.[sladefinition],
SOURCE.sladefinition_id,
SOURCE.stage,
SOURCE.[starttime],
SOURCE.[breachtime],
SOURCE.[stoptime],
SOURCE.[duration],
SOURCE.[percentage],
SOURCE.[timeleft],
SOURCE.[businessduration],
SOURCE.[businesspercentage],
SOURCE.[businesstimeleft],
SOURCE.[hasbreached],
SOURCE.[active],
SOURCE.[createdby],
SOURCE.[createddatetime],
SOURCE.[lastmodby],
SOURCE.[lastmoddatetime],
SOURCE.[task],
SOURCE.task_id
);
