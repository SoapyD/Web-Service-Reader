--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF;

WITH DATA AS (
	SELECT 
	    [id],
	    [surveyid],
	    [rescuesessionid],
	    [incidentnumber],
	    [submittedat],
	    [comments],
	    [nps]/*,
	 	ROW_NUMBER() OVER (PARTITION BY CONVERT(NVARCHAR,ISNULL([rescuesessionid],'')) + CONVERT(NVARCHAR,ISNULL([incidentnumber],'')) + CONVERT(NVARCHAR,[submittedat]) 
		ORDER BY  CONVERT(NVARCHAR,ISNULL([rescuesessionid],'')) + CONVERT(NVARCHAR,ISNULL([incidentnumber],'')) + CONVERT(NVARCHAR,[submittedat])) as duplicates*/
		   
	FROM 
	    [LFLiveExtract].[dbo].[completedsurvey]

	WHERE
	    [submittedat] BETWEEN @start_date AND @end_date
)

SELECT
	[id],
	[surveyid],
	[rescuesessionid],
	[incidentnumber],
	[submittedat],
	[comments],
	[nps] 
FROM 
	DATA 
/*WHERE duplicates = 1*/