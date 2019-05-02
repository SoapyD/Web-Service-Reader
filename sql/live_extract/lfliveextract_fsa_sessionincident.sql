--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT
    [sessionid],
    [incidentrecid],
    [incidentnumber],
    [createdatetime]
FROM 
    [LFLiveExtract].[dbo].[fsa_sessionincident]

WHERE
    [createdatetime] BETWEEN @start_date AND @end_date