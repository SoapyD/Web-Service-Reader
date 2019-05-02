--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT
    sr.[id],
    sr.[completedsurveyid],
    sr.[questionid],
    sr.[answerid],
    ISNULL(cs.submittedat,NULL) as submittedat
FROM 
    [LFLiveExtract].[dbo].[completedsurveyresponse] sr
    LEFT JOIN [LFLiveExtract].[dbo].[completedsurvey] cs ON (cs.id = sr.completedsurveyid)

WHERE
    ISNULL(cs.submittedat,NULL) BETWEEN @start_date AND @end_date