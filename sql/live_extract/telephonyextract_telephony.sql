--set language british;
DECLARE @start_date DATE = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT 
[agentid],
[groupid],
[datetime],
[ringtime],
[totalduration],
[calltype],
[ddi],
[new_ddi],
[threshold_check],
[agent_name],
[agent_email],
[podname],
[groupname],
[received_date],
[quarter],
[weekday],
[weekday_val],
[received_day],
[received_hour],
[time_segment]
FROM 
[reporting_temp].[dbo].[telephony_view]
WHERE
[datetime] BETWEEN @start_date AND @end_date
AND ddi <> 'DDI FOR 597'