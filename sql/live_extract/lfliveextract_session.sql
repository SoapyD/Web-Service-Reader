--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT 
      [id],
      [sessionid],
      [starttime],
      [endtime],
      [lastactiontime],
      [technicianname],
      [technicianid],
      [sessiontype],
      [status],
      [contactname],
      [companyname],
      [email],
      [subject],
      [existingticketno],
      [santander],
      [trackingid],
      [customerip],
      [deviceid],
      [toolsused],
      [channelid],
      [channelname],
      [callingcard],
      [connectingtime],
      [waitingtime],
      [totaltime],
      [activetime],
      [worktime],
      [holdtime],
      [transfertime],
      [rebootingtime],
      [reconnectingtime],
      [platform],
      [technicianemail],
      [techniciangroup],
      [browsertype]
FROM 
      [LFLiveExtract].[dbo].[session]

WHERE
      [starttime] BETWEEN @start_date AND @end_date
      OR
      [endtime] BETWEEN @start_date AND @end_date      
      OR
      [lastactiontime] BETWEEN @start_date AND @end_date      