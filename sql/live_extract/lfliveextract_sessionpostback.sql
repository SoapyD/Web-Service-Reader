--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT 
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
FROM 
      [LFLiveExtract].[dbo].[sessionpostback]

WHERE
      [createdatetime] BETWEEN @start_date AND @end_date
      OR
      [lastmodifydatetime] BETWEEN @start_date AND @end_date