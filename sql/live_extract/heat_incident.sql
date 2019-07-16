--set language british;
DECLARE 
@start_date DATETIME = CONVERT(DATETIME,'_@start'),
@end_date DATETIME = CONVERT(DATETIME,'_@end'),
@offset INT = _@offset,
@max_rows INT = _@max_rows
/*
@start_date DATETIME = CONVERT(DATETIME,'01/01/2019'),
@end_date DATETIME = CONVERT(DATETIME,'02/01/2019'),
@offset FLOAT = 0,
@max_rows FLOAT = 10
*/
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT_
    inc.RecID,
    convert(INT,inc.IncidentNumber) AS IncidentNumber,
    inc.AA_OrgUnitName,
    inc.AA_BusinessUnit,
    inc.IsVIP,
    inc.Subject,
    inc.Priority,
    inc.Status,
    inc.Source,
    inc.Service,
    inc.Category,
    inc.Subcategory,
    inc.FirstCallResolution,
    inc.OwnerTeam,
    inc.Owner,
    inc.OwnerEmail,
    inc.CauseCode,
    inc.ActualService,
    inc.ActualCategory,
    inc.ActualSubCategory,
    inc.AA_RemoteResolution,
    inc.CreatedBy,
    inc.CreatedDateTime,
    inc.RespondedBy,
    inc.RespondedDateTime,
    inc.ResolvedBy,
    inc.ResolvedDateTime,
    inc.ClosedBy,
    inc.ClosedDateTime,
    inc.LastModBy,
    inc.LastModDateTime,
    inc.TypeOfIncident,

    inc.ResponseEscLink_RecID,
    rep_esc.BreachPassed as response_breached,
    rep_esc.TargetClockDuration AS response_targetclockduration,
    rep_esc.[TotalRunningDuration] AS response_totalrunningduration,

    inc.ResolutionEscLink_RecID,
    --CONVERT(DATETIME,res_esc.L1DateTime) AS L1DateTime,
    FORMAT(res_esc.L1DateTime,'yyyy-MM-dd HH:mm:ss') AS L1DateTime,
    res_esc.L1Passed,
    --CONVERT(DATETIME,res_esc.L2DateTime) AS L2DateTime,
    FORMAT(res_esc.L2DateTime,'yyyy-MM-dd HH:mm:ss') AS L2DateTime,
    res_esc.L2Passed, 
    --CONVERT(DATETIME,res_esc.L3DateTime) AS L3DateTime,
    FORMAT(res_esc.L3DateTime,'yyyy-MM-dd HH:mm:ss') AS L3DateTime,
    res_esc.L3Passed,
    FORMAT(res_esc.BreachDateTime,'yyyy-MM-dd HH:mm:ss') AS BreachDateTime,
    res_esc.BreachPassed,
    res_esc.TargetClockDuration,
    res_esc.[TotalRunningDuration],

    ProfileLink_RecID,
    emp.DisplayName AS customer,
    emp.EmployeeLocation AS location,
    inc.resolution

FROM
    [HEATSM].dbo.[Incident] inc
    LEFT JOIN [HEATSM].dbo.[Employee] emp ON (emp.RECID = inc.ProfileLink_RecID)
    LEFT JOIN [HEATSM].dbo.[Frs_data_escalation_watch] rep_esc ON (rep_esc.recid = inc.ResponseEscLink_RecID)

    LEFT JOIN [HEATSM].dbo.[Frs_data_escalation_watch] res_esc ON (res_esc.recid = inc.ResolutionEscLink_RecID)


WHERE

    inc.ClosedDateTime BETWEEN @start_date AND @end_date
    OR
    inc.LastModDateTime BETWEEN @start_date AND @end_date
    OR
    rep_esc.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    rep_esc.LastModDateTime BETWEEN @start_date AND @end_date
    OR
    res_esc.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    res_esc.LastModDateTime BETWEEN @start_date AND @end_date
    


ORDER BY_ IncidentNumber

OFFSET @offset ROWS

FETCH NEXT @max_rows ROWS ONLY
