--set language british;
DECLARE 
@start_date DATETIME = CONVERT(DATETIME,'_@start'),
@end_date DATETIME = CONVERT(DATETIME,'_@end')
/*@start_date DATETIME = CONVERT(DATETIME,'01/01/2019'),
@end_date DATETIME = CONVERT(DATETIME,'02/01/2019')*/
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT
    inc.RecID,
    inc.IncidentNumber,
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
    res_esc.L1DateTime,
    res_esc.L1Passed,
    res_esc.L2DateTime,
    res_esc.L2Passed, 
    res_esc.L3DateTime,
    res_esc.L3Passed,
    res_esc.BreachDateTime,
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
    



