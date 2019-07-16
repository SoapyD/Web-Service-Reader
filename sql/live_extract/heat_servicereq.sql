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
    ser.RecID,
    convert(INT,ser.ServiceReqNumber) as ServiceReqNumber,
    ser.IsVIP,
    ser.Subject,
    ser.Status,
    ser.Source,
    ser.Service,
    ser.OwnerTeam,
    ser.Owner,
    ser.OwnerEmail,
    ser.CreatedBy,
    ser.CreatedDateTime,
    ser.ResolvedBy,
    ser.ResolvedDateTime,
    ser.ClosedBy,
    ser.ClosedDateTime,
    ser.LastModBy,
    ser.LastModDateTime,

    ser.ResponseEscLink_RecID,
    rep_esc.BreachPassed as response_breached,
    rep_esc.TargetClockDuration AS response_targetclockduration,
    rep_esc.[TotalRunningDuration] AS response_totalrunningduration,

    ser.ResolutionEscLink_RecID,
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
    emp.DisplayName,
    emp.EmployeeLocation,
    emp.OrganizationalUnit,
    emp.BusinessUnit

FROM
    [HEATSM].dbo.[SERVICEREQ] ser
    LEFT JOIN [HEATSM].dbo.[Frs_data_escalation_watch] rep_esc ON (rep_esc.recid = ser.ResolutionEscLink_RecID)
    LEFT JOIN [HEATSM].dbo.[Frs_data_escalation_watch] res_esc ON (res_esc.recid = ser.ResolutionEscLink_RecID)

    LEFT JOIN [HEATSM].dbo.[Employee] emp ON (emp.RECID = ser.ProfileLink_RecID)

WHERE

    ser.ClosedDateTime BETWEEN @start_date AND @end_date
    OR
    ser.LastModDateTime BETWEEN @start_date AND @end_date
    OR
    rep_esc.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    rep_esc.LastModDateTime BETWEEN @start_date AND @end_date
    OR
    res_esc.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    res_esc.LastModDateTime BETWEEN @start_date AND @end_date

ORDER BY_ ServiceReqNumber

OFFSET @offset ROWS

FETCH NEXT @max_rows ROWS ONLY