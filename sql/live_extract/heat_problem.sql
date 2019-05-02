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
    prb.recid,
    prb.ProblemNumber,
    prb.OrgUnitName,
    prb.Subject,
    prb.Priority,
    prb.Status,
    prb.Source,
    prb.Category,
    prb.Subcategory,
    prb.OwnerTeam,
    prb.Owner,
    prb.CreatedBy,
    prb.CreatedDateTime,
    prb.ClosedBy,
    prb.ClosedDateTime,
    prb.LastModBy,
    prb.LastModDateTime,
    prb.ResolutionEscLink_recid,
    prb.ProfileLink_RecID,
    res_esc.L1DateTime,
    res_esc.L1Passed,
    res_esc.L2DateTime,
    res_esc.L2Passed,
    res_esc.L3DateTime,
    res_esc.L3Passed,
    res_esc.BreachDateTime,
    res_esc.BreachPassed,
    emp.DisplayName,
    emp.EmployeeLocation,
    ISNULL(jou.notes,NULL) AS worknotes

FROM
    [HEATSM].dbo.Problem prb

    LEFT JOIN [HEATSM].dbo.[Employee] emp ON (emp.RECID = prb.ProfileLink_RecID)
    LEFT JOIN [HEATSM].dbo.[Frs_data_escalation_watch] res_esc ON (res_esc.recid = prb.ResolutionEscLink_RecID)

    LEFT JOIN 
    (
        SELECT
            jou.parentlink_recid,
            jou.CreatedDateTime,
            jou.CreatedBy +' - ' + /*CHAR(10)+CHAR(13)+*/
            CONVERT(NVARCHAR,ISNULL(jou.CreatedDateTime,NULL)) +' - ' + /*CHAR(10)+CHAR(13)+*/
            ISNULL(jou.notesbody,NULL) AS notes,
            ROW_NUMBER() OVER (PARTITION BY parentlink_recid ORDER BY CreatedDateTime DESC) AS row_number
        FROM
            [HEATSM].dbo.[JOURNAL] jou
        WHERE
            journaltype = 'Notes'
            and parentlink_recid is not null
            and CreatedBy <> 'WebservicesUser'
            and Category = 'Memo'
    ) as jou ON (jou.parentlink_recid = prb.recid)

WHERE
    ISNULL(jou.ROW_NUMBER,-1) IN (-1,1)
    AND
    (
        prb.ClosedDateTime BETWEEN @start_date AND @end_date
        OR
        prb.LastModDateTime BETWEEN @start_date AND @end_date
        OR
        res_esc.CreatedDateTime BETWEEN @start_date AND @end_date
        OR
        res_esc.LastModDateTime BETWEEN @start_date AND @end_date
        OR
        ISNULL(jou.CreatedDateTime,NULL) BETWEEN @start_date AND @end_date
    )