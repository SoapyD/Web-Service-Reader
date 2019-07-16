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
    prb.RecID,
    CONVERT(INT,prb.ProblemNumber) AS ProblemNumber,
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

ORDER BY_ ProblemNumber

OFFSET @offset ROWS

FETCH NEXT @max_rows ROWS ONLY