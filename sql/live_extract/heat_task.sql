--set language british;
DECLARE 
@start_date DATETIME = CONVERT(DATETIME,'_@start'),
@end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

SELECT
	tsk.[RecId],
	tsk.assignmentid AS number,
	tsk.ParentLink_Category,
	CASE
		WHEN inc.incidentnumber IS NOT NULL THEN inc.incidentnumber
		WHEN prb.problemnumber IS NOT NULL THEN prb.problemnumber
		WHEN chg.changenumber IS NOT NULL THEN chg.changenumber
		WHEN ser.servicereqnumber IS NOT NULL THEN ser.servicereqnumber
		ELSE NULL
	END as parent_number,
	tsk.[OrganisationalUnitName] AS company,
	tsk.[OwnerTeam],
	tsk.[Owner],
	tsk.[Priority],
	tsk.[Status],
	tsk.[ContactName] as customer,
	tsk.[Address] as location,
	tsk.[RequestType],
	LEFT(tsk.[Subject],500) as subject,
	tsk.[TaskCatalog],
	tsk.[TaskCategory],
	tsk.[TaskType],
	tsk.[ResolutionEscLink_RecID],
	tsk.[ResponseEscLink_RecID],
	tsk.[StartDate],
	tsk.[EndDate],
	tsk.[Cost],
	tsk.[TargetDateTime],
	tsk.[PlannedCost],
	tsk.[PlannedEffort],
	tsk.[PlannedStartDate],
	tsk.[PlannedEndDate],
	tsk.[ActualCost],
	tsk.[ActualEffort],
	tsk.[ActualStartDate],
	tsk.[ActualEndDate],
	tsk.[AssignedBy],
	tsk.[AssignedDateTime],
	tsk.[ApprovalDateTime],
	tsk.[AcknowledgedBy],
	tsk.[AcknowledgedDateTime],
	tsk.[CreatedBy],
	tsk.[CreatedDateTime],
	tsk.[ResolvedBy],
	tsk.[ResolvedDateTime],
	tsk.[LastModBy],
	tsk.[LastModDateTime]

FROM 
	[HEATSM].[dbo].[Task] tsk
	LEFT JOIN [HEATSM].dbo.[INCIDENT] inc ON (tsk.[ParentLink_RecID] = inc.RecId)
	LEFT JOIN [HEATSM].dbo.[CHANGE] chg ON (tsk.[ParentLink_RecID] = chg.RecId)
	LEFT JOIN [HEATSM].dbo.[PROBLEM] prb ON (tsk.[ParentLink_RecID] = prb.RecId)
	LEFT JOIN [HEATSM].dbo.[SERVICEREQ] ser ON (tsk.[ParentLink_RecID] = ser.RecId)

WHERE
    tsk.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    tsk.ResolvedDateTime BETWEEN @start_date AND @end_date
    OR
    tsk.LastModDateTime BETWEEN @start_date AND @end_date