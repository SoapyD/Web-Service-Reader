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
	chg.RecID,
	CONVERT(INT,chg.ChangeNumber) AS ChangeNumber,
	chg.BusinessUnit,
	chg.OrgUnitName,
	chg.Owner,
	chg.OwnerTeam,
	chg.RequestedBy,
	chg.TypeOfChange,
	chg.RiskLevel,
	chg.Priority,
	chg.Status,
	LEFT(chg.CancellationReason,200) AS CancellationReason,
	chg.Service,
	chg.Category,
	chg.Subcategory,
	LEFT(chg.Subject,500) AS Subject,
	LEFT(chg.Description,500) AS Description,
	LEFT(chg.Justification,500) AS Justification,
	LEFT(chg.Reason,500) AS Reason,
	LEFT(chg.AA_BackOutPlan,500) AS AA_BackOutPlan,
	LEFT(chg.AA_TestPlan,500) AS AA_TestPlan,
	LEFT(chg.AA_WhoTesting,500) AS AA_WhoTesting,
	chg.PrimarySystemName,
	chg.PrimarySystemOwner,
	chg.SecondarySystemOwner,
	chg.SOXChange,
	chg.SOXSystemOwnerEmail,
	chg.Sponsor,

	r.ImplementationResult,
	CASE
		WHEN r.EndDateTime IS NULL THEN 'Not Finished'
		WHEN r.EndDateTime >= chg.ScheduledStartDate AND r.EndDateTime <= chg.ScheduledEndDate THEN 'As Planned'
		WHEN r.EndDateTime <= chg.ScheduledStartDate THEN 'Early'
		ELSE 'Late'
	END AS 'ToSchedule',

	chg.ScheduledStartDate,
	chg.ScheduledEndDate,
	r.StartDateTime,
	r.EndDateTime, 

	chg.CMApprovedBy,
	chg.CMApprovedDateTime,

	chg.CreatedBy,		
	chg.CreatedDateTime,	

	chg.LastModBy,
	chg.LastModDateTime,

	chg.closedby,
	FORMAT(chg.closeddatetime,'yyyy-MM-dd HH:mm:ss') closeddatetime


FROM
	[HEATSM].dbo.[CHANGE] as chg
	LEFT JOIN [HEATSM].dbo.[PIR] r ON r.RecId=chg.PIRLink_RecID

WHERE
    chg.CreatedDateTime BETWEEN @start_date AND @end_date
    OR
    chg.LastModDateTime BETWEEN @start_date AND @end_date


ORDER BY_ ChangeNumber

OFFSET @offset ROWS

FETCH NEXT @max_rows ROWS ONLY
