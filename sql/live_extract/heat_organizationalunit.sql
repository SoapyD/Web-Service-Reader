--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end')
;

SET NOCOUNT ON
SET ANSI_WARNINGS OFF


SELECT 
	RecId,
	Name,
	AA_AliasName,
	ParentLink_Category,
	ParentLink_RecID,
	Depth,
	AA_Supportingsite,
	AA_AccountManagerName,
	SR_AccountManager,
	IsDeleted,
	CreatedBy,
	CreatedDateTime,
	LastModBy,
	LastModDateTime

FROM 
	[HEATSM].[dbo].[OrganizationalUnit]

WHERE
	CreatedDateTime BETWEEN @start_date AND @end_date
	OR
	LastModDateTime BETWEEN @start_date AND @end_date