--set language british;
DECLARE @start_date DATETIME = CONVERT(DATETIME,'_@start')
DECLARE @end_date DATETIME = CONVERT(DATETIME,'_@end'),
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

ORDER BY_ recid

OFFSET @offset ROWS

FETCH NEXT @max_rows ROWS ONLY