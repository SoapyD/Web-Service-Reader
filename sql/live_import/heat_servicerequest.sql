set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE heat_servicerequest (*/
DECLARE @Temp_Table TABLE(
recid NVARCHAR(100) PRIMARY KEY, 
number NVARCHAR(100),
system NVARCHAR(100),
company NVARCHAR(150),
businessunit NVARCHAR(150),
isvip NVARCHAR(40),
subject NVARCHAR(255),
status NVARCHAR(40), 
source NVARCHAR(40),
service NVARCHAR(60),
ownerteam NVARCHAR(100),
owner NVARCHAR(100),
owneremail NVARCHAR(100),
createdby NVARCHAR(100),
createddatetime DATETIME,
resolvedby NVARCHAR(100),
resolveddatetime DATETIME,
closedby NVARCHAR(100),
closeddatetime DATETIME,
lastmodby NVARCHAR(100),
lastmoddatetime DATETIME,
responseesclink NVARCHAR(100),
response_breachpassed NVARCHAR(10),
response_targetclockduration FLOAT,
response_totalrunningduration FLOAT,
resolutionesclink NVARCHAR(100),
breachstatus int,
l1datetime DATETIME,
l1passed NVARCHAR(10),
l2datetime DATETIME,
l2passed NVARCHAR(10),
l3datetime DATETIME,
l3passed NVARCHAR(10),
breachdatetime DATETIME,
breachpassed NVARCHAR(10),
targetclockduration FLOAT,
totalrunningduration FLOAT,
profilelink_recid NVARCHAR(100),
location NVARCHAR(100),
customer NVARCHAR(100)
);
INSERT INTO @Temp_Table
SELECT 
recid,
ServiceReqNumber,
'heat' AS system,
OrganizationalUnit AS company,
businessunit,
isvip,
subject,
status,
source,
service,
ownerteam,
owner,
owneremail,
createdby,
CONVERT(DATETIME,[createddatetime]) as createddatetime,
resolvedby,
CASE WHEN ISNULL([resolveddatetime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[resolveddatetime]) END as resolveddatetime,
closedby,
CASE WHEN ISNULL([closeddatetime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closeddatetime]) END as closeddatetime,
lastmodby,
CASE WHEN ISNULL([LastModDateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[LastModDateTime]) END as LastModDateTime,
ResponseEscLink_RecID,
response_breached,
CASE WHEN ISNULL(response_targetclockduration,'') = '' THEN NULL ELSE CONVERT(FLOAT, response_targetclockduration) END AS response_targetclockduration,
CASE WHEN ISNULL(response_totalrunningduration,'') = '' THEN NULL ELSE CONVERT(FLOAT, response_totalrunningduration) END AS response_totalrunningduration,
ResolutionEscLink_RecID,
NULL as breachstatus,
CASE WHEN ISNULL([L1DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L1DateTime]) END as L1DateTime,
L1Passed,
CASE WHEN ISNULL([L2DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L2DateTime]) END as L2DateTime,
L2Passed,
CASE WHEN ISNULL([L3DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L3DateTime]) END as L3DateTime,
L3Passed,
CASE WHEN ISNULL([BreachDateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[BreachDateTime]) END as BreachTime,
BreachPassed,
CASE WHEN ISNULL(targetclockduration,'') = '' THEN NULL ELSE CONVERT(FLOAT, targetclockduration) END AS targetclockduration,
CASE WHEN ISNULL(totalrunningduration,'') = '' THEN NULL ELSE CONVERT(FLOAT, totalrunningduration) END AS totalrunningduration,
ProfileLink_RecID,
EmployeeLocation AS location,
DisplayName AS customer
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[heat_servicerequest] target
Using @Temp_Table source
ON (
target.recid = source.recid
)
WHEN MATCHED
THEN UPDATE
SET
target.recid = source.recid,
target.number = source.number,
target.system = source.system,
target.company = source.company,
target.businessunit = source.businessunit,
target.isvip = source.isvip,
target.subject = source.subject,
target.status  = source.status ,
target.source = source.source,
target.service = source.service,
target.ownerteam = source.ownerteam,
target.owner = source.owner,
target.owneremail = source.owneremail,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.resolvedby = source.resolvedby,
target.resolveddatetime = source.resolveddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.LastModBy = source.LastModBy,
target.LastModDateTime = source.LastModDateTime,
target.ResponseEscLink = source.ResponseEscLink,
target.response_BreachPassed = source.response_BreachPassed,
target.response_targetclockduration = source.response_targetclockduration,
target.response_totalrunningduration = source.response_totalrunningduration,
target.ResolutionEscLink = source.ResolutionEscLink,
target.breachstatus = source.breachstatus,
target.L1DateTime = source.L1DateTime,
target.L1Passed = source.L1Passed,
target.L2DateTime = source.L2DateTime,
target.L2Passed = source.L2Passed,
target.L3DateTime = source.L3DateTime,
target.L3Passed = source.L3Passed,
target.BreachDateTime = source.BreachDateTime,
target.BreachPassed = source.BreachPassed,
target.targetclockduration = source.targetclockduration,
target.totalrunningduration = source.totalrunningduration,
target.ProfileLink_RecID = source.ProfileLink_RecID,
target.location = source.location,
target.customer = source.customer
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
recid,
number,
system,
company,
businessunit,
isvip,
subject,
status, 
source,
service,
ownerteam,
owner,
owneremail,
createdby,
createddatetime,
resolvedby,
resolveddatetime,
closedby,
closeddatetime,
LastModBy,
LastModDateTime,
ResponseEscLink,
response_BreachPassed,
response_targetclockduration,
response_totalrunningduration,
ResolutionEscLink,
breachstatus,
L1DateTime,
L1Passed,
L2DateTime,
L2Passed,
L3DateTime,
L3Passed,
BreachDateTime,
BreachPassed,
targetclockduration,
totalrunningduration,
ProfileLink_RecID,
location,
customer
)
VALUES (
source.recid,
source.number,
source.system,
source.company,
source.businessunit,
source.isvip,
source.subject,
source.status, 
source.source,
source.service,
source.ownerteam,
source.owner,
source.owneremail,
source.createdby,
source.createddatetime,
source.resolvedby,
source.resolveddatetime,
source.closedby,
source.closeddatetime,
source.LastModBy,
source.LastModDateTime,
source.ResponseEscLink,
source.response_BreachPassed,
source.response_targetclockduration,
source.response_totalrunningduration,
source.ResolutionEscLink,
source.breachstatus,
source.L1DateTime,
source.L1Passed,
source.L2DateTime,
source.L2Passed,
source.L3DateTime,
source.L3Passed,
source.BreachDateTime,
source.BreachPassed,
source.targetclockduration,
source.totalrunningduration,
source.ProfileLink_RecID,
source.location,
source.customer
);
