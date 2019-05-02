set language british;
/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE heat_problem (*/
DECLARE @Temp_Table TABLE(
    rec_id NVARCHAR(100) PRIMARY KEY,
    number NVARCHAR(100),
    system NVARCHAR(100),
    company NVARCHAR(150),
    location NVARCHAR(100),
    customer NVARCHAR(100),
    subject NVARCHAR(255),
    priority NVARCHAR(10), 
    status NVARCHAR(40), 
    source NVARCHAR(40),
    category NVARCHAR(60),
    subcategory NVARCHAR(60),
    ownerteam NVARCHAR(100),
    owner NVARCHAR(100),
    createdby NVARCHAR(100),
    createddatetime DATETIME,
    closedby NVARCHAR(100),
    closeddatetime DATETIME,
    LastModBy NVARCHAR(100),
    LastModDateTime DATETIME,
    ResolutionEscLink NVARCHAR(100),
    ProfileLink_RecID NVARCHAR(100),
    breachstatus INT,
    L1DateTime DATETIME,
    L1Passed NVARCHAR(10),
    L2DateTime DATETIME,
    L2Passed NVARCHAR(10),
    L3DateTime DATETIME,
    L3Passed NVARCHAR(10),
    BreachDateTime DATETIME,
    BreachPassed NVARCHAR(10),
    worknotes NVARCHAR(MAX)
);
INSERT INTO @Temp_Table
SELECT 
    recid,
    ProblemNumber as number,
    'heat' AS system,
    OrgUnitName AS company,
    EmployeeLocation AS location,
    DisplayName AS customer,
    subject,
    priority,
    status,
    source,
    category,
    subcategory,
    ownerteam,
    owner,
    createdby,
    CONVERT(DATETIME,[createddatetime]) as createddatetime,
    closedby,
    CASE WHEN ISNULL([closeddatetime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closeddatetime]) END as closeddatetime,
    lastmodby,
    CASE WHEN ISNULL([LastModDateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[LastModDateTime]) END as LastModDateTime,
    ResolutionEscLink_recid,
    ProfileLink_RecID,
    NULL as breachstatus,
    CASE WHEN ISNULL([L1DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L1DateTime]) END as L1DateTime,
    L1Passed,
    CASE WHEN ISNULL([L2DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L2DateTime]) END as L2DateTime,
    L2Passed,
    CASE WHEN ISNULL([L3DateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[L3DateTime]) END as L3DateTime,
    L3Passed,
    CASE WHEN ISNULL([BreachDateTime],'') = '' THEN NULL ELSE CONVERT(DATETIME,[BreachDateTime]) END as BreachTime,
    BreachPassed,
    worknotes
FROM 
[dbo].[stg] stg;

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/
MERGE [dbo].[heat_problem] target
Using @Temp_Table source
ON (
target.rec_id = source.rec_id
)
WHEN MATCHED
THEN UPDATE
SET
target.rec_id = source.rec_id,
target.number = source.number,
target.system = source.system,
target.company = source.company,
target.location = source.location,
target.customer = source.customer,
target.subject = source.subject,
target.priority  = source.priority ,
target.status  = source.status ,
target.source = source.source,
target.category = source.category,
target.subcategory = source.subcategory,
target.ownerteam = source.ownerteam,
target.owner = source.owner,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.LastModBy = source.LastModBy,
target.LastModDateTime = source.LastModDateTime,
target.ResolutionEscLink = source.ResolutionEscLink,
target.ProfileLink_RecID = source.ProfileLink_RecID,
target.breachstatus = source.breachstatus,
target.L1DateTime = source.L1DateTime,
target.L1Passed = source.L1Passed,
target.L2DateTime = source.L2DateTime,
target.L2Passed = source.L2Passed,
target.L3DateTime = source.L3DateTime,
target.L3Passed = source.L3Passed,
target.BreachDateTime = source.BreachDateTime,
target.BreachPassed = source.BreachPassed,
target.worknotes = source.worknotes
WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
rec_id,
number,
system,
company,
location,
customer,
subject,
priority, 
status, 
source,
category,
subcategory,
ownerteam,
owner,
createdby,
createddatetime,
closedby,
closeddatetime,
LastModBy,
LastModDateTime,
ResolutionEscLink,
ProfileLink_RecID,
breachstatus,
L1DateTime,
L1Passed,
L2DateTime,
L2Passed,
L3DateTime,
L3Passed,
BreachDateTime,
BreachPassed,
worknotes
)
VALUES (
source.rec_id,
source.number,
source.system,
source.company,
source.location,
source.customer,
source.subject,
source.priority,
source.status ,
source.source,
source.category,
source.subcategory,
source.ownerteam,
source.owner,
source.createdby,
source.createddatetime,
source.closedby,
source.closeddatetime,
source.LastModBy,
source.LastModDateTime,
source.ResolutionEscLink,
source.ProfileLink_RecID,
source.breachstatus,
source.L1DateTime,
source.L1Passed,
source.L2DateTime,
source.L2Passed,
source.L3DateTime,
source.L3Passed,
source.BreachDateTime,
source.BreachPassed,
source.worknotes
);
