set language british; /*DATE CONVERSIONS WONT WORK WITHOUT THIS*/

/*CREATE A TEMP TABLE THEN INSERT IT INTO THE PERMENTANT TABLE*/
/*CREATE TABLE he_incident (*/
DECLARE @Temp_Table TABLE(
	sys_id CHAR(32) PRIMARY KEY, 
	number NVARCHAR(100),
	system NVARCHAR(100),
	company NVARCHAR(150),
	location NVARCHAR(100),
	--customer NVARCHAR(100),
	customer_id CHAR(32),
	subject NVARCHAR(255),
	priority NVARCHAR(10), 
	status NVARCHAR(40), 
	source NVARCHAR(40),
	category NVARCHAR(60),
	subcategory NVARCHAR(60),
	fcr NVARCHAR(10),
	ownerteam NVARCHAR(100),
	owner NVARCHAR(100),
	createdby NVARCHAR(100),
	createddatetime DATETIME,
	resolvedby NVARCHAR(100),
	resolveddatetime DATETIME,
	closedby NVARCHAR(100),
	closeddatetime DATETIME,
	lastmodby NVARCHAR(100),
	lastmoddatetime DATETIME,
	typeofincident NVARCHAR(20),
	active NVARCHAR(20),

	thirdpartysupplier NVARCHAR(100),
	thirdpartyreference NVARCHAR(100),
	resolvingteam NVARCHAR(100),
	fixcode NVARCHAR(50),
	exception NVARCHAR(10),
	exceptionreason NVARCHAR(30),
	lfcomments NVARCHAR(200),
	exceptionagreed NVARCHAR(10),
	hecomments NVARCHAR(200),
	--parentincident NVARCHAR(100),
	failedaccuracy NVARCHAR(10),
	failedtimeliness NVARCHAR(10),

	parentincident_id CHAR(32),
	changerequest_id CHAR(32),
	problem_id CHAR(32),
	reassignmentcount INT
);
INSERT INTO @Temp_Table
SELECT 
	sys_id,
	LEFT(number,100),
	'he' AS system,
	'Historic England' AS company,
	LEFT(location,100),
	--caller_id AS customer,
	CASE WHEN caller_id_value = '' THEN NULL ELSE caller_id_value END as customer_id,
	LEFT(short_description,255) AS subject,
	priority_value AS priority,
	LEFT(state,40) AS status,
	LEFT(contact_type,40) AS source,
	LEFT(category,60),
	LEFT(subcategory,60),
	CASE WHEN LOWER(u_first_fix) = 'true' THEN 1 ELSE 0 END as fcr,
	LEFT(assignment_group,100) AS ownerteam,
	LEFT(assigned_to,100) AS owner,
	LEFT(sys_created_by,100) AS createdby,
	CONVERT(DATETIME,[sys_created_on]) as createddatetime,
	LEFT(resolved_by,100) AS resolvedby,
	CASE WHEN ISNULL([resolved_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[resolved_at]) END as resolveddatetime,
	LEFT(closed_by,100) AS closedby,
	CASE WHEN ISNULL([closed_at],'') = '' THEN NULL ELSE CONVERT(DATETIME,[closed_at]) END as closeddatetime,
	sys_updated_by AS lastmodby,
	CASE WHEN ISNULL([sys_updated_on],'') = '' THEN NULL ELSE CONVERT(DATETIME,[sys_updated_on]) END as lastmoddatetime,
    CASE 
        WHEN CATEGORY = 'Request' THEN 'Request'
        ELSE 'Failure'
    END as typeofincident,
	active,
	LEFT(u_psupplier,100) AS thirdpartysupplier,
	LEFT(u_3rd_party_reference,100) AS thirdpartyreference,
	LEFT(u_resolving_team,100) AS resolvingteam,
	LEFT(u_fix_code,50) AS fixcode,
	LEFT(u_exception_y_n,100) AS exception,
	LEFT(u_exception_reason,30) AS exceptionreason,
	LEFT(u_lf_comments,200) AS lfcomments,
	u_agreed AS exceptionagreed,
	LEFT(u_he_comments,200) AS hecomments,
	--CASE WHEN parent_incident = '' THEN NULL ELSE parent_incident END as parentincident,
	LEFT(u_fa,10)AS failedaccuracy,
	LEFT(u_ft,10) AS failedtimeliness,

	CASE WHEN parent_incident_value = '' THEN NULL ELSE parent_incident_value END as parentincident_id,
	CASE WHEN rfc_value = '' THEN NULL ELSE rfc_value END as changerequest_id,
	CASE WHEN problem_id_value = '' THEN NULL ELSE problem_id_value END as problem_id,
	reassignment_count

FROM 
	[dbo].[stg] stg;

DECLARE @table_count FLOAT;
SET @table_count = (select COUNT(*) from @Temp_Table)
IF @table_count = 0
BEGIN
THROW 50000, 'TEMP TABLE EMPTY', 1;
END

/*MERGE THE TEMP TABLE WITH THE CLOSED INCIDENTS TABLE*/


MERGE [dbo].[he_incident] target
Using @Temp_Table source
ON (
	target.sys_id = source.sys_id
)
WHEN MATCHED
THEN UPDATE
SET
target.sys_id = source.sys_id,
target.number = source.number,
target.system = source.system,
target.company = source.company,
target.location = source.location,
--target.customer = source.customer,
target.customer_id = source.customer_id,
target.subject = source.subject,
target.priority  = source.priority ,
target.status  = source.status ,
target.source = source.source,
target.category = source.category,
target.subcategory = source.subcategory,
target.fcr = source.fcr,
target.ownerteam = source.ownerteam,
target.owner = source.owner,
target.createdby = source.createdby,
target.createddatetime = source.createddatetime,
target.resolvedby = source.resolvedby,
target.resolveddatetime = source.resolveddatetime,
target.closedby = source.closedby,
target.closeddatetime = source.closeddatetime,
target.lastmodby = source.lastmodby,
target.lastmoddatetime = source.lastmoddatetime,
target.typeofincident = source.typeofincident,
target.active = source.active,
target.thirdpartysupplier = source.thirdpartysupplier,
target.thirdpartyreference = source.thirdpartyreference,
target.resolvingteam = source.resolvingteam,
target.fixcode = source.fixcode,
target.exception = source.exception,
target.exceptionreason = source.exceptionreason,
target.lfcomments = source.lfcomments,
target.exceptionagreed = source.exceptionagreed,
target.hecomments = source.hecomments,
--target.parentincident = source.parentincident,
target.failedaccuracy = source.failedaccuracy,
target.failedtimeliness = source.failedtimeliness,

target.parentincident_id = source.parentincident_id,
target.changerequest_id = source.changerequest_id,
target.problem_id = source.problem_id,
target.reassignmentcount = source.reassignmentcount

WHEN NOT MATCHED BY TARGET
THEN INSERT 
(
sys_id,
number,
system,
company,
location,
--customer,
customer_id,
subject,
priority, 
status, 
source,
category,
subcategory,
fcr,
ownerteam,
owner,
createdby,
createddatetime,
resolvedby,
resolveddatetime,
closedby,
closeddatetime,
lastmodby,
lastmoddatetime,
typeofincident,
active,
thirdpartysupplier,
thirdpartyreference,
resolvingteam,
fixcode,
exception,
exceptionreason,
lfcomments,
exceptionagreed,
hecomments,
--parentincident,
failedaccuracy,
failedtimeliness,

parentincident_id,
changerequest_id,
problem_id,
reassignmentcount
)
VALUES (
source.sys_id,
source.number,
source.system,
source.company,
source.location,
--source.customer,
source.customer_id,
source.subject,
source.priority ,
source.status ,
source.source,
source.category,
source.subcategory,
source.fcr,
source.ownerteam,
source.owner,
source.createdby,
source.createddatetime,
source.resolvedby,
source.resolveddatetime,
source.closedby,
source.closeddatetime,
source.lastmodby,
source.lastmoddatetime,
source.typeofincident,
source.active,
source.thirdpartysupplier,
source.thirdpartyreference,
source.resolvingteam,
source.fixcode,
source.exception,
source.exceptionreason,
source.lfcomments,
source.exceptionagreed,
source.hecomments,
--source.parentincident,
source.failedaccuracy,
source.failedtimeliness,

source.parentincident_id,
source.changerequest_id,
source.problem_id,
source.reassignmentcount
);
/**/