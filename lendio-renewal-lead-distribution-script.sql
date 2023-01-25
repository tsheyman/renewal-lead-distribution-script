-- V5 includes assignment to RMT MOL so marketing can identify unclaimed leads to be nurtured

/*##########################################################################################################################################
#############################################      RENEWALS DISTRIBUTION SCRIPT V5     #####################################################
##########################################################################################################################################*/


/*##########################################################################################################################################
##########################################################      SET UP     #################################################################
##########################################################################################################################################*/

-- Set schema
use LAKE_LENDIO.OPTIMUS;

----- List IDs (separated by commas) of special-case reps
	-- If no reps are to be excluded, set as 0
unset excludedReps; set excludedReps = '[3321311, 4595575]'; -- Keep 3321311 and 4595575 excluded until further notice.
unset pifReps; set pifReps = '[7951491, 7951483, 7951482, 4687639, 6464488]'; -- Riley W, Joseph M, Madison, Jaron C, Cort P
unset TLs; set TLs = '[3511228,3800126,3499554,1534790]';
unset renewalsTeams; set renewalsTeams = '[1338, 1374, 2081, 1373]'; -- Team Xandra, Team Beau, Team Jackson, New MP Team Green (Robb Taylor) 



---- Set limits for lead types to determine under what condition the lead should be considered for distribution
unset PIFpercLimit; set PIFpercLimit = 150;
unset ADRpercLimit; set ADRpercLimit = 250;


-- Parameters for qualified LOC leads
unset lastDrawRange; set lastDrawRange = 90; -- # of days since last draw
--unset locCreditScore; set locCreditScore = 500; -- Minimum qualifying credit score 
--unset locMonthsInBusiness; set locMonthsInBusiness = 6; -- Minimum qualifying months in business
--unset locMonthlySales; set locMonthlySales = 8000; -- Minimum qualifying monthly sales


----- Set claiming caps for reporting to determine if lead distribution amount was correct 
unset coreCap; set coreCap = 6;
unset pifCap; set pifCap = 9;
unset pifLocCap; set pifLocCap = 3;
unset adrCap; set adrCap = 8;
unset adrLocCap; set adrLocCap = 7;

-----

drop table if exists renewalLeads;
drop table if exists locLeads;
drop table if exists reps;
drop table if exists newLeads;
drop table if exists coreRecycledLeads;
drop table if exists coreCoolDownLeads;
drop table if exists pifCoolDownLeads;
drop table if exists adrCoolDownLeads;
drop table if exists coolDownCompleteLeads;
drop table if exists marketingOwnedLeads;
drop table if exists otherLeads;
drop table if exists mainDistribution;
drop table if exists coolDownDistribution;
drop table if exists adrLocCoolDown;
drop table if exists pifLocCoolDown;
drop table if exists locRecentDraw;
drop table if exists coreReps;
drop table if exists insertCoreReps;
drop table if exists pifReps;
drop table if exists insertPifReps;
drop table if exists pifRepsLoc;
drop table if exists insertPifRepsLoc;
drop table if exists adrReps;
drop table if exists insertAdrReps;
drop table if exists adrRepsLoc;
drop table if exists insertAdrRepsLoc;
drop table if exists coreDistribution;
drop table if exists pifDistribution;
drop table if exists adrDistribution;
drop table if exists molDistribution;
drop table if exists rmtCoolDownDistribution;
drop table if exists pifLocDistribution;
drop table if exists adrLocDistribution;
drop table if exists rmtLocCoolDownDistribution;
drop table if exists finalList;


/*##########################################################################################################################################
##################################################     CONSTRUCT RENEWALS LIST      ########################################################
##########################################################################################################################################*/

-- Create list of all renewal eligible borrowers, based only on their most recent deal funded 
-- Includes check for assignment to lead and successful in the last 30 days
-- Includes a number of other filters as described by Kris and Zach
create temporary table renewalLeads as (
	with allDeals as ( 
		select 
			d.id as dealId, d.BORROWERID, d.ACCEPTEDOFFERID, d.LOANPRODUCTLENDERID, d.loanProductCategoryId, 
			o.term, 
			i.id as institutionId, i.name as lender, i.renewalEligibility as renewalEligibilityPerc,
			convert_timezone('America/Denver', d.DATECLOSED) as dateClosed,
			row_number() over (partition by BORROWERID order by DATECLOSED desc) as rn
		from deals d
			join offers o on o.dealId = d.id 
				and o.id = d.acceptedOfferId
				and o.term > 0
			join institutions i on i.id = d.loanProductLenderId 
		----- Core Filters -----
		where d.deleted is null 
			and d.closedBy not in (select lenderUserId from teamLenderUsers where teamId = 1320) -- not Lendio Franchise team
			and d.stage = 'funded'
			and d.STATUS <> 'default'
			and LOANPRODUCTCATEGORYID not in (17,18) -- not a PPP deal
	), recentDeals as (
		select 
			DEALID, BORROWERID, ACCEPTEDOFFERID, LOANPRODUCTLENDERID, LOANPRODUCTCATEGORYID, TERM, INSTITUTIONID, LENDER, RENEWALELIGIBILITYPERC, DATECLOSED
		from allDeals
		where rn = 1
	), allAssignments as (	
		select 
			id as assignmentId, BORROWERID, NEWLENDERUSERID, convert_timezone('America/Denver', CREATED) as assignmentDate,
			row_number() over (partition by BORROWERID, NEWLENDERUSERID order by CREATED desc) as rn 
		from ASSIGNMENTS
		where created > dateadd(day, -30, CURRENT_TIMESTAMP())
			and NEWLENDERUSERID <> 992232
			and deleted is null 
	), recentAssignments as (
		select 
			assignmentId, BORROWERID, NEWLENDERUSERID, assignmentDate
		from allAssignments
		where rn = 1
	), allCalls as (
		select 
			id as callId, borrowerid, lenderuserid, type, duration, convert_timezone('America/Denver', created) as callDate,
			row_number() over (partition by BORROWERID, LENDERUSERID order by created desc) as rn
		from calls 
		where deleted is null 
			and duration >= 30
			and created > dateadd(day, -30, CURRENT_TIMESTAMP())
			and type = 1 -- Outbound call
	), recentCalls as (
		select 
			callId, borrowerid, lenderuserid, type, duration, callDate
		from allCalls 
		where rn = 1
	), allInterest as (
		select
			id as interestId, BORROWERID, attributeId, value, created,
			row_number() over (partition by borrowerid order by created desc) as rn
		from BORROWERVALUES
		where ATTRIBUTEID = 907 
			and deleted is null
	), recentInterest as (
		select 
			interestId, BORROWERID, ATTRIBUTEID, VALUE
		from allInterest
		where rn = 1
	)	
	select
		b.id as borrowerId, 
		b.name as businessName,
		b.userId as currentRep, concat(u.first, ' ', u.last) as repName, 
		d.institutionId, d.lender, 
		b.stage, b.status, b.leadSource, 
		d.loanProductCategoryId, d.dateClosed, 
		timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) as offerTermDays,
		d.renewalEligibilityPerc,
			case -- if lender is on Deck, 6 months, else null
				when d.institutionId = 224 then 6 
				else null
			end as renewalEligibilityMonths,		
		case -- If lender is On Deck and renewalEligibilityMonths <= renewalEligibility%, return renewalEligibilityMonths. Else return renewalEligibility% 
			when d.institutionId = 224
				and dateadd(month, 6, d.dateClosed) <= dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed) 
				then dateadd(month, 6, d.dateClosed)
			else dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed)
		end as renewalEligibilityDate,
		timestampdiff(
			day, 
			case -- If lender is On Deck and renewalEligibilityMonths <= renewalEligibility%, return renewalEligibilityMonths. Else return renewalEligibility% 
				when d.institutionId = 224
					and dateadd(month, 6, d.dateClosed) <= dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed) 
					then dateadd(month, 6, d.dateClosed)
				else dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed)
			end, 
			current_timestamp()
		) as daysSinceRenewalEligibility,
		round(100 * timestampdiff(day, d.dateClosed, CURRENT_TIMESTAMP()) /  timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)), 2) as termCompletePerc,
		a.assignmentDate as assignmentLast30Days,
		c.callDate as callLast30Days
	from 
		borrowers b
		join recentDeals d on b.id = d.borrowerId 
		join users u on b.userId = u.id
		left join recentInterest i on i.borrowerid = b.id
		left join recentAssignments a on b.id = a.BORROWERID and b.USERID = a.NEWLENDERUSERID
		left join recentCalls c on c.BORROWERID = b.id and c.LENDERUSERID = b.userId
	----- Core Filters -----
	where b.ISTEST = 0 and b.DELETED is null 
		and b.ZEEID is null
		and b.status not in ('doNotContact', 'dead')
		and (i.value = 'marketplace' or i.value is null) -- Borrower has marketplace or NULL interest
		and b.USERID <> 6548524 -- Exclude borrowers assigned to Legal Compliance
		and -- renewalEligibilityDate <= current_timestamp
			case 
				when d.institutionId = 224
					and dateadd(month, 6, d.dateClosed) <= dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed) 
					then dateadd(month, 6, d.dateClosed)
				else dateadd(day, (timestampdiff(day, d.dateClosed, dateadd(month, d.term, d.dateClosed)) * (d.renewalEligibilityPerc * .01)), d.dateClosed)
			end <= current_timestamp()
	----- Extra Filters -----
		and d.loanProductCategoryId in (1,4,15) -- ACH, Term, Flex
		and b.id <> 5522704 -- Special deal in progress borrower
		and b.id not in (3085446, 6614544) -- do not distributes
		and d.institutionId <> 44260 -- LoanMe
		and b.leadSource not in ('AMEXMerchantFinancing', 'AMEXMerchantFinancingEmail', 'AMEXMerchantFinancingMailer')
		and (d.institutionId,d.loanProductCategoryId) <> (224,3) -- Exclude On Deck (224) LOC		
);



/*##########################################################################################################################################
##################################################     CONSTRUCT LOC LIST      #############################################################
##########################################################################################################################################*/

-- Create list of locLeads
create temporary table locLeads as (
	with allDeals as ( 
		select 
			d.id as dealId, d.BORROWERID, d.ACCEPTEDOFFERID, d.LOANPRODUCTLENDERID, d.loanProductCategoryId,
			i.id as institutionId, i.name as lender, i.renewalEligibility as renewalEligibilityPerc,
			convert_timezone('America/Denver', d.DATECLOSED) as dateClosed,
		row_number() over (partition by BORROWERID order by DATECLOSED desc) as rn
		from deals d
			join institutions i on i.id = d.loanProductLenderId 
		----- Core Filters -----
		where d.deleted is null 
			and d.closedBy not in (select lenderUserId from teamLenderUsers where teamId = 1320) -- not Lendio Franchise team
			and d.stage = 'funded'
			and d.STATUS <> 'default'
			and LOANPRODUCTCATEGORYID not in (17,18) -- not a PPP deal
	), recentLocDeals as (
		select 
			DEALID, BORROWERID, ACCEPTEDOFFERID, LOANPRODUCTLENDERID, LOANPRODUCTCATEGORYID, INSTITUTIONID, LENDER, RENEWALELIGIBILITYPERC, DATECLOSED
		from allDeals
		where rn = 1
			and LOANPRODUCTCATEGORYID = 3 -- LOC Deal
	), lastDraws as (
			select 
				BORROWERID, max(convert_timezone('America/Denver', d.created)) as lastDraw
			from deals d
			where d.BORROWERID in (select BORROWERID from recentLocDeals)
				and d.type = 'draw'
				and d.deleted is null
			group by BORROWERID having max(convert_timezone('America/Denver', d.created)) < dateadd(day, (-1 * $lastDrawRange), CURRENT_TIMESTAMP()) 
	), allAssignments as (	
		select 
			BORROWERID, NEWLENDERUSERID, convert_timezone('America/Denver', CREATED) as assignmentDate,
			row_number() over (partition by BORROWERID, NEWLENDERUSERID order by CREATED desc) as rn 
		from ASSIGNMENTS
		where created > dateadd(day, -30, CURRENT_TIMESTAMP())
			and NEWLENDERUSERID <> 992232
			and deleted is null 
	), recentAssignments as (
		select 
			BORROWERID, NEWLENDERUSERID, assignmentDate
		from allAssignments
		where rn = 1
	)
	select 
		b.id as borrowerId, b.name as businessName,
		b.userId as currentRep, concat(u.first, ' ', u.last) as repName, 
		d.institutionId, d.lender, 
		b.stage, b.status, b.leadSource, 
		d.loanProductCategoryId, d.dateClosed, 
		a.assignmentDate as assignmentLast30Days,
		l.lastDraw as lastEligibleDraw
	from 
		borrowers b
		join recentLocDeals d on b.id = d.borrowerId 
		join users u on b.userId = u.id
		left join recentAssignments a on b.id = a.BORROWERID and b.USERID = a.NEWLENDERUSERID
		left join lastDraws l on b.id = l.borrowerid
);



/*##########################################################################################################################################
##################################################     CONSTRUCT REP LIST      #############################################################
##########################################################################################################################################*/

-- Create list of active renewal reps for reference throughout the script
create temporary table reps as (
	select 
		t.LENDERUSERID, t.TEAMID, tm.name as teamName,
		case 
			when t.LENDERUSERID in (select value from table(flatten(input => parse_json($TLs)))) then 1 -- Current TLs
			else 0
		end as tl, 
		case 
			when t.LENDERUSERID in (select value from table(flatten(input => parse_json($pifReps)))) then 1 -- Current PIF reps
			else 0
		end as pif,
		case 
			when t.teamid = 1373 then 1 -- Current ADRs
			else 0
		end as adr 
	from teamLenderUsers t 
		join teams tm on t.teamid = tm.id
	where t.deleted is null and t.status = 1
		and t.teamId in (select value from table(flatten(input => parse_json($renewalsTeams)))) -- Current renewals teams
		and t.lenderUserId not in (3570793, 3184535, 5376933, 4339995, 4492237, 5639073, 3547614, 4472229, 809204, 8207124) -- Exclude The Hudsons & Kris G & RMT MOL
);




/*##########################################################################################################################################
###############################################     NEW / CORERECYCLED / CORECOOLDOWN      #################################################
##########################################################################################################################################*/

-- Create list of new leads for Core reps (newly eligible leads since last run)
create temporary table newLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, RENEWALELIGIBILITYDATE,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'new' as leadType
	from renewalLeads
	where (currentrep not in (select lenderuserid from reps) or (currentrep in (select lenderuserid from reps where adr = 1) and termCompletePerc <= $PIFpercLimit))
		and (
				   iff(dayofweek(current_timestamp()) = 1 and renewalEligibilityDate::date between dateadd(day, -3, current_date()) and current_date(), 1,0) = 1 -- Mon
				or iff(dayofweek(current_timestamp()) <> 1 and renewalEligibilityDate::date between dateadd(day, -1, current_date()) and current_date(), 1,0) = 1 -- Tues-Sun
			)
);


-- Create list of core recycleable leads that will be assigned to a new core rep(leads without assignmentDate or outboundCall in the past 30 days)
create temporary table coreRecycledLeads as (
		select 
			BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
			'coreRecycled' as leadType		
		from renewalLeads
		where CURRENTREP in (select lenderUserId from reps where pif = 0 and adr = 0) -- only check leads that are currently assigned to CORE
			and termCompletePerc < $PIFpercLimit -- Non-PIF eligible leads
			and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
			and ( -- Statuses are specified to ensure we don't pull "deal in progress", etc leads
				   (status = 'inContact' and ASSIGNMENTLAST30DAYS is null and CALLLAST30DAYS is null)
				or (status in ('noAttempt', 'attempted') and ASSIGNMENTLAST30DAYS is null)
				or (stage = 'inactive' and ASSIGNMENTLAST30DAYS is null)
			)	
);


-- Create list of recycleable Core leads to be assigned to RMT Cool Down
create temporary table coreCoolDownLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'coreCoolDown' as leadType		
	from renewalLeads
	where CURRENTREP in (select lenderUserId from reps where pif = 0 and adr = 0) -- only check leads that are currently assigned to CORE
		and termCompletePerc >= $PIFpercLimit -- PIF eligible leads
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and ( -- Statuses are specified to ensure we don't pull "deal in progress", etc leads
				   (status = 'inContact' and ASSIGNMENTLAST30DAYS is null and CALLLAST30DAYS is null)
				or (status in ('noAttempt', 'attempted') and ASSIGNMENTLAST30DAYS is null)
				or (stage = 'inactive' and ASSIGNMENTLAST30DAYS is null)
			)
);


/*##########################################################################################################################################
###########################################################     PIFCOOLDOWN       ##########################################################
##########################################################################################################################################*/

-- Create list of recycleable Core leads to be assigned to RMT Cool Down
create temporary table pifCoolDownLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'pifCoolDown' as leadType		
	from renewalLeads
	where CURRENTREP in (select lenderUserId from reps where pif = 1) -- only check leads that are currently assigned to PIF
		and termCompletePerc >= $PIFpercLimit
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and borrowerId not in (select borrowerid from coreCoolDownLeads) -- exclude coolDown leads that have already been identified
		and ASSIGNMENTLAST30DAYS is null 
		and CALLLAST30DAYS is null
);



/*##########################################################################################################################################
#########################################################     ADRCOOLDOWN      #############################################################
##########################################################################################################################################*/

-- Create list of recycleable ADR leads to be assigned to RMT Cool Down
create temporary table adrCoolDownLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'adrCoolDown' as leadType		
	from renewalLeads
	where CURRENTREP in (select lenderuserid from reps where adr = 1) -- only check leads that are currently assigned to ADRs
		and termCompletePerc >= $PIFpercLimit
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and borrowerId not in (select borrowerid from coreCoolDownLeads) -- exclude coolDown leads that have already been identified
		and borrowerId not in (select borrowerid from pifCoolDownLeads) -- exclude pifCoolDown leads that have already been identified
		and ASSIGNMENTLAST30DAYS is null -- Recycle lead after 30 days regardless of status
);

	


/*##########################################################################################################################################
###########################################     OTHER DISTRIBUTION LEADS     ##################################################
##########################################################################################################################################*/
 
-- Create list of RMT Cool Down leads that have finished their cool down period and are eligible for re-distribution
create temporary table coolDownCompleteLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'coolDownComplete' as leadType
	from RENEWALLEADS
	where currentrep = 4339995 -- RMT Cool Down
		and assignmentlast30days is null -- cool down period complete
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and borrowerId not in (select borrowerid from coreCoolDownLeads) -- exclude coolDown leads that have already been identified
		and borrowerId not in (select borrowerid from pifCoolDownLeads) -- exclude pifCoolDown leads that have already been identified
		and borrowerId not in (select borrowerid from adrCoolDownLeads) -- exclude adrCoolDown leads that have already been identified
);


-- Create list of "Other" leads eligible to be distributed to reps
create temporary table otherLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'otherLeads' as leadType
	from RENEWALLEADS
	where currentrep not in (select lenderuserid from reps) -- exclude owned by current FM
		and CURRENTREP <> 7276170 -- Exclude RMT Dialer	
		and ASSIGNMENTLAST30DAYS is NULL 
--		and calllast30days is null 
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and borrowerId not in (select borrowerid from coreCoolDownLeads) -- exclude coolDown leads that have already been identified
		and borrowerId not in (select borrowerid from pifCoolDownLeads) -- exclude pifCoolDown leads that have already been identified
		and borrowerId not in (select borrowerid from adrCoolDownLeads) -- exclude adrCoolDown leads that have already been identified
		and borrowerId not in (select borrowerid from coolDownCompleteLeads) -- exclude coolDownComplete leads that have already been identified
);


-- Create list of Marketing Owned Leads (MOL) eligible to be distributed to reps
 create temporary table marketingOwnedLeads as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED, OFFERTERMDAYS, renewalEligibilityDate,  RENEWALELIGIBILITYPERC, TERMCOMPLETEPERC, daysSinceRenewalEligibility, ASSIGNMENTLAST30DAYS, CALLLAST30DAYS,
		'MOL' as leadType
	from RENEWALLEADS
	where CURRENTREP = 8207124 -- RMT MOL
		and borrowerId not in (select borrowerid from newLeads) -- exclude new leads that have already been identified
		and borrowerId not in (select borrowerid from coreRecycledLeads) -- exclude recycled leads that have already been identified
		and borrowerId not in (select borrowerid from coreCoolDownLeads) -- exclude coolDown leads that have already been identified
		and borrowerId not in (select borrowerid from pifCoolDownLeads) -- exclude pifCoolDown leads that have already been identified
		and borrowerId not in (select borrowerid from adrCoolDownLeads) -- exclude adrCoolDown leads that have already been identified
		and borrowerId not in (select borrowerid from coolDownCompleteLeads) -- exclude coolDownComplete leads that have already been identified
		and borrowerId not in (select borrowerid from otherLeads) -- exclude otherLeads that have already been identified		
);





/*##########################################################################################################################################
###############################################     CONSTRUCT FM DISTRIBUTION LIST       ###################################################
##########################################################################################################################################*/


-- Finalized list of leads for human distribution
create temporary table mainDistribution as (
	select * from newLeads
		union all
	select * from coreRecycledLeads
		union all
	select * from coolDownCompleteLeads
		union all 
	select * from marketingOwnedLeads
		union all 
	select * from otherLeads
);




/*##########################################################################################################################################
###############################################     CONSTRUCT Cool Down DISTRIBUTION LIST      #############################################
##########################################################################################################################################*/

-- Finalized list of leads for RMT Cool Down reassignment
create temporary table coolDownDistribution as (
	select * from coreCoolDownLeads
		union all 
	select * from pifCoolDownLeads
		union all
	select * from adrCoolDownLeads
);




/*##########################################################################################################################################
###########################################     PIFLOCCOOLDOWN / ADRLOCCOOLDOWN / locRecentDraw     ########################################
##########################################################################################################################################*/

-- PIF LOC leads that have been assigned for 30+ days to be re-assigned to RMT LOC Cool Down
create temporary table pifLocCoolDown as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED,  ASSIGNMENTLAST30DAYS, lastEligibleDraw,
		'pifLocCoolDown' as leadType
	from locLeads 
	where currentRep in (select lenderuserid from reps where pif = 1) -- assigned to PIF
		and assignmentLast30Days is null
);



-- ADR LOC leads that have been assigned for 30+ days to be re-assigned to RMT LOC Cool Down
create temporary table adrLocCoolDown as (
	select 
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED,  ASSIGNMENTLAST30DAYS, lastEligibleDraw,
		'adrLocCoolDown' as leadType
	from locLeads 
	where currentRep in (select lenderuserid from reps where adr = 1) -- assigned to ADR
		and assignmentLast30Days is null
);



-- Create list of LOC borrowers eligible to be distributed to PIFs and ADRs
create temporary table locRecentDraw as (
--	with creditScore as (
--		select 
--			borrowerid
--		from BORROWERVALUES
--		where ATTRIBUTEID = 66
--			and DELETED is null 
--			and try_cast(value as integer) >= $locCreditScore 
--	), monthsInBusiness as (
--		select 
--			borrowerid
--		from BORROWERVALUES
--		where ATTRIBUTEID = 70
--			and DELETED is null 
--			and try_cast(value as integer) >= $locMonthsInBusiness
--	), monthlySales as ( 
--		select 
--			borrowerid
--		from BORROWERVALUES
--		where ATTRIBUTEID = 466
--			and DELETED is null 
--			and try_cast(value as integer) >= $locMonthlySales
--	)
	select
		BORROWERID, BUSINESSNAME, CURRENTREP, REPNAME, INSTITUTIONID, LENDER, "STAGE", STATUS, LEADSOURCE, LOANPRODUCTCATEGORYID, DATECLOSED,  ASSIGNMENTLAST30DAYS, lastEligibleDraw,
		'LOC' as leadType
	from locleads
	where lastEligibleDraw is not null
		and BORROWERID not in (select borrowerid from pifLocCoolDown)
		and BORROWERID not in (select borrowerid from adrLocCoolDown)
--		and BORROWERID in (select BORROWERID from creditScore)
--		and BORROWERID in (select BORROWERID from monthsInBusiness)
--		and BORROWERID in (select BORROWERID from monthlySales)
		and (
			currentRep in (7456177, 992232) -- Success Team, System Account
			or (currentRep = 4492237 and assignmentLast30Days is null) -- RMT LOC Cool Down after 30 day cool down
		)
);


/*##########################################################################################################################################
####################################################      FM GROUPS AND CAP      ###########################################################
##########################################################################################################################################*/


-------------------- Core Reps, cap = 6 --------------------

-- Create table to store Core Reps
create temporary table coreReps (
	name varchar(50),
	lenderUserId number(38,0),
	teamId number(38,0),
	type varchar(25), 
	repRn number(38,0)
);


-- Create list of Core Reps in randomized order
create temporary table insertCoreReps as (
	select 
		concat(u.first, ' ', u.last) as name,
		LENDERUSERID, TEAMID,
		'Core' as repType,
		row_number() over (order by random()) as repRn
	from reps r
		join users u on r.lenderUserId = u.id
	where tl = 0 -- exclude TLs
		and pif = 0 -- exclude PIF reps
		and adr = 0 -- exclude ADR reps
		and lenderUserId not in (select value from table(flatten(input => parse_json($excludedReps)))) -- Today's absent reps
);		


-- Insert reps to coreReps table until cap is met (cap = 6)
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 1
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 2
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 3
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 4
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 5
insert into coreReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from coreReps) as repRn from insertCoreReps); -- 6


------------------ PIF Reps, cap = 12, 9 PIF ----------------------
 
-- Create table to store PIF Reps
create temporary table pifReps like coreReps;


-- Create list of PIF Reps in randomized order
create temporary table insertPifReps as (
	select 
		concat(u.first, ' ', u.last) as name,
		LENDERUSERID, TEAMID,
		'PIF' as repType,
		row_number() over (order by random()) as repRn
	from reps r
		join users u on r.lenderUserId = u.id
	where tl = 0 -- exclude TLs
		and pif = 1 -- Include PIF reps
		and adr = 0 -- exclude ADR reps
		and lenderUserId not in (select value from table(flatten(input => parse_json($excludedReps)))) -- Today's absent reps
);


-- Insert reps to pifReps table until cap is met (cap = 10) 
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 1
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 2
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 3
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 4
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 5
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 6
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 7
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 8
insert into pifReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifReps) as repRn from insertpifReps); -- 9




-------------------- PIF Reps, cap = 12, 3 LOC --------------------

-- Create table to store PIF LOC Reps
create temporary table pifRepsLoc like coreReps;


-- Create list of PIF LOC Reps in randomized order
create temporary table insertPifRepsLoc as (
	select 
		concat(u.first, ' ', u.last) as name,
		LENDERUSERID, TEAMID,
		'PIF LOC' as repType,
		row_number() over (order by random()) as repRn
	from reps r
		join users u on r.lenderUserId = u.id
	where tl = 0 -- exclude TLs
		and pif = 1 -- Include PIF reps
		and adr = 0 -- exclude ADR reps
		and lenderUserId not in (select value from table(flatten(input => parse_json($excludedReps)))) -- Today's absent reps	
);


-- Insert reps to pifRepsLoc table until cap is met (cap = 3 LOC) 
insert into pifRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifRepsLoc) as repRn from insertPifRepsLoc); -- 1
insert into pifRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifRepsLoc) as repRn from insertPifRepsLoc); -- 2
insert into pifRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from pifRepsLoc) as repRn from insertPifRepsLoc); -- 3



-------------------- ADR Reps, cap = 15, 8 PIF --------------------

-- Create table to store ADR Reps
create temporary table adrReps like coreReps;


-- Create list of ADR Reps in randomized order
create temporary table insertAdrReps as (
	select 
		concat(u.first, ' ', u.last) as name,
		LENDERUSERID, TEAMID,
		'ADR' as repType,
		row_number() over (order by random()) as repRn
	from reps r
		join users u on r.lenderUserId = u.id
	where tl = 0 -- exclude TLs
		and pif = 0 -- Include PIF reps
		and adr = 1 -- exclude ADR reps
		and lenderUserId not in (select value from table(flatten(input => parse_json($excludedReps)))) -- Today's absent reps
);


-- Insert reps to adrReps table until cap is met (cap = 8 PIF) 
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 1
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 2
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 3
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 4
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 5
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 6
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 7
insert into adrReps (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrReps) as repRn from insertadrReps); -- 8


-------------------- ADR Reps, cap = 15, 7 LOC --------------------

-- Create table to store ADR LOC Reps
create temporary table adrRepsLoc like coreReps;


-- Create list of ADR Reps in randomized order
create temporary table insertAdrRepsLoc as (
	select 
		concat(u.first, ' ', u.last) as name,
		LENDERUSERID, TEAMID,
		'ADR LOC' as repType,
		row_number() over (order by random()) as repRn
	from reps r
		join users u on r.lenderUserId = u.id
	where tl = 0 -- exclude TLs
		and pif = 0 -- Include PIF reps
		and adr = 1 -- exclude ADR reps
		and lenderUserId not in (select value from table(flatten(input => parse_json($excludedReps)))) -- Today's absent reps	
);


-- Insert reps to adrRepsLoc table until cap is met (cap = 7 LOC) 
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 1
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 2
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 3
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 4
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 5
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 6
insert into adrRepsLoc (select name, lenderuserid, teamid, repType, repRn + (select count(*) from adrRepsLoc) as repRn from insertAdrRepsLoc); -- 7	



/*##########################################################################################################################################
##################################################      USER -> LEAD MATCHING      #########################################################
##########################################################################################################################################*/
 

-- Join core reps to mainDistribution
	-- Using row_number() at this level so the list is dynamic and re-orders itself as we go back through the lists again to join to new reps
create temporary table coreDistribution as (
	with mainDistributionRn as (
		select 
			*,
			row_number() over (order by daysSinceRenewalEligibility) as leadRn
		from MAINDISTRIBUTION
	)
	select *
	from mainDistributionRn m
		join COREREPS c on m.leadRn = c.repRn
);
	
-- Remove coreDistributed borrowers so they are not re-distributed for the next FM group
delete from mainDistribution
where borrowerId in (select borrowerId from coreDistribution);



-- Join PIF reps to mainDistribution
	-- Using row_number() at this level so the list is dynamic and re-orders itself as we go back through the lists again to join to new reps
create temporary table pifDistribution as (
	with mainDistributionRn as (
		select 
			*,
			row_number() over (order by daysSinceRenewalEligibility) as leadRn
		from MAINDISTRIBUTION
		where termCompletePerc > $PIFpercLimit
	)
	select *
	from mainDistributionRn m
		join PIFREPS p on m.leadRn = p.repRn 
);


-- Remove pifDistributed borrowers so they are not re-distributed for the next FM group
delete from mainDistribution
where borrowerId in (select borrowerId from pifDistribution);



-- Join ADR reps to mainDistribution
	-- Using row_number() at this level so the list is dynamic and re-orders itself as we go back through the lists again to join to new reps
create temporary table adrDistribution as (
	with mainDistributionRn as (
		select 
			*,
			row_number() over (order by daysSinceRenewalEligibility) as leadRn
		from MAINDISTRIBUTION
		where termCompletePerc > $ADRpercLimit
	)
	select *
	from mainDistributionRn m
		join ADRREPS a on m.leadRn = a.repRn
);

-- Remove adrDistributed borrowers so they are not re-distributed for the next FM group
delete from mainDistribution
where borrowerId in (select borrowerId from adrDistribution);


/*##########################################################################################################################################
#######################################################      MOL MATCHING      #############################################################
##########################################################################################################################################*/

-- Assign leads to RMT MOL here
create temporary table molDistribution as (
	select 
		*,
		null as leadRn,
		'RMT MOL' as name, 8207124 as lenderUserId, 
		null as teamId, 'MOL' as type, null as repRn
	from MAINDISTRIBUTION
	where leadtype not in ('new', 'coreRecycled') 
		and CURRENTREP <> 8207124 -- Not assigned to RMT MOL
);


/*##########################################################################################################################################
####################################################      COOLDOWN MATCHING      ###########################################################
##########################################################################################################################################*/

-- Assign leads to RMT Cool Down here 
create temporary table rmtCoolDownDistribution as (
	select 
		*,
		null as leadRn,
		'RMT Cool Down' as name, 4339995 as lenderUserId, 
		null as teamId, 'coolDown' as type, null as repRn
	from coolDownDistribution
	order by daysSinceRenewalEligibility
);


/*##########################################################################################################################################
######################################################      LOC MATCHING      ##############################################################
##########################################################################################################################################*/


-- Assign LOC leads to PIF here
create temporary table pifLocDistribution as (
	with locRecentDrawRn as (
		select 
			*,
			row_number() over (order by lastEligibleDraw desc) as leadRn 
		from LOCRECENTDRAW
	)
	select 
		*
	from locRecentDrawRn l
		join pifRepsLoc a on l.leadRn = a.repRn
);


-- Remove pifDistributed LOC borrowers so they are not re-distributed for the next FM group
delete from LOCRECENTDRAW
where borrowerid in (select borrowerid from pifLocDistribution);


-- Assign LOC leads to ADR here
	-- locRecentDraw -> ADR
create temporary table adrLocDistribution as (
	with locRecentDrawRn as (
		select 
			*,
			row_number() over (order by lastEligibleDraw desc) as leadRn 
		from LOCRECENTDRAW
	)
	select 
		*
	from locRecentDrawRn l
		join adrRepsLoc a on l.leadRn = a.repRn
);


/*##########################################################################################################################################
################################################      LOC COOLDOWN MATCHING      ###########################################################
##########################################################################################################################################*/

-- Assign leads to RMT LOC Cool Down here
create temporary table rmtLocCoolDownDistribution as (
		select 
			*,
			'RMT LOC Cool Down' as name, 4492237 as lenderUserId, null as teamId, 'locCoolDown' as type, null as repRn
		from PIFLOCCOOLDOWN
	union all 
		select 
			*,
			'RMT LOC Cool Down' as name, 4492237 as lenderUserId, null as teamId, 'locCoolDown' as type, null as repRn
		from ADRLOCCOOLDOWN	
);



/*##########################################################################################################################################
############################################      COMPILE FINAL DISTRIBUTION LIST      #####################################################
##########################################################################################################################################*/

-- Compile Core, ADR, LOC, RMT Cool Down and RMT LOC Cool Down lists
create temporary table finalList as (
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, renewalEligibilityDate, termCompletePerc, daysSinceRenewalEligibility, null as lastEligibleDraw,
		1 as prio
	from COREDISTRIBUTION
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, renewalEligibilityDate, termCompletePerc, daysSinceRenewalEligibility, null as lastEligibleDraw,
		2 as prio
	from PIFDISTRIBUTION	
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, null as renewalEligibilityDate, null as termCompletePerc, null as daysSinceRenewalEligibility, lastEligibleDraw,
		3 as prio
	from PIFLOCDISTRIBUTION
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, renewalEligibilityDate, termCompletePerc, daysSinceRenewalEligibility, null as lastEligibleDraw,
		4 as prio
	from ADRDISTRIBUTION
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, null as renewalEligibilityDate, null as termCompletePerc, null as daysSinceRenewalEligibility, lastEligibleDraw,
		5 as prio
	from ADRLOCDISTRIBUTION
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, renewalEligibilityDate, termCompletePerc, daysSinceRenewalEligibility, null as lastEligibleDraw,
		7 as prio
	from rmtCoolDownDistribution
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, null as renewalEligibilityDate, null as termCompletePerc, null as daysSinceRenewalEligibility, lastEligibleDraw,
		8 as prio
	from rmtLocCoolDownDistribution
union all 
	select 
		borrowerId, lenderUserId as newRep, name as newRepName, type as newRepType, leadType, stage, status,  currentRep, repName as currentRepName, renewalEligibilityDate, termCompletePerc, daysSinceRenewalEligibility, null as lastEligibleDraw,
		9 as prio
	from molDistribution	
order by prio, daysSinceRenewalEligibility, lastEligibleDraw desc
);





/*##########################################################################################################################################
###################################################      DISTRIBUTION INFO       ###########################################################
##########################################################################################################################################*/

-- List of Teams, Reps, and the reps' new leads for the day
select 
	teamname, newrepname,
	listagg(borrowerid, ', ') as newBorrowerIds,
	current_date() as dateAssigned
from finallist f 
	join reps r on r.lenderuserid = f.newrep
group by newrepname, teamname
order by teamname, newrepname;


-- Return results of final list
select * from finalList order by prio, daysSinceRenewalEligibility, lastEligibleDraw desc;



/*##########################################################################################################################################
########################################################      REPORTING      ###############################################################
##########################################################################################################################################*/

-- Reporting for assignment and lead info
	with multipleDistributions as (
		select 
			borrowerid, count(*) as distributed
		from finalList 
		group by borrowerid having count(*) > 1
	)
	select 'This report was run at ' || 
		year(convert_timezone('America/Denver', current_timestamp())) || '-' || month(convert_timezone('America/Denver', current_timestamp())) || '-' || day(convert_timezone('America/Denver', current_timestamp())) || ' ' || 
		hour(convert_timezone('America/Denver', current_timestamp())) || ':' || minute(convert_timezone('America/Denver', current_timestamp())) || ':' || second(convert_timezone('America/Denver', current_timestamp())) 
	as report
union all 
	select 
		case 
			when count(*) = 0 then ''
			when count(*) > 0 then 'The following borrowers were distributed multiple times: ' || listagg(distinct borrowerid, ', ')
			else 'MULTIPLE LEADS COUNT ERROR!'
		end as report
	from multipleDistributions
union all 
	select
		case 
			when count(*) / count(distinct newRep) <  $coreCap then 'Some Core reps were assigned less than ' || $coreCap::varchar || ' leads.'
			when count(*) / count(distinct newRep) =  $coreCap then count(distinct newRep) || ' Core reps received ' || $coreCap::varchar || ' leads each.'
			when count(*) / count(distinct newRep) >  $coreCap then 'Some Core reps were assigned more than ' || $coreCap::varchar || ' leads.'
		end as report
	from finalList
	where newRepType = 'Core'
union all 
	select 
		'There were ' || count(*) || ' new leads.'
	from FINALLIST
	where leadType = 'new'
union all 
	select 
		'There were ' || count(*) || ' leads recycled to Core reps.'
	from FINALLIST
	where leadType = 'coreRecycled'
		and newrep <> 8207124
union all 
	select 
		case 
			when count(*) = 0 then ''
			else 'There were ' || count(*) || ' PIF leads distributed.'
		end as report
	from FINALLIST
	where newRepType = 'Core' and leadType not in ('new', 'coreRecycled')
union all 
	select 
		case 
			when max(termCompletePerc) is null then 'There were no PIF leads assigned to Core reps, so there is no max PIF % to report.' 
			else 'The oldest PIF lead was ' || max(termCompletePerc) || '% term complete.' 
		end as report
	from FINALLIST
	where newRepType = 'Core' and leadType not in ('new', 'coreRecycled')
union all 
	select 
		case 
			when avg(termCompletePerc) is null then 'There were no PIF leads assigned to Core reps, so there is no avg PIF % to report.' 
			else 'The avg PIF lead was ' || round(avg(termCompletePerc), 2) || '% term complete.' 
		end as report
	from FINALLIST
	where newRepType = 'Core' and leadType not in ('new', 'coreRecycled')	
union all
	select
		case 
			when count(*) / count(distinct newRep) <  $pifCap then 'Some PIF reps were assigned less than ' || $pifCap::varchar || ' leads.'
			when count(*) / count(distinct newRep) =  $pifCap then count(distinct newRep) || ' PIF reps received ' || $pifCap::varchar || ' leads each.'
			when count(*) / count(distinct newRep) >  $pifCap then 'Some PIF reps were assigned more than ' || $pifCap::varchar || ' leads.'
		end as report
	from FINALLIST
	where newRepType = 'PIF'
union all 
	select
		case 
			when count(*) / count(distinct newRep) <  $pifLocCap then 'Some PIF reps were assigned less than ' || $pifLocCap::varchar || ' LOC leads.'
			when count(*) / count(distinct newRep) =  $pifLocCap then count(distinct newRep) || ' PIF reps received ' || $pifLocCap::varchar || ' LOC leads each.'
			when count(*) / count(distinct newRep) >  $pifLocCap then 'Some PIF reps were assigned more than ' || $pifLocCap::varchar || ' LOC leads.'
		end as report
	from FINALLIST
	where newRepType = 'PIF LOC'
union all
	select
		case 
			when count(*) / count(distinct newRep) <  $adrCap then 'Some ADR reps were assigned less than ' || $adrCap::varchar || ' leads.'
			when count(*) / count(distinct newRep) =  $adrCap then count(distinct newRep) || ' ADR reps received ' || $adrCap::varchar || ' leads each.'
			when count(*) / count(distinct newRep) >  $adrCap then 'Some ADR reps were assigned more than ' || $adrCap::varchar || ' leads.'
		end as report
	from FINALLIST
	where newRepType = 'ADR'
union all 
	select
		case 
			when count(*) / count(distinct newRep) <  $adrLocCap then 'Some ADR reps were assigned less than ' || $adrLocCap::varchar || ' LOC leads.'
			when count(*) / count(distinct newRep) =  $adrLocCap then count(distinct newRep) || ' ADR reps received ' || $adrLocCap::varchar || ' LOC leads each.'
			when count(*) / count(distinct newRep) >  $adrLocCap then 'Some ADR reps were assigned more than ' || $adrLocCap::varchar || ' LOC leads.'
		end as report
	from FINALLIST
	where newRepType = 'ADR LOC'
union all
	select 
		count(*) || ' leads were reassigned for a 30 day cool down period.' as report
	from FINALLIST
	where newRep = 4339995 -- RMT Cool Down 
union all 
	select
		count(*) || ' LOC leads were reassigned for a 30 day cool down period.' as report
	from finalList
	where newRep = 4492237 -- RMT LOC Cool Down
union all 
	select
		count(*) || ' leads were reassigned as Marketing Owned Leads.' as report
	from finalList
	where newRep = 8207124 -- RMT MOL	
;



/*##########################################################################################################################################
#################################################      Manually Distribute PIF Leads      ##################################################
##########################################################################################################################################*/

/*with LEADS as (
		select 
			*
		from marketingOwnedLeads
		where borrowerid not in (select borrowerid from FINALLIST where newreptype in ('Core', 'PIF', 'PIF LOC', 'ADR', 'ADR LOC', 'coolDown', 'locCoolDown'))
	union all
		select 
			*
		from coolDownCompleteLeads
		where borrowerid not in (select borrowerid from FINALLIST where newreptype in ('Core', 'PIF', 'PIF LOC', 'ADR', 'ADR LOC', 'coolDown', 'locCoolDown'))
), ORDERED as (
	select 
		*,
		row_number() over (order by daysSinceRenewalEligibility) as rn
	from LEADS
)
select 
	borrowerid, businessname, currentrep, repname, termcompleteperc, daysSinceRenewalEligibility,
--	7846314 as newRep, -- Assign repID here. Leads should be evenly distributed amongst new reps (ex: rn % 0 for 2 reps)
--	'Dylan Hunter' as newRepName -- Assign repName here
	case 
		when rn % 2 = 0 then 7951491
		when rn % 2 <> 0 then 8034415
	end as newRep,
	case 
		when rn % 2 = 0 then 'Riley Warren'
		when rn % 2 <> 0 then 'Jesse Shepherd'
	end as repName
from ORDERED
where 
	rn <= 200 -- Set # of leads to be distributed
order by rn;*/



/*##########################################################################################################################################
###########################################################      NOTES     ################################################################
##########################################################################################################################################*/

-- V5 does not include MOL for LOC leads. V6 should include that.	
	-- We may need to create a LOC MOL alias for the easiest way for that that lead flow to work


-- Extend pool from which reps can pull from
	-- Include LoanMe deals, but prioritize them low with LOC leads	
	-- Add LOC leads to main distribution and prioritize them accordingly
	

------------------------- List of Renewals Reps -------------------------
/*select 
	concat(u.first, ' ', u.last) as name, LENDERUSERID, TEAMID,
	case 
		when LENDERUSERID in (select value from table(flatten(input => parse_json($TLs)))) then 1 -- Current TLs
		else 0
	end as tl, 
	case 
		when LENDERUSERID in (select value from table(flatten(input => parse_json($pifReps)))) then 1 -- Current PIF reps
		else 0
	end as pif,
	case 
		when teamid = 1373 then 1 -- Current ADRs
		else 0
	end as adr 
from teamLenderUsers t 
	join users u on t.lenderuserid = u.id
where deleted is null and status = 1
    and t.teamId in (select value from table(flatten(input => parse_json($renewalsTeams)))) -- Current renewals teams
	and lenderUserId not in (3570793, 3184535, 5376933, 4339995, 4492237, 5639073, 3547614, 4472229, 809204, 8207124) -- Exclude The Hudsons & Kris G & RMT MOL
order by name;*/