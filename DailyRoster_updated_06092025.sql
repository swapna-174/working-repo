/* Patient discharged  on or after 12/6/24*/

WITH DailyRoster as(

select distinct ---SocialCareEpisodeFact.  SocialCareEnrollmentEncounterKey  ,  SocialCareEpisodeKey,SocialCareEpisodeFact.EpisodeKey,PatientDurableDurableKey,
[Subscriber ID]					= COALESCE(PatientExternalCoverageFactX.externalCoverageSubscriberNumber, CASE WHEN LEN(PatientSubscriberNumber) >1 then 
RIGHT(PatientSubscriberNumber, LEN(PatientSubscriberNumber) - 3) ELSE NULL END )
---,SocialCareEpisodeFact.EnrollmentDate
---,episodeenddate---testing 
,[First Name]					= PatientDim.FirstName
,[Last Name]					= PatientDim.LastName
,[Status]						= CASE WHEN trackingstatus ='Enrolled' Then 'Y' else 'N' END 
,[GroupID]					   = COALESCE(CoverageDim.SubscriberGroupNumber,ExtCoverageDim.SubscriberGroupNumber)
,[Program Enrolled]			= SocialCareEpisodeFact.type
,ExtCoverageDim.BenefitPlanName ODSBenefitPlanName
,ExtCoverageDim.PayorName  ODSPayorName
,CoverageDim.BenefitPlanName RTEBenefitPlanName
,ExtCoverageDim.BenefitPlanProductType  as ODSBenefitPlanProductType
,CoverageDim.BenefitPlanProductType  RTEBenefitPlanProductType
,episodeenddate
,episodestartdate
,PatientDim.Primarymrn

FROM SocialCareEpisodeFact  
INNER JOIN PatientDim ON PatientDim.DurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey
			AND PatientDim.IsCurrent = 1 and PatientDim.test =0 and PatientDim.Status <>'Deceased'
LEFT OUTER JOIN CoveragePatientMappingFactX	ON CoveragePatientMappingFactX.PatientDurableDurableKey = PatientDim.DurableKey
LEFT OUTER JOIN CoverageDim CoverageDim ON CoverageDim.CoverageKey = CoveragePatientMappingFactX.CoverageKey
LEFT OUTER JOIN PatientExternalCoverageFactX ON PatientExternalCoverageFactX.PatientDurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey
LEFT OUTER JOIN CoverageDim ExtCoverageDim ON ExtCoverageDim.CoverageKey = PatientExternalCoverageFactX.ExternalCoverageKey
  WHERE  

(episodeenddate  >='2024-12-06' OR  episodeenddate is null) and SocialCareEpisodeFact.enrollmentdate is not null 

--(SocialCareEpisodeFact.EnrollmentDate >= '12/06/2024' OR SocialCareEpisodeFact.EpisodeStartDate >= '12/06/2024' OR SocialCareEpisodeFact.EpisodeEndDate >= '12/06/2024')


AND SocialCareEpisodeFact.Type = 'CH Palliative Care Program'---	Patients who are currently enrolled in PC as well


/*MEDICARE */
AND (
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%ANTHEM BLUE CROSS MEDICARE ADVANTAGE%'
  OR ExtCoverageDim.PayorName Like 'ANTHEM BLUE CROSS MEDICARE ADVANTAGE%'
)
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%ANTHEM BLUE CROSS AND BLUE SHIELD MEDICARE ADVANTAGE%'
  OR ExtCoverageDim.PayorName Like 'ANTHEM BLUE CROSS AND BLUE SHIELD MEDICARE ADVANTAGE%'
)
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%Anthem GRS%'
  OR ExtCoverageDim.PayorName Like '%Anthem GRS%'
)
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MMP%'
  OR ExtCoverageDim.PayorName Like '%MMP%'
)
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%WellPoint Medicare Advantage%'
  OR ExtCoverageDim.PayorName Like '%WellPoint Medicare Advantage%'
)
/*Change in Plan name WellPoint Medicare Advantage to HEALTHY BLUE MEDICARE ADVANTAGE*/
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%HEALTHY BLUE MEDICARE ADVANTAGE%'
  OR ExtCoverageDim.PayorName Like '%HEALTHY BLUE MEDICARE ADVANTAGE%'
  )
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%Wellpoint GRS%'
  OR ExtCoverageDim.PayorName Like '%Wellpoint GRS%'
)
OR (COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%SIMPLY HEALTHCARE%'
  OR ExtCoverageDim.PayorName Like '%SIMPLY HEALTHCARE%'
)


/*MEDICAID*/

OR 
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%WELLPOINT MEDICAID%'
  OR ExtCoverageDim.PayorName Like  '%WELLPOINT MEDICAID%')
  
---INC18866032-Daily Medicaid Roster File
  OR
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - FL%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - FL%')


  OR 
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - WI%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - WI%')

    OR 
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - WA%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - WA%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - VA%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - VA%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - TX%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - TX%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - TN%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - TN%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - NY%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - NY%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - NV%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - NV%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - NJ%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - NJ%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - MO%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - MO%')

    OR 
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - MD%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - MD%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - LA%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - LA%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - IN%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - IN%')

    OR 

(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - IA%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - IA%')

    OR 
(COALESCE(ExtCoverageDim.BenefitPlanName, CoverageDim.BenefitPlanName) LIKE '%MEDICAID - GA%'
  OR ExtCoverageDim.PayorName Like  '%MEDICAID - GA%')
)
  

  ) ,
  Dailyroster_output as (
  select [Subscriber ID],
---,SocialCareEpisodeFact.EnrollmentDate
---,episodeenddate---testing 
[First Name]		
,[Last Name]	
,[Status]	
,[GroupID]	
,[Program Enrolled]
	from DailyRoster where
  (	
  [GroupID] <>  '*Not Applicable' 
				AND   ISNUMERIC(SUBSTRING ([GroupID], 1, 2)) <> 1 
				AND  [GroupID] NOT IN ('KYPDPWP0','CF030000','NYSUPWP0','MOPDPWP0','H510KYH00001KY013002','GA6306M030','OHPDPWP0','TXMMP000') 
				AND LEN(  [GroupID]) >0
 
)
)


 select distinct 'HR' [Subscriber ID],
---,SocialCareEpisodeFact.EnrollmentDate
---,episodeenddate---testing 
 FORMAT(getdate(), 'yyyyMMdd') [First Name]		
,'' [Last Name]	
,'' [Status]	
,'' [GroupID]	
,''[Program Enrolled]
	from Dailyroster_output

	UNION  ALL

  select [Subscriber ID] [Subscriber ID] ,

[First Name]		
,[Last Name]	
,[Status]	
,[GroupID]	
,[Program Enrolled]
	from Dailyroster_output

	UNION ALL


  select distinct 'TR' [Subscriber ID],
---,SocialCareEpisodeFact.EnrollmentDate
---,episodeenddate---testing 
 FORMAT(getdate(), 'yyyyMMdd') [First Name]		
,CAST( (select count(*) from ( select CASE WHEN  [Subscriber ID] is null then '19056879' else [Subscriber ID] END [Subscriber ID] ,
---,SocialCareEpisodeFact.EnrollmentDate
---,episodeenddate---testing 
[First Name]		
,[Last Name]	
,[Status]	
,[GroupID]	
,[Program Enrolled]
	from Dailyroster_output)  as a)  as varchar(10) )countnumber

,'' [Status]	
,'' [GroupID]	
,''[Program Enrolled]
	from DailyRoster