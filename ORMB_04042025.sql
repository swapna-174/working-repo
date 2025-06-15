SET  TRANSACTION ISOLATION LEVEL SNAPSHOT;

DECLARE @RunDate as date = '20250401'--GetDate();

SELECT 
	@RunDate = IIF(@RunDate = '19000101', GETDATE(), @RunDate);


---******************Get 1 row for each benefit plan and their associated grouper values*********************


DROP TABLE IF EXISTS #BenefitPlanGroupers 
  SELECT 
      * 
INTO #BenefitPlanGroupers

  FROM (
        SELECT distinct 
      CoverageDim.BenefitPlanEpicId
      ,CoverageDim.BenefitPlanName
      ,CoverageDim.PayorName
      ---,CoverageDim.CoverageKey BenefitPlanKey
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP12' THEN CoverageSetDim.Name END) AS BillingType
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP13' THEN CoverageSetDim.Name END) AS LookbackWindow
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP14' THEN CoverageSetDim.Name END) AS BillingWindowStart
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP15' THEN CoverageSetDim.Name END) AS BillingWindowEnd
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP16' THEN CoverageSetDim.Name END) AS BillingDateType
      ,MAX(CASE WHEN CoverageSetDim.Type = 'Epic Benefit Plan Report Grouper EPP11' THEN CoverageSetDim.Name END) AS SubmissionMethod
 ,ROW_NUMBER() OVER (PARTITION BY CoverageDim.BenefitPlanName ORDER BY CoverageDim.BenefitPlanEpicId DESC) RN 
  FROM CoverageDim  
  
    join  CoverageDim CoverageDim_epp 
 /***  SB Added  the Join Condition to use the BenefitPlanEpicId ***/
  on CoverageDim.BenefitPlanEpicId  = CoverageDim_epp.BenefitPlanEpicId  and CoverageDim_epp.IsBenefitPlan=1
    JOIN Coveragesetdim Coveragesetdim ON CoverageDim_epp.CoverageKey = Coveragesetdim.CoverageKey
  /*** SB End of new join condition**/
    GROUP BY CoverageDim.BenefitPlanEpicId
      ,CoverageDim.BenefitPlanName
      ,CoverageDim.PayorName
      ,CoverageDim.CoverageKey
  )Details
  WHERE Details.BillingType IN( 'ORMB INVOICE', 'ORMB INVOICE AND CLAIM') AND RN=1



  DROP TABLE IF EXISTS #DatesGrid 
  SELECT
    @RunDate AS RunDate
    ,CurrentMonth.DateKey DateKey_RunDate
    ,CASE WHEN @RunDate <= DATEADD(D,14,CurrentMonth.MonthStartDate) THEN 1 ELSE 0 END AS Before15
    ,[Month-3].MonthStartDate [MB-3]
    ,DATEADD(D,14,[Month-3].MonthStartDate) [MB-3_15]
    --,DATEADD(D,15,[Month-3].MonthStartDate) [MB-3_16]
    ,[Month-3].MonthEndDate [ME-3]
    ,[Month-2].MonthStartDate [MB-2]
    ,DATEADD(D,14,[Month-2].MonthStartDate) [MB-2_15]
    --,DATEADD(D,15,[Month-2].MonthStartDate) [MB-2_16]
    ,[Month-2].MonthEndDate [ME-2]
    ,[Month-1].MonthStartDate [MB-1]
    ,DATEADD(D,14,[Month-1].MonthStartDate) [MB-1_15]
    --,DATEADD(D,15,[Month-1].MonthStartDate) [MB-1_16]
    ,[Month-1].MonthEndDate [ME-1]
    ,CurrentMonth.MonthStartDate MB
    ,DATEADD(D,14,CurrentMonth.MonthStartDate) [MB_15]
    ,CurrentMonth.MonthEndDate ME
    ,NextMonth.MonthStartDate [MB+1]
    ,DATEADD(D,14,NextMonth.MonthStartDate) [MB+1_15]
	into #DatesGrid
  FROM DateDim CurrentMonth
  INNER JOIN DateDim NextMonth
    ON DATEADD(M,1,CurrentMonth.MonthStartDate) = NextMonth.MonthStartDate
    AND NextMonth.DayOfMonth = 1
  INNER JOIN DateDim [Month-1]
    ON DATEADD(M,-1,CurrentMonth.MonthStartDate) = [Month-1].MonthStartDate
    AND [Month-1].DayOfMonth = 1
  INNER JOIN DateDim [Month-2]
    ON DATEADD(M,-2,CurrentMonth.MonthStartDate) = [Month-2].MonthStartDate
    AND [Month-2].DayOfMonth = 2
  INNER JOIN DateDim [Month-3]
    ON DATEADD(M,-3,CurrentMonth.MonthStartDate) = [Month-3].MonthStartDate
    AND [Month-3].DayOfMonth = 3
  WHERE CurrentMonth.DateValue = @RunDate


  DROP TABLE IF EXISTS #PlanBillingWindowDates 

  SELECT
    *
    ,CASE WHEN WithoutLookback.LookbackWindow = '1 Month' AND WithoutLookback.BillingWindowEnd LIKE '%15%' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-1, 0) + 14
          WHEN WithoutLookback.LookbackWindow = '1 Month' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate), 0)
          WHEN WithoutLookback.LookbackWindow = '2 Months' AND WithoutLookback.BillingWindowEnd LIKE '%15%' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-2, 0) + 14
          WHEN WithoutLookback.LookbackWindow = '2 Months' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-2, 0)
          WHEN WithoutLookback.LookbackWindow = '45 Days' AND WithoutLookback.BillingWindowEnd LIKE '%15%' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-1, 0)
          WHEN WithoutLookback.LookbackWindow = '45 Days' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-1, 0) + 14
          WHEN WithoutLookback.LookbackWindow = 'Modified 2 Months' AND WithoutLookback.BillingWindowEnd LIKE '%15%' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-2, 0) + 14
          WHEN WithoutLookback.LookbackWindow = 'Modified 2 Months' THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, WithoutLookback.BillingWindowEndDate)-2, 0)  ---Need to update modified 2 months based on definition
          ELSE '2099-01-01' END AS LookbackStartDate
into #PlanBillingWindowDates
  FROM (
    SELECT
      @RunDate AS ReportRunDate
      ,BenefitPlanGroupers.BenefitPlanEpicId
      ,BenefitPlanGroupers.BenefitPlanName
      ,BenefitPlanGroupers.PayorName
      ,BenefitPlanGroupers.BillingType
      ,BenefitPlanGroupers.SubmissionMethod
     -- ,BenefitPlanGroupers.BenefitPlanKey
      ,BenefitPlanGroupers.BillingWindowStart
      ,CASE WHEN BenefitPlanGroupers.BillingWindowStart = '15th of prior Month - 2' AND DatesGrid.Before15 = 1 THEN DatesGrid.[MB-2_15]  
            WHEN BenefitPlanGroupers.BillingWindowStart = '15th of prior Month - 2' AND DatesGrid.Before15 = 0 THEN DatesGrid.[MB-1_15]  
            WHEN BenefitPlanGroupers.BillingWindowStart = '1st of prior month-1' THEN DatesGrid.[MB-1]
            WHEN BenefitPlanGroupers.BillingWindowStart = '1st of prior month-2' THEN DatesGrid.[MB-2]
            ELSE '2099-01-01' END AS BillingWindowStartDate
      ,BenefitPlanGroupers.BillingWindowEnd
      ,CASE WHEN BenefitPlanGroupers.BillingWindowEnd = 'Last day of prior month' THEN DatesGrid.[ME-1]
            WHEN BenefitPlanGroupers.BillingWindowEnd = '15th of prior month-1' AND DatesGrid.Before15 = 1 THEN DatesGrid.[MB-1_15]
            WHEN BenefitPlanGroupers.BillingWindowEnd = '15th of prior month-1' AND DatesGrid.Before15 = 0 THEN DatesGrid.[MB_15]
            ELSE '2099-01-01' END AS BillingWindowEndDate
      ,BenefitPlanGroupers.BillingDateType
      ,CASE WHEN BenefitPlanGroupers.BillingDateType = '1st of Month' THEN DatesGrid.[MB+1]
            WHEN BenefitPlanGroupers.BillingDateType = '15th of month' AND DatesGrid.Before15 = 1 THEN DatesGrid.[MB_15]
            WHEN BenefitPlanGroupers.BillingDateType = '15th of month' AND DatesGrid.Before15 = 0 THEN DatesGrid.[MB+1_15]
            ELSE '2099-01-01' END AS NextBillingDate
      ,BenefitPlanGroupers.LookbackWindow
    FROM #BenefitPlanGroupers BenefitPlanGroupers
    LEFT OUTER JOIN #DatesGrid DatesGrid
      ON DatesGrid.RunDate = @RunDate
    ) WithoutLookback where billingdatetype is not null
	

---where  BenefitPlanKey=196189
DROP TABLE IF EXISTS #LatestCompletedVisitInRange 
  SELECT
    VisitFact.PatientDurableKey
    ,DateDim.DateValue AS VisitDate
	,PlanBillingWindowDates.LookbackStartDate
	,PlanBillingWindowDates.BillingWindowEndDate
	into #LatestCompletedVisitInRange
  FROM VisitFact
   LEFT OUTER JOIN patientdim on visitfact.patientdurablekey = patientdim.durablekey and patientdim.iscurrent=1
  LEFT OUTER JOIN DateDim
     ON DateDim.DateKey = VisitFact.AppointmentDateKey

	   LEFT OUTER JOIN PatientMemberAttributeDimX  on VisitFact.PatientDurableKey = PatientMemberAttributeDimX.PatientDurableKey

	    LEFT OUTER JOIN CoveragePatientMappingFactX  ON CoveragePatientMappingFactX.PatientDurableDurableKey = VisitFact.PatientDurableKey and PatientEffectiveToDate >= GetDate() 
     LEFT OUTER JOIN CoverageDim CoveragePatientMappingFactCoverageDimX  ON CoveragePatientMappingFactCoverageDimX.CoverageKey = CoveragePatientMappingFactX.CoverageKey


  LEFT OUTER JOIN PatientExternalCoverageFactX ON PatientExternalCoverageFactX.PatientDurableKey = VisitFact.PatientDurableKey
  LEFT OUTER JOIN CoverageDim PatientExternalCoverageFactXCoverageDim  ON PatientExternalCoverageFactXCoverageDim.CoverageKey = PatientExternalCoverageFactX.ExternalCoverageKey
  LEFT OUTER JOIN #BenefitPlanGroupers CoverageDim_Cvg ON CoverageDim_Cvg.BenefitPlanName = 
  COALESCE(PatientMemberAttributeDimX.HealthPlanName,CoveragePatientMappingFactCoverageDimX.BenefitPlanName, PatientExternalCoverageFactXCoverageDim.BenefitPlanName)
  LEFT OUTER JOIN #PlanBillingWindowDates PlanBillingWindowDates
		ON PlanBillingWindowDates.BenefitPlanName = CoverageDim_Cvg.BenefitPlanName

  WHERE VisitFact.AppointmentStatus  IN ('Arrived','Completed')
  AND VisitFact.Closed = 1 AND VisitFact.VisitKey > 0
  AND VisitFact.EncounterType not in ('Erroneous Encounter')
	-- Checking if DOS falls withinn Billing Lookback Period --
AND DateDim.DateValue >= CAST(PlanBillingWindowDates.LookbackStartDate as date)
 AND DateDim.DateValue <=  CAST(PlanBillingWindowDates.BillingWindowEndDate as  date)

 AND CoveragePatientMappingFactCoverageDimX.benefitplanname not like '%ANTHEM%COMMERCIAL%ASO%'
AND CoveragePatientMappingFactCoverageDimX.benefitplanname   not like '%WELLPOINT%COMMERCIAL%ASO%'
AND CoveragePatientMappingFactCoverageDimX.benefitplanname not like '%AMERIGROUP%COMMERCIAL%ASO%'
AND CoveragePatientMappingFactCoverageDimX.benefitplanname   not like '%BCBS AL MEDICARE ADVANTAGE%' 

---select * from #LatestCompletedVisitInRange

DROP TABLE IF EXISTS #InitialResults 
  SELECT
    [Record Type]					= 'D' --Y
    ,[Account Name]					= PatientDim.Name --Y
    ,[Contact ID]					= PatientDim.PrimaryMRN --	Y
    ,[Health Plan Patient Id]		= Coalesce(CoveragePatientMappingFactX.PatientSubscriberNumber,PatientExternalCoverageFactX .ExternalCoverageSubscriberNumber )
    ,[First Name]					= PatientDim.FirstName--Y
    ,[Middle Name]					= PatientDim.MiddleName --	N
    ,[Last Name]					= PatientDim.LastName --Y
    ,[Date of Birth]				= PatientDim.BirthDate --Y
    ,[Initial Active Date]			= SocialCareEpisodeFact.EnrollmentDate--Y
    ,[Discharge Date]				= SocialCareEpisodeFact.EpisodeEndDate--	N
	,[DischargeDate2]		= LEAD(SocialCareEpisodeFact.EpisodeEndDate,1,NULL) OVER (PARTITION BY PatientDim.PrimaryMrn ORDER BY SocialCareEpisodeFact.EpisodeEndDate,SocialCareEpisodeFact.MostRecentStatusChangeInstant)
    ,[Primary Insurance Number]		= Coalesce(CoveragePatientMappingFactX.PatientSubscriberNumber,PatientExternalCoverageFactX .ExternalCoverageSubscriberNumber )
    ,[Primary Insurance Name]		= ExtCoverageDim.PayorName
    ,[ODS ID]						= ExtCoverageDim.BenefitPlanEpicId --Y 
    ,[Market]						= PatientDim.StateOrProvinceAbbreviation
    ,[Program Enrolled]				= CASE WHEN SocialCareEpisodeFact.Type = 'CH Palliative Care Program' THEN 'Home Base Palliative Care' ELSE SocialCareEpisodeFact.Type END --Y
    ,[Mailing Zip/Postal Code]		= PatientDim.PostalCode--Y
    ,[Primary Contract]				= CASE WHEN ExtCoverageDim.BenefitPlanName Like '%ANTHEM%MEDICAID%' or  ExtCoverageDim.BenefitPlanName Like '%WELLPOINT%MEDICAID%'
												or ExtCoverageDim.BenefitPlanName Like '%AMERIGROUP%MEDICAID%' or ExtCoverageDim.BenefitPlanName Like '%- CA%'
												THEN CONCAT('Anthem CA Medicaid',' ',PatientDim.City,' ',ExtCoverageDim.PayorFinancialClass)

										   WHEN ExtCoverageDim.BenefitPlanName Like '%ANTHEM%MEDICAID%' or  ExtCoverageDim.BenefitPlanName Like '%WELLPOINT%MEDICAID%'
												or ExtCoverageDim.BenefitPlanName Like '%AMERIGROUP%MEDICAID%' or ExtCoverageDim.BenefitPlanName Not Like '%- CA%'
												THEN CONCAT('Anthem Medicaid',' ',PatientDim.City,' ',ExtCoverageDim.PayorFinancialClass)

										   WHEN ((ExtCoverageDim.BenefitPlanName like '%ANTHEM%COMMERCIAL%' or  ExtCoverageDim.BenefitPlanName Like '%WELLPOINT%COMMERCIAL%'
												or ExtCoverageDim.BenefitPlanName Like '%AMERIGROUP%COMMERCIAL%') and ExtCoverageDim.BenefitPlanName Not Like '%ASO%')
												THEN CONCAT('Anthem Commercial FI',' ',PatientDim.City,' ',ExtCoverageDim.PayorFinancialClass)

										   WHEN ((ExtCoverageDim.BenefitPlanName Like '%ANTHEM%' or ExtCoverageDim.BenefitPlanName Like '%WELLPOINT%'
												or ExtCoverageDim.BenefitPlanName Like 'AMERIGROUP%' or ExtCoverageDim.BenefitPlanName Like '%SIMPLY HEALTH%')
												and ExtCoverageDim.Payorfinancialclass = 'Medicare Replacement')
												THEN CONCAT('Anthem Medicare',' ',PatientDim.City,' ',ExtCoverageDim.PayorFinancialClass)
										   
										   ELSE CONCAT(ExtCoverageDim.BenefitPlanName,' ',PatientDim.City,' ',ExtCoverageDim.PayorFinancialClass)
									  END 
    ,[Patient Status]				= SocialCareEpisodeFact.OverallStatus--Y
    ,[Date Reinstated]				= LEAD(SocialCareEpisodeFact.EnrollmentDate,1,NULL) OVER (PARTITION BY PatientDim.PrimaryMrn ORDER BY SocialCareEpisodeFact.EnrollmentDate,SocialCareEpisodeFact.MostRecentStatusChangeInstant)
    ,[Last In Person Assessment Date] = Cast(LatestCompletedVisitInRange.VisitDate as date)--N
    ,[Health Plan Account ID]		= '' --N
    ,[Health Plan Segment ID]		=  '' --N
    ,[Date of Death]				= PatientDim.DeathDate --N
    ,[Primary Care Physician]		= ProviderDim_PCP.Name--N
    ,[State Code]					= PatientDim.StateOrProvinceAbbreviation--Y
    ,[Patient SFID]					= PatientDim.PatientEpicId --N
    ,[County]						= PatientDim.County--N
    ,[Initial Visit Scheduled Date] = ''  --N Confirmed Blank
    ,[Initial Visit Completed Date] = ''  --N Confirmed Blank
    ,[Second Visit Completed Date]	= '' --N Confirmed Blank
    ,[Completed Visits This Month]	= ''  --N Confirmed Blank
    ,[Member Key]					= PatientExternalCoverageFactX.ExternalCoverageSubscriberNumber 
    ,[Last Relevant Date]			= Cast(LatestCompletedVisitInRange.VisitDate as date)--N
    ,[MBI]							= ''  --This is medicare number
    ,[suffix]						= ''  --Y if Payr = MCBS MI ELSE NO
    ,[group_id]						= ''  --Y if Payro is BCBS MI ELSE NO
    ,[HPBP]							=''  --Y or N field, but dont know what drives it
    ,[member_id]					= '' --For BCBS MI, WE get Plan Suffex, For HAP,
    ,[IPA]							= '' --N
    --*****************Testing columns************
    ,[TESTING COLUMNS]				= '|||||||||||||||||||||||||||||||||||'
    ,SocialCareEpisodeFact.EpisodeEpicId
    ,PatientDim.PrimaryMRN
    ,PlanBillingWindowDates.*
    ,ROW_NUMBER() OVER (PARTITION BY PatientDim.PrimaryMRN ORDER BY SocialCareEpisodeFact.EnrollmentDate,SocialCareEpisodeFact.MostRecentStatusChangeInstant) RN 
	,COUNT(*) OVER (PARTITION BY PatientDim.PrimaryMrn) PatCount
    ,LastStatus = LEAD(SocialCareEpisodeFact.OverallStatus) OVER (PARTITION BY PatientDim.PrimaryMRN ORDER BY SocialCareEpisodeFact.EnrollmentDate,SocialCareEpisodeFact.MostRecentStatusChangeInstant) 
  into #InitialResults
  FROM SocialCareEpisodeFact
   LEFT OUTER JOIN PatientDim
    ON PatientDim.DurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey AND PatientDim.IsCurrent = 1
  LEFT OUTER JOIN PatientMemberAttributeDimX  on SocialCareEpisodeFact.PrimaryPatientDurableKey = PatientMemberAttributeDimX.PatientDurableKey
    LEFT OUTER JOIN CoveragePatientMappingFactX
    ON CoveragePatientMappingFactX.PatientDurableDurableKey = PatientDim.DurableKey and CoveragePatientMappingFactX.PatientEffectiveToDate >= GetDate()
	LEFT OUTER JOIN CoverageDim CoveragePatientMappingFactCoverageDimX  ON CoveragePatientMappingFactCoverageDimX.CoverageKey = CoveragePatientMappingFactX.CoverageKey
  LEFT OUTER JOIN PatientExternalCoverageFactX ON PatientExternalCoverageFactX.PatientDurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey
  LEFT OUTER JOIN CoverageDim PatientExternalCoverageFactXCoverageDim  ON PatientExternalCoverageFactXCoverageDim.CoverageKey = PatientExternalCoverageFactX.ExternalCoverageKey
  LEFT OUTER JOIN #LatestCompletedVisitInRange LatestCompletedVisitInRange
    ON LatestCompletedVisitInRange.PatientDurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey
 LEFT OUTER JOIN ProviderDim ProviderDim_PCP
    ON ProviderDim_PCP.ProviderKey = PatientDim.PrimaryCareProviderKey
	 LEFT OUTER JOIN #PlanBillingWindowDates PlanBillingWindowDates
    ON PlanBillingWindowDates.BenefitPlanName =  COALESCE(PatientMemberAttributeDimX.HealthPlanName,CoveragePatientMappingFactCoverageDimX.BenefitPlanName, PatientExternalCoverageFactXCoverageDim.BenefitPlanName)
	 LEFT OUTER JOIN CoverageDim  ExtCoverageDim---CoverageSubscriberNumber info
    ON ExtCoverageDim.CoverageKey = Coalesce(PatientMemberAttributeDimX.BenefitPlanKey,CoveragePatientMappingFactX.CoverageKey, PatientExternalCoverageFactX.ExternalCoverageKey)
  WHERE SocialCareEpisodeFact.SocialCareEpisodeKey > 0  --Actual Episodes
    AND SocialCareEpisodeFact.EnrollmentDate <= CAST(PlanBillingWindowDates.BillingWindowEndDate as  date)  --Enrolled before end of billing window
    AND (SocialCareEpisodeFact.EpisodeEndDate >=CAST(PlanBillingWindowDates.BillingWindowEndDate as  date) OR SocialCareEpisodeFact.EpisodeEndDate IS NULL)  --Discharged after billing window starts or still active
    AND SocialCareEpisodeFact.EnrollmentDate IS NOT NULL  -- Was at one point on service
  AND PlanBillingWindowDates.BillingType IN( 'ORMB INVOICE', 'ORMB INVOICE AND CLAIM')--Details.BillingType IN( 'ORMB INVOICE', 'ORMB INVOICE AND CLAIM')
	AND SocialCareEpisodeFact.Type = 'CH Palliative Care Program'

    AND LatestCompletedVisitInRange.VisitDate IS NOT NULL


SELECT  
  [Record Type]					
  ,[Account Name]					
  ,[Contact ID]					
  ,[Health Plan Patient Id]		
  ,[First Name]					
  ,[Middle Name]					
  ,[Last Name]					
  ,[Date of Birth]				
  ,[Initial Active Date]			
  ,[Discharge Date]
--  ,[DischargeDate2]
  ,[Primary Insurance Number]
  ,[Primary Insurance Name]		
  ,[ODS ID]						
  ,[Market]						
  ,[Program Enrolled]				
  ,[Mailing Zip/Postal Code]		
  ,[Primary Contract]	
  ,CASE WHEN [Date Reinstated] IS NOT NULL THEN LastStatus ELSE [Patient Status] END [Patient Status]				
  ,[Date Reinstated]				
  , MAX([Last In Person Assessment Date]) as [Last In Person Assessment Date] 
  ,[Health Plan Account ID]		
  ,[Health Plan Segment ID]		
  ,[Date of Death]				
  ,[Primary Care Physician]		
  ,[State Code]					
  ,[Patient SFID]					
  ,[County]						
  ,[Initial Visit Scheduled Date] 
  ,[Initial Visit Completed Date] 
  ,[Second Visit Completed Date]	
  ,[Completed Visits This Month]	
  ,[Member Key]					
  ,MAX([Last Relevant Date]) AS [Last Relevant Date]		
  ,[MBI]							
  ,[suffix]						
  ,[group_id]						
  ,[HPBP]							
  ,[member_id]					
  ,[IPA]							
FROM #InitialResults 
WHERE RN = 1 
  AND ([Discharge Date] > CAST(BillingWindowEndDate as  date) OR [Discharge Date] IS NULL OR ([Discharge Date] <= BillingWindowEndDate AND [Date Reinstated] >= [Discharge Date] and  [Date Reinstated] <= BillingWindowEndDate) AND ([DisChargeDate2] > BillingWindowEndDate OR [DisChargeDate2] IS NULL) )
  AND ([Date of Death] > CAST(BillingWindowEndDate as date) OR [Date of Death] IS NULL)

GROUP BY
  [Record Type]					
  ,[Account Name]					
  ,[Contact ID]					
  ,[Health Plan Patient Id]		
  ,[First Name]					
  ,[Middle Name]					
  ,[Last Name]					
  ,[Date of Birth]				
  ,[Initial Active Date]			
  ,[Discharge Date]	
  ,[DischargeDate2]
  ,[Primary Insurance Number]
  ,[Primary Insurance Name]		
  ,[ODS ID]						
  ,[Market]						
  ,[Program Enrolled]				
  ,[Mailing Zip/Postal Code]		
  ,[Primary Contract]
  ,[LastStatus]
  ,[Patient Status]	
  ,[Date Reinstated]				
  ,[Health Plan Account ID]		
  ,[Health Plan Segment ID]		
  ,[Date of Death]				
  ,[Primary Care Physician]		
  ,[State Code]					
  ,[Patient SFID]					
  ,[County]						
  ,[Initial Visit Scheduled Date] 
  ,[Initial Visit Completed Date] 
  ,[Second Visit Completed Date]	
  ,[Completed Visits This Month]	
  ,[Member Key]	
  ,[MBI]							
  ,[suffix]						
  ,[group_id]						
  ,[HPBP]							
  ,[member_id]					
  ,[IPA]	  