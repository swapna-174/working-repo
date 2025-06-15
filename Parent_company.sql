 WITH PT_COmpany as (
 
    SELECT distinct 
    
      CoverageDim.BenefitPlanName,
   
	  CoverageDim.ParentCompany_X
   
  FROM CoverageDim  
  /*** SB End of new join condition**/
  where CoverageDim.ParentCompany_X <>'*Not Applicable'
  ) 
 
 select  distinct PatientDim.DurableKey,EligibilityEventCoverageDim.BenefitPlanName as ODSEligiblityBenefitPlanName,EligibilityEventCoverageDimPTCOmpany.ParentCompany_X as ODSEligiblityParentCompany,
 CoveragePatientMappingFactCoverageDimX.BenefitPlanName as RTEBenefitPlanName,CoveragePatientMappingFactCoveragePTCOmpany.ParentCompany_X as RTEParentCompany,
PatientExternalCoverageFactXCoverageDim.BenefitPlanName as  HL7BenefitPlanName,PatientExternalCoverageFactXCoverageDimPTCOmpany.ParentCompany_X as HL7ParentCompany
 FROM SocialCareEpisodeFact
     LEFT OUTER JOIN PatientDim
    ON PatientDim.DurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey AND PatientDim.IsCurrent = 1
  LEFT OUTER JOIN PatientMemberAttributeDimX  on SocialCareEpisodeFact.PrimaryPatientDurableKey = PatientMemberAttributeDimX.PatientDurableKey
   left join EligibilityEventFact on EligibilityEventFact.PatientDurableKey = PatientDim.DurableKey
    LEFT OUTER JOIN CoverageDim  EligibilityEventCoverageDim
    	ON EligibilityEventCoverageDim.CoverageKey = EligibilityEventFact.BenefitPlanKey
		    LEFT OUTER JOIN PT_COmpany  EligibilityEventCoverageDimPTCOmpany
    	ON EligibilityEventCoverageDim.BenefitPlanName = EligibilityEventCoverageDimPTCOmpany.BenefitPlanName
    LEFT OUTER JOIN CoveragePatientMappingFactX
    ON CoveragePatientMappingFactX.PatientDurableDurableKey = PatientDim.DurableKey and CoveragePatientMappingFactX.PatientEffectiveToDate >= GetDate()
	LEFT OUTER JOIN CoverageDim CoveragePatientMappingFactCoverageDimX  ON CoveragePatientMappingFactCoverageDimX.CoverageKey = CoveragePatientMappingFactX.CoverageKey
	LEFT OUTER JOIN PT_COmpany CoveragePatientMappingFactCoveragePTCOmpany  ON CoveragePatientMappingFactCoverageDimX.BenefitPlanName = CoveragePatientMappingFactCoveragePTCOmpany.BenefitPlanName
  LEFT OUTER JOIN PatientExternalCoverageFactX ON PatientExternalCoverageFactX.PatientDurableKey = SocialCareEpisodeFact.PrimaryPatientDurableKey
  LEFT OUTER JOIN CoverageDim PatientExternalCoverageFactXCoverageDim  ON PatientExternalCoverageFactXCoverageDim.CoverageKey = PatientExternalCoverageFactX.ExternalCoverageKey
  	LEFT OUTER JOIN PT_COmpany PatientExternalCoverageFactXCoverageDimPTCOmpany  ON PatientExternalCoverageFactXCoverageDim.BenefitPlanName = PatientExternalCoverageFactXCoverageDimPTCOmpany.BenefitPlanName
	where DurableKey in(189695,
325164,
127429,
181548,
164021,
114824,
270042,
221104,
192827,
307401,
140852)

 


  select *from CoverageDim where CoverageKey in(212954,187933)


    SELECT distinct 
    
      CoverageDim.BenefitPlanName,
   
	  CoverageDim.ParentCompany_X
   
  FROM CoverageDim  
  /*** SB End of new join condition**/
  where CoverageDim.ParentCompany_X <>'*Not Applicable'
   

    GROUP BY CoverageDim.BenefitPlanEpicId
      ,CoverageDim.BenefitPlanName
      ,CoverageDim.PayorName
      ,CoverageDim.CoverageKey