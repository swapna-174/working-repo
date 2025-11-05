WITH base_population AS (
    SELECT DISTINCT
        scf.PATIENTDURABLEKEY,
        pat.PRIMARYMRN,
        pat.AgeInYears                               AS CurrentAge,
        pat.Sex                                       AS Gender,
        vitaldm.BmiValue,
        vitaldm.CurrentHeight,
        vitaldm.CurrentWeight,
        scf.BilledAsaScore                            AS ASAScore,
        vitaldm.SmokingStatus,
        CASE
            WHEN patientdx.Type1Diabetes = 1 OR patientdx.Type2Diabetes = 1 THEN 1
            ELSE 0
        END                                           AS DiabetesMellitus,
        CASE
            WHEN patientdx.Hypertension = 1 THEN 1
            ELSE 0
        END                                           AS Hypertension,
        CASE
            WHEN hyperlipidemia.patientdurablekey IS NULL THEN 0
            ELSE 1
        END                                           AS Hyperlipidemia,
        CASE
            WHEN cad.patientdurablekey IS NULL THEN 0
            ELSE 1
        END                                           AS CoronaryArteryDisease,
        pd.Name                                       AS Procedure_Name,
         CAST(dd.DateValue AS date)                               AS Surg_Date,
         CAST(admidt.DateValue AS date)                           AS Admin_Date,
        CAST(dischdt.DateValue AS date)                         AS Disch_Date,
        scf.InpatientLengthOfStayInDays               AS LOS,
        DATEDIFF('day', scf.ProcedureCompleteInstant, scf.DischargeInstant) AS PostoperativeLOS,
        scf.PatientOutOfRoomInstant,
        scf.AnesthesiaStartInstant,
        scf.AnesthesiaStopInstant,
        scf.ProcedureStartDateKey,
        scf.HasDiabetes,
        scf.HasHypertension,
        scf.HasCoronaryArteryDisease,
        scf.HasChronicKidneyDisease,
        scf.ReturnToSurgeryInDays,
        scf.AnesthesiaStartTimeOfDayKey,
        scf.AnesthesiaStopTimeOfDayKey,
        CAST(dthdate.DateValue AS date)  DeathDate,
        dep.DepartmentSpecialty                        AS PostOp_Department,
        hspadm.AdmissionConfirmationStatus             AS ReadmissionStatus,
        hspadmdd.DisplayString                         AS ReadmissionDate,
        hx.Alcohol_Comment                             AS AlcoholUse,
        scf.AdmissionDateKey,
        ReasonNotPerformed,
		vitaldm.Race ,
		  ProcedureStartInstant , 
		ProcedureCompleteInstant,
    FROM caboodle.SurgicalCaseFact AS scf
    LEFT JOIN caboodle.ProcedureDim AS pd
        ON scf.PrimaryProcedureDurableKey = pd.DurableKey
    LEFT JOIN caboodle.HospitalAdmissionFact AS hspadm
        ON scf.PatientDurableKey = hspadm.PatientDurableKey
       AND hspadm.AdmissionDateKey > scf.AdmissionDateKey
    LEFT JOIN caboodle.DateDim AS hspadmdd
        ON hspadm.AdmissionDateKey = hspadmdd.DateKey
    LEFT JOIN caboodle.DiagnosisDim AS principaldx
        ON hspadm.PrincipalProblemKey = principaldx.DiagnosisKey
    LEFT JOIN stage.rdsc.rdsc_epic_pt_vitallabresults AS vitaldm
        ON vitaldm.PatientDurableKey = scf.PatientDurableKey
    LEFT JOIN stage.rdsc.rdsc_epic_pt_diagnosislist AS patientdx
        ON patientdx.PatientDurableKey = scf.PatientDurableKey
    LEFT JOIN (
        SELECT DISTINCT def.PatientDurableKey
        FROM access.caboodle.DiagnosisEventFact AS def
        LEFT JOIN access.caboodle.DiagnosisTerminologyDim AS t
            ON def.DiagnosisKey = t.DiagnosisKey
        WHERE t.Type = 'ICD-10-CM'
          AND t.Value LIKE 'E78.%'
    ) AS hyperlipidemia
        ON hyperlipidemia.PatientDurableKey = scf.PatientDurableKey
    LEFT JOIN (
        SELECT DISTINCT def.PatientDurableKey
        FROM access.caboodle.DiagnosisEventFact AS def
        LEFT JOIN access.caboodle.DiagnosisTerminologyDim AS t
            ON def.DiagnosisKey = t.DiagnosisKey
        WHERE t.Type = 'ICD-10-CM'
          AND t.Value LIKE 'I25.%'
    ) AS cad
        ON cad.PatientDurableKey = scf.PatientDurableKey
    LEFT JOIN caboodle.ProviderDim AS prov
        ON scf.PrimarySurgeonKey = prov.ProviderKey
    LEFT JOIN caboodle.PatientDim AS pat
        ON scf.PatientDurableKey = pat.DurableKey
       AND pat.IsCurrent = 1
    LEFT JOIN clarity.Social_Hx AS hx
        ON pat.PatientEpicID = hx.Pat_ID
    LEFT JOIN caboodle.DateDim AS dd
        ON scf.SurgeryDateKey = dd.DateKey
    LEFT JOIN caboodle.DateDim AS admidt
        ON scf.AdmissionDateKey = admidt.DateKey
    LEFT JOIN caboodle.DateDim AS dischdt
        ON scf.DischargeDateKey = dischdt.DateKey
		   LEFT JOIN caboodle.DateDim AS dthdate
        ON scf.DeathDateKey = dthdate.DateKey
    LEFT JOIN caboodle.DepartmentDim AS dep
        ON scf.PostOpDepartmentKey = dep.DepartmentKey
    WHERE scf.SurgicalCaseKey > 0
      AND pat.Test = 0
      AND UPPER(prov.Name) LIKE '%MCDONALD%MICHAEL%'
      AND scf.SurgeryDateKey > 1
      AND scf.Canceled = 0
      AND LENGTH(scf.ReasonNotPerformed) = 0
),

ReadmissionDate AS (
    SELECT DISTINCT
        PrimaryMrn,
        LISTAGG(ReadmissionDate, ',')
            WITHIN GROUP (ORDER BY ReadmissionDate ASC) AS ReadmissionDate
    FROM base_population
    GROUP BY PrimaryMrn
)



,ReadmissionDx AS (
    SELECT DISTINCT
        bp.PrimaryMrn,
        LISTAGG(principaldx.Name, ', ')
            WITHIN GROUP (ORDER BY principaldx.Name ASC) AS ReadmissionDx
    FROM base_population AS bp
    LEFT JOIN caboodle.HospitalAdmissionFact AS hspadm
        ON bp.PatientDurableKey = hspadm.PatientDurableKey
       AND bp.AdmissionDateKey > hspadm.AdmissionDateKey
    LEFT JOIN caboodle.DiagnosisDim AS principaldx
        ON hspadm.PrincipalProblemKey = principaldx.DiagnosisKey
    GROUP BY bp.PrimaryMrn
),

CurrentMeds AS (
    SELECT DISTINCT
        bp.PrimaryMrn,
        LISTAGG(meds.PharmaceuticalClass, ', ')
            WITHIN GROUP (ORDER BY meds.PharmaceuticalClass ASC) AS CurrentMeds
    FROM base_population AS bp
    LEFT JOIN caboodle.MedicationEventFact AS mef
        ON bp.PatientDurableKey = mef.PatientDurableKey
    LEFT JOIN caboodle.MedicationDim AS meds
        ON mef.MedicationKey = meds.MedicationKey
    WHERE mef.StartDateKey > 1
      AND mef.EndDateKey < 1
    GROUP BY bp.PatientDurableKey, bp.PrimaryMrn
),

PreImagingData AS (
    SELECT DISTINCT
        img.PatientDurableKey,
        CAST(ExamStartInstant AS DATE) AS PreoperativeImagingDate,
        imgtxt.Narrative                 AS PreoperativeImagingResults
    FROM base_population AS bp
    LEFT JOIN caboodle.ImagingFact AS img
        ON bp.PatientDurableKey = img.PatientDurableKey
    JOIN caboodle.ImagingTextFact AS imgtxt
        ON img.ImagingKey = imgtxt.ImagingKey
        LEFT JOIN  caboodle.datedim dd  
        ON img.ExamStartDateKey  = dd.DateKey
    WHERE CAST(dd.DateValue AS DATE) <  bp.Surg_Date
      AND CAST(dd.DateValue AS DATE) >= bp.Admin_Date AND CAST(dd.DateValue AS DATE) <= bp.Admin_Date
        Qualify (row_number() over (partition by bp.PatientDurableKey order by img.ExamStartInstant desc)) =1
),

PostOpImagingData AS (
    SELECT DISTINCT
        img.PatientDurableKey,
        CAST(ExamStartInstant AS DATE) AS PostoperativeImagingDate,
        imgtxt.Narrative                 AS PostoperativeImagingResults
    FROM base_population AS bp
    LEFT JOIN caboodle.ImagingFact AS img
        ON bp.PatientDurableKey = img.PatientDurableKey
    JOIN caboodle.ImagingTextFact AS imgtxt
        ON img.ImagingKey = imgtxt.ImagingKey
    WHERE CAST(ExamStartInstant AS DATE) >  bp.Surg_Date
      AND CAST(ExamStartInstant AS DATE) <= bp.Disch_Date
      Qualify (row_number() over (partition by bp.PatientDurableKey order by img.ExamStartInstant desc)) =1
),

FollowUpImagingData AS (
    SELECT DISTINCT
        img.PatientDurableKey,
        CAST(ExamStartInstant AS DATE) AS FollowUpImagingDate,
        imgtxt.Narrative                 AS FollowUpImagingResults
    FROM base_population AS bp
    LEFT JOIN caboodle.ImagingFact AS img
        ON bp.PatientDurableKey = img.PatientDurableKey
    JOIN caboodle.ImagingTextFact AS imgtxt
        ON img.ImagingKey = imgtxt.ImagingKey
    WHERE CAST(ExamStartInstant AS DATE) >= bp.Disch_Date
),

CancerPathology AS (
    SELECT DISTINCT
        bp.PatientDurableKey,
        csf.StageDescription,
        csf.HistologicGradeG,
        csf.TumorSizeInMm,
        csf.ResidualTumorClassificationR,
        csf.LymphVascularInvasionLvi,
        csf.LymphaticVesselInvasionL
    FROM base_population AS bp
    LEFT JOIN caboodle.CancerStagingFact AS csf
        ON bp.PatientDurableKey = csf.PatientDurableKey
),

KidneyCancer AS (
    SELECT DISTINCT
        def.PatientDurableKey,
        bp.PrimaryMrn,
        CASE
            WHEN sks.value LIKE 'Q60%'    THEN 'Renal agenesis, unilateralQ'
            WHEN sks.value LIKE 'Z90%'    THEN 'Acquired absence of kidney'
            WHEN sks.value LIKE 'Z90.49%' THEN 'Acquired absence of other specified parts of urinary track'
            WHEN sks.value LIKE 'N28.89%' THEN 'Other specified disorders of kidney and ureter'
            WHEN sks.value LIKE 'Z94.0%'  THEN 'Kidney transplant status'
            ELSE NULL
        END AS SolitaryKidneyStatus,
        csf.Laterality,
        csf.TumorSizeInMm,
        csf.BodySiteOrCancerType AS TumorLocation
    FROM base_population AS bp
    LEFT JOIN access.caboodle.DiagnosisEventFact AS def
        ON bp.PatientDurableKey = def.PatientDurableKey
    LEFT JOIN access.caboodle.DiagnosisDim AS dd
        ON def.DiagnosisKey = dd.DiagnosisKey
    LEFT JOIN access.caboodle.DiagnosisTerminologyDim AS t
        ON def.DiagnosisKey = t.DiagnosisKey
    LEFT JOIN ( 
        SELECT DISTINCT
            def.PatientDurableKey,
            bp.PrimaryMrn,
            t.value
        FROM base_population AS bp
        LEFT JOIN access.caboodle.DiagnosisEventFact AS def
            ON bp.PatientDurableKey = def.PatientDurableKey
        LEFT JOIN access.caboodle.DiagnosisDim AS dd
            ON def.DiagnosisKey = dd.DiagnosisKey
        LEFT JOIN access.caboodle.DiagnosisTerminologyDim AS t
            ON def.DiagnosisKey = t.DiagnosisKey
        WHERE t.Type = 'ICD-10-CM'
          AND t.Value IN ('Q60.0', 'Z90.5', 'Z90.49', 'N28.89', 'Z94.0')
    ) AS sks
        ON sks.PatientDurableKey = def.PatientDurableKey
       AND def.PatientDurableKey IS NOT NULL
    LEFT JOIN caboodle.CancerStagingFact AS csf
        ON csf.PatientDurableKey = def.PatientDurableKey
       AND def.PatientDurableKey IS NOT NULL
    WHERE t.Type = 'ICD-10-CM'
      AND t.Value IN ('C64.1', 'C64.2', 'C64.9', 'C65.1', 'C65.2', 'C65.9')
)

,Stones AS(

 SELECT DISTINCT primarymrn, t.displaystring 
        FROM DiagnosisEventFact AS def
        LEFT JOIN DiagnosisTerminologyDim AS t
            ON def.DiagnosisKey = t.DiagnosisKey
			   LEFT JOIN Patientdim AS pd
        ON def.PatientDurableKey = pd.DurableKey
        WHERE t.Type = 'ICD-10-CM'
          AND t.Value LIKE 'Z87.442%'
		  and primarymrn <>'21614660'
		  )

,cURRENTStones AS(
		   SELECT DISTINCT primarymrn,  t.DisplayString,def.*
        FROM DiagnosisEventFact AS def
        LEFT JOIN DiagnosisTerminologyDim AS t
            ON def.DiagnosisKey = t.DiagnosisKey
			   LEFT JOIN Patientdim AS pd
        ON def.PatientDurableKey = pd.DurableKey
        WHERE t.Type = 'ICD-10-CM'
          AND t.Value  in('N20.0','N20.1','N21.0','N21.1','N20.9')
		  and def.type = 'Encounter Diagnosis'
		  )




		  * Labs */


		  /* PR-OP Hemoglobin*/


select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('Hemoglobin','BUN','Creatinine'.'eGFR' ) and Resultantinstant <= ProcedureStartInstant 
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by img.ProcedureStartInstant desc)) =1





/* POST-OP Hemoglobin*/


select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('Hemoglobin','BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by img.ProcedureCompleteInstant asc)) =1






 /* Weekly*/

/* PR-OP BUN*/


select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('week', ProcedureCompleteInstant, lrf.Resultantinstant)=1
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by lrf.Resultantinstant desc)) =1



 

 /* ! month*/



 
select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name   in('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('month', ProcedureCompleteInstant, lrf.Resultantinstant)=1
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1



 

 /* 3 month*/



 
select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name   in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('month', ProcedureCompleteInstant, lrf.Resultantinstant)=3
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1



 
/* 6 months*/



select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('month', ProcedureCompleteInstant, lrf.Resultantinstant)=6
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1


 /* 1 year*/

 
select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('year', ProcedureCompleteInstant, lrf.Resultantinstant)=1
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1

 
 /* >1 year*/
 select *From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('year', ProcedureCompleteInstant, lrf.Resultantinstant)>=1
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1


/*Date - BUN/Cr/eGFR (> 1 year postop) */


  select Resultantinstant From LabComponentResultFact lrf
join LabComponentDim lcd on lrf.labcomponentkey=lcd.labcomponentkey
where name  in ('BUN','Creatinine'.'eGFR' ) and Resultantinstant >= ProcedureCompleteInstant 
and DATEDIFF('year', ProcedureCompleteInstant, lrf.Resultantinstant)>=1
 Qualify (row_number() over (partition by lrf.PatientDurableKey order by  lrf.Resultantinstant desc)) =1






 


 **********************************************

SELECT DISTINCT 
bp.CurrentAge Age,
bp.Gender Gender,
bp.Race [Race/Ethnicity],
CurrentHeight as [Height],
CurrentWeight Weight,
bp.BmiValue BMI,
ASAScore,
bp.DiabetesMellitus [DiabetesMellitus],
bp.Hypertension [Hypertension],
bp.Hyperlipidemia,
bp.CoronaryArteryDisease,
bp.SmokingStatus,
[Alcohol Use],
 [Family History - Urologic Conditions],
[Family History - Oncologic Conditions],
[Current Medications],
bp.Admin_Date [Admission Date],
bp.Surg_Date  as [Procedure Date],
bp.Disch_Date as [Discharge Date],
bp.Procedure_Name as [Procedure Name],
bp.LOS [Total Length of Stay],
bp.PostoperativeLOS as [Postoperative Length of Stay],
''[Intraoperative Complications],
'' [Postoperative Complications],
[Clavien-Dindo Classification],
bp.PatientOutOfRoomInstant [Room In–Out Time],
bp.AnesthesiaStartInstant as [Anesthesia Start],
bp.AnesthesiaStopInstant [Anesthesia Finish],
[Estimated Blood Loss],
bp.PostOp_Department as  [Postoperative Disposition],
bp.ReadmissionStatus as [Readmission Status],
rd.ReadmissionDate [Readmission Date],
rdx.ReadmissionDx as [Readmission Reason],
[Disease Recurrence Status],
[Disease Recurrence Date],
[Survival Status],
bp.DeathDate as [Date of Death],
preImg.PreoperativeImagingDate as [ Preoperative Imaging Date],
[Preoperative Imaging Modality],
[Preoperative Imaging Results],
[Clinical Staging],
PostoperativeImagingDate [Postoperative Imaging Date],
[Postoperative Imaging Modality],
posimg.PostoperativeImagingResults [Postoperative Imaging Results],
followimg.FollowUpImagingDate [Follow Up Imaging Date],
[Follow Up Imaging Modality],
followimg.FollowUpImagingResults as [Follow Up Imaging Results],
[Tumor Pathology],
cp.StageDescription [Pathological Stage],
cp.HistologicGradeG as [Tumor Grade (WHO/ISUP)],
cp.TumorSizeInMm [Tumor Size],
cp.ResidualTumorClassificationR [R0 Resection],
cp.LymphVascularInvasionLvi [Lymph Nodes Removed],
cp.LymphaticVesselInvasionL [Lymph Nodes Involved],
[Solitary Kidney Status],
[Lesion Laterality],
[Number of Arteries],
[Number of Veins],
[Number of Lesions],
[Tumor Size],
[Tumor Location],
[Orientation],
[Percent Endophytic Component],
[Proximity to Collecting System],
[Nephrometry Score],
[Warm Ischemia Time],
[Use of Placental Grafts],
CASE WHEN Stones.PatientDurableKey is null then 'No' ELSE 'Yes' END [Previous Hx of Stones],
CASE WHEN Stones.PatientDurableKey is null then '' ELSE  st.displaystring END  [Previous Hx of Stones - Location],
[Previous Hx of Stones - Date],
[Previous Hx of Stones - Composition],
[Previous Hx of Stones - Treatment Received],
[Current Stones Total Number],
[Current Stone Size],
[Current Stone Location],
[Current Stone Composition],
[Stone Treatment],
[Stone Recurrence],
[Hydronephrosis Severity]



FROM base_population AS bp
LEFT JOIN ReadmissionDate     AS rd      ON bp.PrimaryMrn       = rd.PrimaryMrn
LEFT JOIN ReadmissionDx       AS rdx     ON bp.PrimaryMrn       = rdx.PrimaryMrn
LEFT JOIN CurrentMeds         AS currmx  ON bp.PrimaryMrn       = currmx.PrimaryMrn
LEFT JOIN PreImagingData      AS preImg  ON bp.PatientDurableKey = preImg.PatientDurableKey
LEFT JOIN PostOpImagingData   AS posimg  ON bp.PatientDurableKey = posimg.PatientDurableKey
LEFT JOIN FollowUpImagingData AS followimg ON bp.PatientDurableKey = followimg.PatientDurableKey
LEFT JOIN CancerPathology     AS cp      ON bp.PatientDurableKey = cp.PatientDurableKey
LEFT JOIN Stones       AS st      ON st.PatientDurableKey = bp.PatientDurableKey
LEFT JOIN cURRENTStones       AS cst      ON cst.PatientDurableKey = bp.PatientDurableKey

