CREATE OR REPLACE TABLE stage.rdsc.RDSC_EPIC_UROLOGY_REGISTRY AS 

WITH base_population AS (

    /* ------------------------------------------------------------------------
       BASE POPULATION:
       --------------------------------------------------------------------- */
    SELECT DISTINCT
        scf.SurgicalCaseKey,
        scf.PatientDurableKey,
        pat.PrimaryMrn,
        pat.AgeInYears                   AS CurrentAge,
        pat.Sex                          AS Gender,
        v.BmiValue,
        v.CurrentHeight,
        v.CurrentWeight,
        scf.AsaRating_X               AS ASAScore,
        v.SmokingStatus,

     
        CASE 
            WHEN dx.Type1Diabetes = 1 
              OR dx.Type2Diabetes = 1 
            THEN 1 ELSE 0 
        END                              AS DiabetesMellitus,

        CASE 
            WHEN dx.Hypertension = 1 
            THEN 1 ELSE 0 
        END                              AS Hypertension,

     
        IFF(
            EXISTS (
                SELECT 1
                FROM access.caboodle.DiagnosisEventFact def
                JOIN access.caboodle.DiagnosisTerminologyDim t
                  ON def.DiagnosisKey = t.DiagnosisKey
                WHERE def.PatientDurableKey = scf.PatientDurableKey
                  AND t.Type  = 'ICD-10-CM'
                  AND t.Value LIKE 'E78.%'
            ),
            1, 0
        )                                AS Hyperlipidemia,

 
        IFF(
            EXISTS (
                SELECT 1
                FROM access.caboodle.DiagnosisEventFact def
                JOIN access.caboodle.DiagnosisTerminologyDim t
                  ON def.DiagnosisKey = t.DiagnosisKey
                WHERE def.PatientDurableKey = scf.PatientDurableKey
                  AND t.Type  = 'ICD-10-CM'
                  AND t.Value LIKE 'I25.%'
            ),
            1, 0
        )                                AS CoronaryArteryDisease,
  
        /* Procedure & dates */
        pd.Name                          AS Procedure_Name,
        CAST(dd.DateValue      AS DATE)  AS Surg_Date,
        CAST(admidt.DateValue  AS DATE)  AS Admin_Date,
        CAST(dischdt.DateValue AS DATE)  AS Disch_Date,

         DATEDIFF('day', scf.AdmissionInstant, scf.DischargeInstant)
                                           AS LOS,

        /* Post-op LOS (procedure complete → discharge) */
        DATEDIFF('day', scf.ProcedureCompleteInstant, scf.DischargeInstant)
                                         AS PostoperativeLOS,

        /* Timing fields */
        scf.PatientOutOfRoomInstant,
        scf.PatientInRoomInstant ,
        scf.AnesthesiaStartInstant,
        scf.AnesthesiaStopInstant,
        scf.SurgeryDateKey,
        scf.ProcedureStartDateKey,

        /* Comorbidity flags also present on the case fact */
        scf.HasDiabetes,
        scf.HasHypertension,
        scf.HasCoronaryArteryDisease,
        scf.HasChronicKidneyDisease,

        scf.ReturnToSurgeryInDays,
        scf.AnesthesiaStartTimeOfDayKey,
        scf.AnesthesiaStopTimeOfDayKey,

        CAST(dthdate.DateValue AS DATE)  AS DeathDate,
        dep.DepartmentSpecialty          AS PostOp_Department,

        /* Social history (latest alcohol comment) */
        hx.SMOKELESS              AS SMOKELESS,
        hx.SMOKING as SMOKING,

        scf.AdmissionDateKey,
        scf.ReasonNotPerformed,
        pat.FirstRace                      AS Race,
         pat.Ethnicity  as Ethnicity,
        scf.SurgeryEncounterKey,
        scf.ProcedureStartInstant,
        scf.ProcedureCompleteInstant,
        scf.DischargeDateKey
    FROM caboodle.SurgicalCaseFact AS scf
    JOIN caboodle.ProviderDim AS prov
      ON scf.PrimarySurgeonKey = prov.ProviderKey
    JOIN caboodle.PatientDim AS pat
      ON scf.PatientDurableKey = pat.DurableKey 
    LEFT JOIN caboodle.ProcedureDim AS pd
      ON scf.PrimaryProcedureDurableKey = pd.DurableKey
    LEFT JOIN stage.rdsc.rdsc_epic_pt_vitallabresults AS v
      ON v.PatientDurableKey = scf.PatientDurableKey
    LEFT JOIN stage.rdsc.rdsc_epic_pt_diagnosislist AS dx
      ON dx.PatientDurableKey = scf.PatientDurableKey

    /* Most recent alcohol comment per patient from Social_Hx */
    LEFT JOIN (
        SELECT distinct 
            hx.Pat_ID,
      
            ZC_SMKL.NAME SMOKELESS,
      
       ZC_SMK.NAME SMOKING,
       
            ROW_NUMBER() OVER (
                PARTITION BY hx.Pat_ID
                ORDER BY contact_date DESC
            ) AS rn
        FROM clarity.Social_Hx hx
          LEFT JOIN CLARITY.ZC_SMOKELESS_TOB_U ZC_SMKL
    ON hx.SMOKELESS_TOB_USE_C = ZC_SMKL.SMOKELESS_TOB_U_C
  LEFT JOIN CLARITY.ZC_SMOKING_TOB_USE ZC_SMK
    ON hx.SMOKING_TOB_USE_C = ZC_SMK.SMOKING_TOB_USE_C
    ) hx
      ON pat.PatientEpicID = hx.Pat_ID
     AND hx.rn = 1

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

    WHERE scf.SurgicalCaseKey > 0              -- exclude bad/zero keys
      AND pat.Test = 0                         -- exclude test patients
      -- Surgeon filter: by provider (fallback via upper-name pattern)
      AND UPPER(prov.Name) LIKE '%MCDONALD%MICHAEL%'
      AND scf.SurgeryDateKey > 1
      AND scf.ProcedureCompleteInstant IS NOT NULL  -- ensure surgery completed
      AND scf.Canceled = 0                    -- exclude cancelled cases
      AND LENGTH(scf.ReasonNotPerformed) = 0  -- exclude cases not performed
      AND pat.IsCurrent = 1                   -- only current patient dim rows
),

/* ---------------------------------------------------------------------------
   CLINICAL NOTES: operative note on surgery date & encounter
--------------------------------------------------------------------------- */
ClinicalNotes AS (


select PatientDurableKey,OPNoteTextOnDTSurgery,Note_date,ProcedureStartInstant from (
    SELECT DISTINCT
        bp.PatientDurableKey,
        cntf.Text                        AS OPNoteTextOnDTSurgery,
        CAST(cnf.ServiceInstant AS DATE) AS Note_date,
        cnf.ClinicalNoteEpicId,
        bp.ProcedureStartInstant,
         ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureStartInstant,
                    ServiceDateKey
                ORDER BY cnf.lasteditedinstant  DESC ) rn      -- latest pre-op
    FROM base_population AS bp
    LEFT JOIN caboodle.ClinicalNoteFact AS cnf
      ON bp.PatientDurableKey   = cnf.PatientDurableKey
     AND cnf.ServiceDateKey = bp.SurgeryDateKey
    LEFT JOIN caboodle.ClinicalNoteTextFact AS cntf
      ON cnf.ClinicalNoteKey    = cntf.ClinicalNoteKey
    WHERE 
     cnf.ServiceDateKey = bp.SurgeryDateKey AND 
       LOWER(cnf.Type) = 'op note' and Status ='Signed'
       and cnf.Service='Urology'
) where rn=1
) ,

/* ---------------------------------------------------------------------------
   READMISSION DATES--90 days 
--------------------------------------------------------------------------- */
ReadmitDates AS (
    SELECT
        PatientDurableKey,
        DischargeDateKey,
        LISTAGG(admt_dt, ',') WITHIN GROUP (ORDER BY admt_dt DESC) AS ReadmissionDate
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            hdd.DisplayString AS admt_dt,
            bp.DischargeDateKey,
            ROW_NUMBER() OVER (
                PARTITION BY hsp.AdmissionDateKey, bp.DischargeDateKey
                ORDER BY hsp.AdmissionDateKey DESC
            ) AS rn
        FROM base_population bp
        JOIN caboodle.HospitalAdmissionFact hsp
          ON hsp.PatientDurableKey = bp.PatientDurableKey
         AND hsp.AdmissionDateKey > bp.DischargeDateKey   -- only readmissions
        JOIN caboodle.DateDim hdd
          ON hsp.AdmissionDateKey = hdd.DateKey
          and hsp.AdmissionDateKey is not null 
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, DischargeDateKey
),

/* ---------------------------------------------------------------------------
   READMISSION PRINCIPAL DIAGNOSES
--------------------------------------------------------------------------- */
ReadmitDx AS (
    SELECT
        PatientDurableKey,
        DischargeDateKey,
        LISTAGG(dxname, ',') WITHIN GROUP (ORDER BY admt_dt DESC) AS ReadmissionDx
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            hdd.DisplayString AS admt_dt,
            bp.DischargeDateKey,
            dx.Name           AS dxname,
            ROW_NUMBER() OVER (
                PARTITION BY dx.Name             
                ORDER BY hsp.AdmissionDateKey DESC
            ) AS rn
        FROM base_population bp
        JOIN caboodle.HospitalAdmissionFact hsp
          ON hsp.PatientDurableKey = bp.PatientDurableKey
        JOIN caboodle.DateDim hdd
          ON hsp.AdmissionDateKey = hdd.DateKey
        LEFT JOIN caboodle.DiagnosisDim dx
          ON hsp.PrincipalProblemKey = dx.DiagnosisKey
        WHERE hsp.AdmissionDateKey > bp.DischargeDateKey -- post-discharge only 
        and hsp.PrincipalProblemKey is not null  
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, DischargeDateKey
),

/* ---------------------------------------------------------------------------
   READMISSION STATUS: most recent readmission status by date
--------------------------------------------------------------------------- */
ReadmitStatus AS (
    SELECT
        PatientDurableKey,
        AdmissionConfirmationStatus AS ReadmissionStatus
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            hsp.AdmissionConfirmationStatus,
            ROW_NUMBER() OVER (
                PARTITION BY bp.SurgicalCaseKey
                ORDER BY hsp.AdmissionDateKey DESC
            ) AS rn
        FROM base_population bp
        JOIN caboodle.HospitalAdmissionFact hsp
          ON hsp.PatientDurableKey = bp.PatientDurableKey
        WHERE hsp.AdmissionDateKey > bp.DischargeDateKey  -- readmissions only
    ) s
    WHERE rn = 1
),

/* ---------------------------------------------------------------------------
   PRE-OP LABS:Hemoglobin, BUN, Creatinine, eGFR
--------------------------------------------------------------------------- */
PREOPLabs AS (
    SELECT
        PatientDurableKey,
        ProcedureStartInstant,
        MAX(CASE WHEN Compname = 'Hemoglobin' THEN Value END)  AS HemoglobinPreOp,
        MAX(CASE WHEN Compname = 'BUN'        THEN Value END)  AS BUNPreOp,
        MAX(CASE WHEN Compname = 'Creatinine' THEN Value END)  AS CreatininePreOp,
        MAX(CASE WHEN Compname = 'eGFR'       THEN Value END)  AS eGFRPreOp
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lrf.Value,
            CASE 
                WHEN lcd.Name ILIKE '%Hemoglobin%' THEN 'Hemoglobin'
                WHEN lcd.Name ILIKE '%BUN'         THEN 'BUN'
                WHEN lcd.Name ILIKE 'Creatinine' THEN 'Creatinine'
                WHEN lcd.Name ILIKE '%eGFR%'       THEN 'eGFR'
                ELSE NULL
            END                                       AS Compname,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureStartInstant,
                    CASE 
                        WHEN lcd.Name ILIKE '%Hemoglobin%' THEN 'Hemoglobin'
                        WHEN lcd.Name ILIKE '%BUN'         THEN 'BUN'
                        WHEN lcd.Name ILIKE 'Creatinine' THEN 'Creatinine'
                        WHEN lcd.Name ILIKE '%eGFR%'       THEN 'eGFR'
                    END
                ORDER BY lrf.CollectionInstant DESC      -- latest pre-op
            )                                         AS rn,
            bp.ProcedureStartInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE lcd.Name ILIKE ANY ('%Hemoglobin%', '%BUN%', 'Creatinine', '%eGFR%')
          AND CAST(lrf.CollectionInstant AS DATETIME)
              <= CAST(bp.ProcedureStartInstant AS DATETIME) -- pre-op only
          AND lcd.Name IS NOT NULL
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureStartInstant
),

/* ---------------------------------------------------------------------------
   IMMEDIATE POST-OP LABS:Hemoglobin, BUN, Creatinine, eGFR

--------------------------------------------------------------------------- */
PostOPLabs AS (
    SELECT
        PatientDurableKey,
        ProcedureCompleteInstant,
        MAX(CASE WHEN Compname = 'Hemoglobin' THEN Value END)  AS HemoglobinPostOp,
        MAX(CASE WHEN Compname = 'BUN'        THEN Value END)  AS BUNPostOp,
        MAX(CASE WHEN Compname = 'Creatinine' THEN Value END)  AS CreatininePostOp,
        MAX(CASE WHEN Compname = 'eGFR'       THEN Value END)  AS eGFRPostOp
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lrf.Value,
            CASE 
                WHEN lcd.Name ILIKE '%Hemoglobin%' THEN 'Hemoglobin'
                WHEN lcd.Name ILIKE '%BUN'         THEN 'BUN'
                WHEN lcd.Name ILIKE 'Creatinine' THEN 'Creatinine'
                WHEN lcd.Name ILIKE '%eGFR%'       THEN 'eGFR'
                ELSE NULL
            END                                       AS Compname,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureCompleteInstant,
                    CASE 
                        WHEN lcd.Name ILIKE '%Hemoglobin%' THEN 'Hemoglobin'
                        WHEN lcd.Name ILIKE '%BUN'         THEN 'BUN'
                        WHEN lcd.Name ILIKE 'Creatinine' THEN 'Creatinine'
                        WHEN lcd.Name ILIKE '%eGFR%'       THEN 'eGFR'
                    END
                ORDER BY lrf.CollectionInstant ASC       -- earliest post-op
            )                                         AS rn,
            bp.ProcedureCompleteInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE lcd.Name ILIKE ANY ('%Hemoglobin%', '%BUN', 'Creatinine', '%eGFR%')
          AND CAST(lrf.CollectionInstant AS DATETIME)
              >= CAST(bp.ProcedureCompleteInstant AS DATETIME) -- post-op only
          AND lcd.Name IS NOT NULL
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureCompleteInstant
),

/* ---------------------------------------------------------------------------
   ALL POST-OP LABS:SB
   BUN/Cr/eGFR 
--------------------------------------------------------------------------- */
AllPostOpLabs AS (
    SELECT DISTINCT
        bp.PatientDurableKey,
        CASE 
            WHEN lcd.Name ILIKE '%BUN'        THEN 'BUN'
            WHEN lcd.Name ILIKE 'Creatinine' THEN 'Creatinine'
            WHEN lcd.Name ILIKE '%eGFR%'       THEN 'eGFR'
            ELSE NULL
        END                                AS ComponentName,
        lrf.Value,
        lrf.ResultInstant,
        bp.ProcedureCompleteInstant,
        lrf.CollectionInstant,
        CASE
            WHEN DATEDIFF('week',  bp.ProcedureCompleteInstant, lrf.CollectionInstant) = 1 THEN 'w1'
            WHEN DATEDIFF('month', bp.ProcedureCompleteInstant, lrf.CollectionInstant) = 1 THEN 'm1'
            WHEN DATEDIFF('month', bp.ProcedureCompleteInstant, lrf.CollectionInstant) = 3 THEN 'm3'
            WHEN DATEDIFF('month', bp.ProcedureCompleteInstant, lrf.CollectionInstant) = 6 THEN 'm6'
            WHEN DATEDIFF('year',  bp.ProcedureCompleteInstant, lrf.CollectionInstant) = 1 THEN 'y1'
            WHEN DATEDIFF('year',  bp.ProcedureCompleteInstant, lrf.CollectionInstant) >= 1 THEN 'ygt1'
        END                                AS win
    FROM base_population bp
    LEFT JOIN caboodle.LabComponentResultFact lrf
      ON bp.PatientDurableKey = lrf.PatientDurableKey
    LEFT JOIN caboodle.LabComponentDim lcd
      ON lrf.LabComponentKey = lcd.LabComponentKey
    WHERE lcd.Name ILIKE ANY ('%BUN', 'Creatinine', '%eGFR%')
      AND CAST(lrf.CollectionInstant AS DATETIME)
          >= CAST(bp.ProcedureCompleteInstant AS DATETIME)   -- post-op only
      AND lcd.Name IS NOT NULL
),

PickPerWindow AS (
    SELECT DISTINCT
        PatientDurableKey,
        ComponentName,
        win,
        Value,
        ProcedureCompleteInstant,
        ResultInstant
    FROM (
        SELECT DISTINCT
            PatientDurableKey,
            ComponentName,
            win,
            Value,
            ResultInstant,
            ProcedureCompleteInstant,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    PatientDurableKey,
                    ComponentName,
                    ProcedureCompleteInstant,
                    win
                ORDER BY CollectionInstant DESC
            ) AS rn
        FROM AllPostOpLabs
        WHERE win IS NOT NULL
    ) s
    WHERE rn = 1
),

/* ---------------------------------------------------------------------------
   POST-OP LABS (wideDates):
--------------------------------------------------------------------------- */
PostOpLabsWide AS (
    SELECT DISTINCT
        PatientDurableKey,
        ProcedureCompleteInstant,
        MAX(CASE WHEN ComponentName = 'BUN'        AND win = 'w1'   THEN Value END) AS BUN1wkPostop,
        MAX(CASE WHEN ComponentName = 'Creatinine' AND win = 'w1'   THEN Value END) AS Creatinine1wkPostop,
        MAX(CASE WHEN ComponentName = 'eGFR'       AND win = 'w1'   THEN Value END) AS eGFR1wkPostop,

        MAX(CASE WHEN ComponentName = 'BUN'        AND win = 'm1'   THEN Value END) AS BUN1moPostop,
        MAX(CASE WHEN ComponentName = 'Creatinine' AND win = 'm1'   THEN Value END) AS Creatinine1moPostop,
        MAX(CASE WHEN ComponentName = 'eGFR'       AND win = 'm1'   THEN Value END) AS eGFR1moPostop,

        MAX(CASE WHEN ComponentName = 'BUN'        AND win = 'm3'   THEN Value END) AS BUN3moPostop,
        MAX(CASE WHEN ComponentName = 'Creatinine' AND win = 'm3'   THEN Value END) AS Creatinine3moPostop,
        MAX(CASE WHEN ComponentName = 'eGFR'       AND win = 'm3'   THEN Value END) AS eGFR3moPostop,

        MAX(CASE WHEN ComponentName ILIKE '%BUN'        AND win = 'm6'   THEN Value END) AS BUN6moPostop,
        MAX(CASE WHEN ComponentName ILIKE 'Creatinine' AND win = 'm6'   THEN Value END) AS Creatinine6moPostop,
        MAX(CASE WHEN ComponentName ILIKE '%eGFR%'       AND win = 'm6'   THEN Value END) AS eGFR6moPostop,

        MAX(CASE WHEN ComponentName ILIKE '%BUN'        AND win = 'y1'   THEN Value END) AS BUN1yrPostop,
        MAX(CASE WHEN ComponentName ILIKE 'Creatinine' AND win = 'y1'   THEN Value END) AS Creatinine1yrPostop,
        MAX(CASE WHEN ComponentName ILIKE '%eGFR%'       AND win = 'y1'   THEN Value END) AS eGFR1yrPostop,

        MAX(CASE WHEN ComponentName ILIKE '%BUN'        AND win = 'ygt1' THEN Value END) AS BUNgt1yrPostop,
        MAX(CASE WHEN ComponentName ILIKE 'Creatinine' AND win = 'ygt1' THEN Value END) AS Creatininegt1yrPostop,
        MAX(CASE WHEN ComponentName ILIKE '%eGFR%'       AND win = 'ygt1' THEN Value END) AS eGFRgt1yrPostop
    FROM PickPerWindow
    GROUP BY PatientDurableKey, ProcedureCompleteInstant
),

/* ---------------------------------------------------------------------------
   POST-OP LAB DATES (>1 YEAR)
--------------------------------------------------------------------------- */
PostOpLabsDates AS (
    SELECT DISTINCT
        PatientDurableKey,
        ProcedureCompleteInstant,
        MAX(CASE WHEN ComponentName ILIKE '%BUN'        AND win = 'ygt1' THEN ResultInstant END) AS BUNgt1yrPostopDate,
        MAX(CASE WHEN ComponentName ILIKE 'Creatinine' AND win = 'ygt1' THEN ResultInstant END) AS Creatininegt1yrPostopDate,
        MAX(CASE WHEN ComponentName ILIKE '%eGFR%'       AND win = 'ygt1' THEN ResultInstant END) AS eGFRgt1yrPostopDate
    FROM PickPerWindow
    GROUP BY PatientDurableKey, ProcedureCompleteInstant
),

/* ---------------------------------------------------------------------------
   URINALYSIS PRE-OP:
--------------------------------------------------------------------------- */
UrinePreOP AS (
    SELECT DISTINCT
        PatientDurableKey,
        ProcedureStartInstant,
        MAX(CASE WHEN Compname = 'Glucoseurine'                 THEN Value END) AS Glucoseurine,
        MAX(CASE WHEN Compname = 'Clarityurine'                 THEN Value END) AS Clarityurine,
        MAX(CASE WHEN Compname = 'Ketonesurine'                 THEN Value END) AS Ketonesurine,
        MAX(CASE WHEN Compname = 'pHurine'                      THEN Value END) AS pHurine,
        MAX(CASE WHEN Compname = 'Bloodurine'                   THEN Value END) AS Bloodurine,
        MAX(CASE WHEN Compname = 'Nitritesurine'                THEN Value END) AS Nitritesurine,
        MAX(CASE WHEN Compname = 'LeukocyteEsteraseurine'       THEN Value END) AS LeukocyteEsteraseurine,
        MAX(CASE WHEN Compname = 'Proteinurine'                 THEN Value END) AS Proteinurine,
        MAX(CASE WHEN Compname = 'Bilirubinurine'               THEN Value END) AS Bilirubinurine,
        MAX(CASE WHEN Compname = 'Urobilinogenurine'            THEN Value END) AS Urobilinogenurine,
        MAX(CASE WHEN Compname = 'SpecificGravityurine'         THEN Value END) AS SpecificGravityurine,
        MAX(CASE WHEN Compname = 'RBCsurine'                    THEN Value END) AS RBCsurine,
        MAX(CASE WHEN Compname = 'WBCsurine'                    THEN Value END) AS WBCsurine,
        MAX(CASE WHEN Compname = 'Bacteriaurine'                THEN Value END) AS Bacteriaurine,
        MAX(CASE WHEN Compname = 'SquamousEpithelialCellsurine' THEN Value END) AS SquamousEpithelialCellsurine,
        MAX(CASE WHEN Compname = 'Crystalsurine'                THEN Value END) AS Crystalsurine,
        MAX(CASE WHEN Compname = 'HyalineCastsurine'            THEN Value END) AS HyalineCastsurine,
        MAX(CASE WHEN Compname = 'Mucusurine'                   THEN Value END) AS Mucusurine
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            CASE 
                WHEN lcd.Name ILIKE 'Clarity%urine%'                   THEN 'Clarityurine'
                WHEN lcd.Name ILIKE 'Glucose%urine%'                   THEN 'Glucoseurine'
                WHEN lcd.Name ILIKE 'Ketones%urine%'                   THEN 'Ketonesurine'
                WHEN lcd.Name ILIKE 'pH%urine%'                        THEN 'pHurine'
                WHEN lcd.Name ILIKE 'Blood%urine%'                     THEN 'Bloodurine'
                WHEN lcd.Name ILIKE 'Nitrites%urine%'                  THEN 'Nitritesurine'
                WHEN lcd.Name ILIKE 'Leukocyte%Esterase%urine%'        THEN 'LeukocyteEsteraseurine'
                WHEN lcd.Name ILIKE 'Protein%urine%'                   THEN 'Proteinurine'
                WHEN lcd.Name ILIKE 'Bilirubin%urine%'                 THEN 'Bilirubinurine'
                WHEN lcd.Name ILIKE 'Urobilinogen%urine%'              THEN 'Urobilinogenurine'
                WHEN lcd.Name ILIKE 'Specific%Gravity%urine%'          THEN 'SpecificGravityurine'
                WHEN lcd.Name ILIKE 'RBC%urine%'                       THEN 'RBCsurine'
                WHEN lcd.Name ILIKE 'WBC%urine%'                       THEN 'WBCsurine'
                WHEN lcd.Name ILIKE 'Bacteria%urine%'                  THEN 'Bacteriaurine'
                WHEN lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%' THEN 'SquamousEpithelialCellsurine'
                WHEN lcd.Name ILIKE 'Crystals%urine%'                  THEN 'Crystalsurine'
                WHEN lcd.Name ILIKE 'Hyaline%Casts%urine%'             THEN 'HyalineCastsurine'
                WHEN lcd.Name ILIKE 'Mucus%urine%'                     THEN 'Mucusurine'
                ELSE NULL
            END       AS Compname,
            lrf.Value AS Value,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureStartInstant,
                    CASE 
                        WHEN lcd.Name ILIKE 'Clarity%urine%'                   THEN 'Clarityurine'
                        WHEN lcd.Name ILIKE 'Glucose%urine%'                   THEN 'Glucoseurine'
                        WHEN lcd.Name ILIKE 'Ketones%urine%'                   THEN 'Ketonesurine'
                        WHEN lcd.Name ILIKE 'pH%urine%'                        THEN 'pHurine'
                        WHEN lcd.Name ILIKE 'Blood%urine%'                     THEN 'Bloodurine'
                        WHEN lcd.Name ILIKE 'Nitrites%urine%'                  THEN 'Nitritesurine'
                        WHEN lcd.Name ILIKE 'Leukocyte%Esterase%urine%'        THEN 'LeukocyteEsteraseurine'
                        WHEN lcd.Name ILIKE 'Protein%urine%'                   THEN 'Proteinurine'
                        WHEN lcd.Name ILIKE 'Bilirubin%urine%'                 THEN 'Bilirubinurine'
                        WHEN lcd.Name ILIKE 'Urobilinogen%urine%'              THEN 'Urobilinogenurine'
                        WHEN lcd.Name ILIKE 'Specific%Gravity%urine%'          THEN 'SpecificGravityurine'
                        WHEN lcd.Name ILIKE 'RBC%urine%'                       THEN 'RBCsurine'
                        WHEN lcd.Name ILIKE 'WBC%urine%'                       THEN 'WBCsurine'
                        WHEN lcd.Name ILIKE 'Bacteria%urine%'                  THEN 'Bacteriaurine'
                        WHEN lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%' THEN 'SquamousEpithelialCellsurine'
                        WHEN lcd.Name ILIKE 'Crystals%urine%'                  THEN 'Crystalsurine'
                        WHEN lcd.Name ILIKE 'Hyaline%Casts%urine%'             THEN 'HyalineCastsurine'
                        WHEN lcd.Name ILIKE 'Mucus%urine%'                     THEN 'Mucusurine'
                        ELSE NULL
                    END
                ORDER BY lrf.CollectionInstant DESC  -- latest pre-op
            )       AS rn,
            bp.ProcedureStartInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE (
            lcd.Name ILIKE 'Clarity%urine%'
            OR lcd.Name ILIKE 'Glucose%urine%'
            OR lcd.Name ILIKE 'Ketones%urine%'
            OR lcd.Name ILIKE 'pH%urine%'
            OR lcd.Name ILIKE 'Blood%urine%'
            OR lcd.Name ILIKE 'Nitrites%urine%'
            OR lcd.Name ILIKE 'Leukocyte%Esterase%urine%'
            OR lcd.Name ILIKE 'Protein%urine%'
            OR lcd.Name ILIKE 'Bilirubin%urine%'
            OR lcd.Name ILIKE 'Urobilinogen%urine%'
            OR lcd.Name ILIKE 'Specific%Gravity%urine%'
            OR lcd.Name ILIKE 'RBC%urine%'
            OR lcd.Name ILIKE 'WBC%urine%'
            OR lcd.Name ILIKE 'Bacteria%urine%'
            OR lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%'
            OR lcd.Name ILIKE 'Crystals%urine%'
            OR lcd.Name ILIKE 'Hyaline%Casts%urine%'
            OR lcd.Name ILIKE 'Mucus%urine%'
        )
          AND CAST(lrf.CollectionInstant AS DATETIME)
              <= CAST(bp.ProcedureStartInstant AS DATETIME) -- pre-op only
          AND lcd.Name IS NOT NULL
    ) u
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureStartInstant
),

/* ---------------------------------------------------------------------------
   URINALYSIS POST-OP:
--------------------------------------------------------------------------- */
UrinePostOP AS (
    SELECT DISTINCT
        PatientDurableKey,
        ProcedureCompleteInstant,
        MAX(CASE WHEN Compname = 'Glucoseurine'                 THEN Value END) AS Glucoseurine,
        MAX(CASE WHEN Compname = 'Clarityurine'                 THEN Value END) AS Clarityurine,
        MAX(CASE WHEN Compname = 'Ketonesurine'                 THEN Value END) AS Ketonesurine,
        MAX(CASE WHEN Compname = 'pHurine'                      THEN Value END) AS pHurine,
        MAX(CASE WHEN Compname = 'Bloodurine'                   THEN Value END) AS Bloodurine,
        MAX(CASE WHEN Compname = 'Nitritesurine'                THEN Value END) AS Nitritesurine,
        MAX(CASE WHEN Compname = 'LeukocyteEsteraseurine'       THEN Value END) AS LeukocyteEsteraseurine,
        MAX(CASE WHEN Compname = 'Proteinurine'                 THEN Value END) AS Proteinurine,
        MAX(CASE WHEN Compname = 'Bilirubinurine'               THEN Value END) AS Bilirubinurine,
        MAX(CASE WHEN Compname = 'Urobilinogenurine'            THEN Value END) AS Urobilinogenurine,
        MAX(CASE WHEN Compname = 'SpecificGravityurine'         THEN Value END) AS SpecificGravityurine,
        MAX(CASE WHEN Compname = 'RBCsurine'                    THEN Value END) AS RBCsurine,
        MAX(CASE WHEN Compname = 'WBCsurine'                    THEN Value END) AS WBCsurine,
        MAX(CASE WHEN Compname = 'Bacteriaurine'                THEN Value END) AS Bacteriaurine,
        MAX(CASE WHEN Compname = 'SquamousEpithelialCellsurine' THEN Value END) AS SquamousEpithelialCellsurine,
        MAX(CASE WHEN Compname = 'Crystalsurine'                THEN Value END) AS Crystalsurine,
        MAX(CASE WHEN Compname = 'HyalineCastsurine'            THEN Value END) AS HyalineCastsurine,
        MAX(CASE WHEN Compname = 'Mucusurine'                   THEN Value END) AS Mucusurine
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            CASE 
                WHEN lcd.Name ILIKE 'Clarity%urine%'                   THEN 'Clarityurine'
                WHEN lcd.Name ILIKE 'Glucose%urine%'                   THEN 'Glucoseurine'
                WHEN lcd.Name ILIKE 'Ketones%urine%'                   THEN 'Ketonesurine'
                WHEN lcd.Name ILIKE 'pH%urine%'                        THEN 'pHurine'
                WHEN lcd.Name ILIKE 'Blood%urine%'                     THEN 'Bloodurine'
                WHEN lcd.Name ILIKE 'Nitrites%urine%'                  THEN 'Nitritesurine'
                WHEN lcd.Name ILIKE 'Leukocyte%Esterase%urine%'        THEN 'LeukocyteEsteraseurine'
                WHEN lcd.Name ILIKE 'Protein%urine%'                   THEN 'Proteinurine'
                WHEN lcd.Name ILIKE 'Bilirubin%urine%'                 THEN 'Bilirubinurine'
                WHEN lcd.Name ILIKE 'Urobilinogen%urine%'              THEN 'Urobilinogenurine'
                WHEN lcd.Name ILIKE 'Specific%Gravity%urine%'          THEN 'SpecificGravityurine'
                WHEN lcd.Name ILIKE 'RBC%urine%'                       THEN 'RBCsurine'
                WHEN lcd.Name ILIKE 'WBC%urine%'                       THEN 'WBCsurine'
                WHEN lcd.Name ILIKE 'Bacteria%urine%'                  THEN 'Bacteriaurine'
                WHEN lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%' THEN 'SquamousEpithelialCellsurine'
                WHEN lcd.Name ILIKE 'Crystals%urine%'                  THEN 'Crystalsurine'
                WHEN lcd.Name ILIKE 'Hyaline%Casts%urine%'             THEN 'HyalineCastsurine'
                WHEN lcd.Name ILIKE 'Mucus%urine%'                     THEN 'Mucusurine'
                ELSE NULL
            END       AS Compname,
            lrf.Value AS Value,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureCompleteInstant,
                    CASE 
                        WHEN lcd.Name ILIKE 'Clarity%urine%'                   THEN 'Clarityurine'
                        WHEN lcd.Name ILIKE 'Glucose%urine%'                   THEN 'Glucoseurine'
                        WHEN lcd.Name ILIKE 'Ketones%urine%'                   THEN 'Ketonesurine'
                        WHEN lcd.Name ILIKE 'pH%urine%'                        THEN 'pHurine'
                        WHEN lcd.Name ILIKE 'Blood%urine%'                     THEN 'Bloodurine'
                        WHEN lcd.Name ILIKE 'Nitrites%urine%'                  THEN 'Nitritesurine'
                        WHEN lcd.Name ILIKE 'Leukocyte%Esterase%urine%'        THEN 'LeukocyteEsteraseurine'
                        WHEN lcd.Name ILIKE 'Protein%urine%'                   THEN 'Proteinurine'
                        WHEN lcd.Name ILIKE 'Bilirubin%urine%'                 THEN 'Bilirubinurine'
                        WHEN lcd.Name ILIKE 'Urobilinogen%urine%'              THEN 'Urobilinogenurine'
                        WHEN lcd.Name ILIKE 'Specific%Gravity%urine%'          THEN 'SpecificGravityurine'
                        WHEN lcd.Name ILIKE 'RBC%urine%'                       THEN 'RBCsurine'
                        WHEN lcd.Name ILIKE 'WBC%urine%'                       THEN 'WBCsurine'
                        WHEN lcd.Name ILIKE 'Bacteria%urine%'                  THEN 'Bacteriaurine'
                        WHEN lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%' THEN 'SquamousEpithelialCellsurine'
                        WHEN lcd.Name ILIKE 'Crystals%urine%'                  THEN 'Crystalsurine'
                        WHEN lcd.Name ILIKE 'Hyaline%Casts%urine%'             THEN 'HyalineCastsurine'
                        WHEN lcd.Name ILIKE 'Mucus%urine%'                     THEN 'Mucusurine'
                        ELSE NULL
                    END
                ORDER BY lrf.CollectionInstant ASC  -- earliest post-op
            )       AS rn,
            bp.ProcedureCompleteInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE (
            lcd.Name ILIKE 'Clarity%urine%'
            OR lcd.Name ILIKE 'Glucose%urine%'
            OR lcd.Name ILIKE 'Ketones%urine%'
            OR lcd.Name ILIKE 'pH%urine%'
            OR lcd.Name ILIKE 'Blood%urine%'
            OR lcd.Name ILIKE 'Nitrites%urine%'
            OR lcd.Name ILIKE 'Leukocyte%Esterase%urine%'
            OR lcd.Name ILIKE 'Protein%urine%'
            OR lcd.Name ILIKE 'Bilirubin%urine%'
            OR lcd.Name ILIKE 'Urobilinogen%urine%'
            OR lcd.Name ILIKE 'Specific%Gravity%urine%'
            OR lcd.Name ILIKE 'RBC%urine%'
            OR lcd.Name ILIKE 'WBC%urine%'
            OR lcd.Name ILIKE 'Bacteria%urine%'
            OR lcd.Name ILIKE 'Squamous%Epithelial%Cells%urine%'
            OR lcd.Name ILIKE 'Crystals%urine%'
            OR lcd.Name ILIKE 'Hyaline%Casts%urine%'
            OR lcd.Name ILIKE 'Mucus%urine%'
        )
          AND CAST(lrf.CollectionInstant AS DATETIME)
              >= CAST(bp.ProcedureCompleteInstant AS DATETIME) -- post-op only
          AND lcd.Name IS NOT NULL
    ) u
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureCompleteInstant
),


/* ---------------------------------------------------------------------------
  PRE OP URINECULTURE 
--------------------------------------------------------------------------- */
PreOPUrineCulture as (select PatientDurableKey, ProcedureStartInstant,Preopurineculture from (SELECT DISTINCT
            bp.PatientDurableKey,
            CONCAT(lcrtf.value, '   ', lcrtf.comment) as Preopurineculture,
            
            ROW_NUMBER() OVER (
                PARTITION BY 
                    bp.PatientDurableKey,
                    bp.ProcedureStartInstant
                
                   
                ORDER BY lrf.CollectionInstant desc       -- earliest post-op
            )                                         AS rn,
            bp.ProcedureStartInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
                 LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
        WHERE (lcd.Name ILIKE ANY ('%CULTURE%URINE') OR lcd.Name ILIKE ANY ('%URINE%CULTURE'))
          AND CAST(lrf.CollectionInstant AS DATETIME)
                <= CAST(bp.ProcedureStartInstant AS DATETIME)
          AND lcd.Name IS NOT NULL
          )
 WHERE rn = 1
    
    )
,
/* ---------------------------------------------------------------------------
   PATHOLOGY CASE REPORT -SB
--------------------------------------------------------------------------- */
PathologyCaseReport AS (
    SELECT
        PatientDurableKey,
        Value AS PathologyCaseReport,
        ProcedureStartInstant
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lcrtf.Value,
            lcd.Name,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey, bp.ProcedureStartInstant
                ORDER BY CollectionInstant asc
            ) AS rn,
            bp.ProcedureStartInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE lcd.Name IN ('Case Report')
          AND lcd.Type ILIKE '%Pathology%'
          AND CAST(lrf.CollectionInstant AS DATE)
              >= CAST(bp.ProcedureStartInstant AS DATE)
          AND lcd.Name IS NOT NULL
    ) s
    WHERE rn = 1
),

/* ---------------------------------------------------------------------------
   PATHOLOGY FINAL DIAGNOSIS (text)-SB
--------------------------------------------------------------------------- */
PathologyFinalReport AS (
    SELECT distinct 
        PatientDurableKey,
        Value AS PathologyFinalReport,
        ProcedureStartInstant,
        laborderepicid
        
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lcrtf.Value,
            lcd.Name,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey, bp.ProcedureStartInstant
                ORDER BY lrf.CollectionInstant asc
            ) AS rn,
            bp.ProcedureStartInstant,
            lrf.laborderepicid
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE lcd.Name IN ('Final Diagnosis')
          AND lcd.Type ILIKE '%Pathology%'
          AND CAST(lrf.CollectionInstant AS DATE)
              >= CAST(bp.ProcedureStartInstant AS DATE)
          AND lcd.Name IS NOT NULL
    ) s
    WHERE rn = 1
),

/* ---------------------------------------------------------------------------
   Synoptic DESCRIPTION (Pathology)-SB
--------------------------------------------------------------------------- */
PathologySynopticReport AS (
    SELECT distinct 
        PatientDurableKey,
      
        ProcedureStartInstant,
        laborderepicid,
       LISTAGG(SynopticReport, ',') WITHIN GROUP (ORDER BY CollectionInstant DESC) AS SynopticReport
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
           
              CONCAT (lcd.Name,'==', lcrtf.Value) SynopticReport,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey, bp.ProcedureStartInstant,lcd.Name
                ORDER BY bp.ProcedureStartInstant asc
            ) AS rn,
            bp.ProcedureStartInstant,
            CollectionInstant,
            lrf.laborderepicid
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
           JOIN PathologyFinalReport PathologyFinalReport
          ON PathologyFinalReport.laborderepicid = lrf.laborderepicid
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        WHERE lcd.Type ILIKE '%Pathology%' and subtype='Synoptic Checklist Question'
           AND  DATEDIFF('minute', bp.PROCEDURESTARTINSTANT, lrf.Collectioninstant)>=1
            and  PathologyFinalReport.ProcedureStartInstant=bp.ProcedureStartInstant
          AND lcd.Name IS NOT NULL and lcrtf.Value is not null
    ) s
    WHERE rn = 1
      GROUP BY PatientDurableKey, ProcedureStartInstant,laborderepicid
),

/* ---------------------------------------------------------------------------
   CALCULI COMPOSITION (stone analysis) -SB
--------------------------------------------------------------------------- */
CalculiComposition AS (
    SELECT
        PatientDurableKey,
        comment AS CalculiComposition,
        CollectionDate,
        ProcedureStartInstant
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lcrtf.comment,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey, bp.ProcedureStartInstant
                ORDER BY lrf.CollectionInstant asc
            ) AS rn,
            CAST(lrf.CollectionInstant AS DATE) CollectionDate,
            ProcedureStartInstant
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        JOIN caboodle.Proceduredim AS pcd
          ON lrf.Proceduredurablekey = pcd.durablekey  
        WHERE lcd.Name ILIKE 'Calculi%Composition%'
          AND pcd.Name ILIKE '%STONE%'
          AND LENGTH(comment) > 0
          AND CAST(lrf.CollectionInstant AS Datetime) >= CAST(bp.ProcedureStartInstant AS datetime)
    ) s
    WHERE rn = 1
),




/* ---------------------------------------------------------------------------
   CALCULI DESCRIPTION (stone analysis)-SB
--------------------------------------------------------------------------- */
CalculiDescription AS (
    SELECT DISTINCT
        PatientDurableKey,
        comment AS CalculiDescription,
        CollectionDate,
        ProcedureStartInstant
    FROM (
        SELECT DISTINCT
            bp.PatientDurableKey,
            lcrtf.comment,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey, bp.ProcedureStartInstant
                ORDER BY lrf.CollectionInstant asc
            ) AS rn,
            CAST(lrf.CollectionInstant AS DATE) CollectionDate,
            bp.ProcedureStartInstant---799
        FROM base_population AS bp
        LEFT JOIN caboodle.LabComponentResultFact lrf
          ON bp.PatientDurableKey = lrf.PatientDurableKey
        LEFT JOIN caboodle.LabComponentResultTextFact lcrtf
          ON lcrtf.LabComponentResultKey = lrf.LabComponentResultKey
        JOIN caboodle.LabComponentDim lcd
          ON lrf.LabComponentKey = lcd.LabComponentKey
        JOIN caboodle.Proceduredim AS pcd
          ON lrf.Proceduredurablekey = pcd.durablekey  
        WHERE lcd.Name ILIKE 'Calculi%Description%'
          AND pcd.Name ILIKE '%STONE%'
          AND LENGTH(comment) > 0
          AND CAST(lrf.CollectionInstant AS Datetime)>= CAST(bp.ProcedureStartInstant AS Datetime)
    ) s
    WHERE rn = 1
),



/* ---------------------------------------------------------------------------
   IMAGING (CT / MRI) PRE-OP SB
--------------------------------------------------------------------------- */
PreImagingData AS (
    SELECT DISTINCT
        PatientDurableKey,
        MAX(CASE WHEN ProcName = 'CT'  THEN ExamStartInstant END) AS PreOPCTImagingDate,
        MAX(CASE WHEN ProcName = 'MRI' THEN ExamStartInstant END) AS PreOPMRImagingDate,
        MAX(CASE WHEN ProcName = 'CT'  THEN Narrative       END) AS PreOPCTImagingResults,
        MAX(CASE WHEN ProcName = 'MRI' THEN Narrative       END) AS PreOPMRIImagingResults,
         MAX(CASE WHEN ProcName = 'CT' THEN Impression       END) AS PreOPCTImpression,
          MAX(CASE WHEN ProcName = 'MRI' THEN Impression       END) AS PreOPMRIImpression,
        ProcedureStartInstant 
    FROM (
        SELECT DISTINCT
            bp.ProcedureStartInstant,
            CASE 
                WHEN proceduredim.Name ILIKE 'ct%' THEN 'CT'
                ELSE 'MRI'
            END AS ProcName,
            bp.PatientDurableKey,
            imgtxt.Narrative,
            img.ExamStartInstant,
            Impression,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey,
                             bp.ProcedureStartInstant,
                             CASE WHEN proceduredim.Name ILIKE 'ct%' THEN 'CT' ELSE 'MRI' END
                ORDER BY img.ExamStartInstant DESC
            ) AS rn 
        FROM base_population AS bp
         LEFT JOIN caboodle.ImagingFact AS img
          ON bp.PatientDurableKey = img.PatientDurableKey
         LEFT JOIN caboodle.ImagingTextFact AS imgtxt
          ON img.ImagingKey = imgtxt.ImagingKey
         LEFT JOIN caboodle.ProcedureDim AS proceduredim
          ON img.FirstProcedureDurableKey = proceduredim.DurableKey
        WHERE img.ExamStartInstant <= bp.ProcedureStartInstant
          AND ((proceduredim.Name) ILIKE 'ct%' OR (proceduredim.Name) ILIKE 'mr%')
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureStartInstant
),

/* ---------------------------------------------------------------------------
   IMAGING (CT / MRI) POST-OP SB
--------------------------------------------------------------------------- */
PostOPImagingData AS (
    SELECT DISTINCT
        PatientDurableKey,
        MAX(CASE WHEN ProcName = 'CT'  THEN ExamStartInstant END) AS PostOPCTImagingDate,
        MAX(CASE WHEN ProcName = 'MRI' THEN ExamStartInstant END) AS PostOPMRImagingDate,
        MAX(CASE WHEN ProcName = 'CT'  THEN Narrative       END) AS PostOPCTImagingResults,
        MAX(CASE WHEN ProcName = 'MRI' THEN Narrative       END) AS PostOPMRIImagingResults,
             MAX(CASE WHEN ProcName = 'CT' THEN Impression       END) AS PostOPCTImpression,
          MAX(CASE WHEN ProcName = 'MRI' THEN Impression       END) AS PostOPMRIImpression,
        ProcedureCompleteInstant 
    FROM (
        SELECT DISTINCT
            bp.ProcedureCompleteInstant,
            CASE 
                WHEN proceduredim.Name ILIKE 'ct%' THEN 'CT'
                ELSE 'MRI'
            END AS ProcName,
            bp.PatientDurableKey,
            imgtxt.Narrative,
            img.ExamStartInstant,
            Impression,
            ROW_NUMBER() OVER (
                PARTITION BY bp.PatientDurableKey,
                             bp.ProcedureCompleteInstant,
                             CASE WHEN proceduredim.Name ILIKE 'ct%' THEN 'CT' ELSE 'MRI' END
                ORDER BY img.ExamStartInstant ASC
            ) AS rn
        FROM base_population AS bp
         left JOIN caboodle.ImagingFact AS img
          ON bp.PatientDurableKey = img.PatientDurableKey
          left  JOIN caboodle.ImagingTextFact AS imgtxt
          ON img.ImagingKey = imgtxt.ImagingKey
        left  JOIN caboodle.ProcedureDim AS proceduredim
          ON img.FirstProcedureDurableKey = proceduredim.DurableKey
        WHERE img.ExamStartInstant >= bp.ProcedureCompleteInstant
          AND (proceduredim.Name ILIKE 'ct%' OR proceduredim.Name ILIKE 'mr%')
    ) s
    WHERE rn = 1
    GROUP BY PatientDurableKey, ProcedureCompleteInstant
)



/* =============================================================================
   FINAL SELECT from COHORT --SB 
   ========================================================================== */
SELECT DISTINCT

    bp.PrimaryMrn                     PrimaryMrn    ,
    bp.CurrentAge                         AS "Age",
    bp.Gender                             AS "Gender",
    bp.Race                               AS "Race",
    bp.Ethnicity                          AS "Ethnicity",
    bp.CurrentHeight                      AS "Height",
    bp.CurrentWeight                      AS "Weight",
    bp.BmiValue                           AS "BMI",
    bp.ASAScore                           AS "ASA Score",
    bp.DiabetesMellitus                   AS "Diabetes Mellitus",
    bp.Hypertension                       AS "Hypertension",
    bp.Hyperlipidemia                     AS "Hyperlipidemia",
    bp.CoronaryArteryDisease              AS "Coronary Artery Disease",
    bp.SMOKING                           AS "Smoking Status",
    bp.SMOKELESS                         AS "SMOKELESS",
    bp.Admin_Date                         AS "Admission Date",
    bp.Surg_Date                        as "Proc date",
    bp.Disch_Date                         AS "Discharge Date",

    /* Surgical details & LOS */
    bp.Procedure_Name                     AS "Procedure Name",
    bp.LOS                                AS "Total Length of Stay",
    bp.PostoperativeLOS                   AS "Postoperative Length of Stay",
    bp.PatientInRoomInstant                AS "Room In Time",  
    bp.PatientOutOfRoomInstant            AS "Room Out Time",
    
    bp.AnesthesiaStartInstant             AS "Anesthesia Start",
    bp.AnesthesiaStopInstant              AS "Anesthesia Finish",

    /* Readmissions */
    rs.ReadmissionStatus                  AS "Readmission Status",
    rdd.ReadmissionDate                   AS "Readmission Date",
    rdx.ReadmissionDx                  AS "Readmission Reason",   
    /* Pre-op labs */
    preops.HemoglobinPreOp                AS "Hemoglobin Pre-op",
    preops.BUNPreOp                       AS "BUN Pre-op",
    preops.CreatininePreOp                AS "Creatinine Pre-op",
    preops.eGFRPreOp                      AS "eGFR Pre-op",

    /* Immediate post-op labs */
    postops.HemoglobinPostOp              AS "Hemoglobin Post-op",
    postops.BUNPostOp                     AS "BUN Post-op",
    postops.CreatininePostOp              AS "Creatinine Post-op",
    postops.eGFRPostOp                    AS "eGFR Post-op",
    cn.OPNoteTextOnDTSurgery              As "OP Note ",

    /* Post-op longitudinal labs by window */
    pow.BUN1wkPostop                      AS "BUN 1wk Post-op",
    pow.Creatinine1wkPostop               AS "Creatinine 1wk Post-op",
    pow.eGFR1wkPostop                     AS "eGFR 1wk Post-op",
    pow.BUN1moPostop                      AS "BUN 1mo Post-op",
    pow.Creatinine1moPostop               AS "Creatinine 1mo Post-op",
    pow.eGFR1moPostop                     AS "eGFR 1mo Post-op",
    pow.BUN3moPostop                      AS "BUN 3mo Post-op",
    pow.Creatinine3moPostop               AS "Creatinine 3mo Post-op",
    pow.eGFR3moPostop                     AS "eGFR 3mo Post-op",
    pow.BUN6moPostop                      AS "BUN 6mo Post-op",
    pow.Creatinine6moPostop               AS "Creatinine 6mo Post-op",
    pow.eGFR6moPostop                     AS "eGFR 6mo Post-op",
    pow.BUN1yrPostop                      AS "BUN 1yr Post-op",
    pow.Creatinine1yrPostop               AS "Creatinine 1yr Post-op",
    pow.eGFR1yrPostop                     AS "eGFR 1yr Post-op",
    pow.BUNgt1yrPostop                    AS "BUN >1yr Post-op",
    pow.Creatininegt1yrPostop             AS "Creatinine >1yr Post-op",
    pow.eGFRgt1yrPostop                   AS "eGFR >1yr Post-op",

    /* >1 year post-op lab dates */
    pdt.BUNgt1yrPostopDate                AS "BUN >1yr Post-op Date",
    pdt.Creatininegt1yrPostopDate         AS "Creatinine >1yr Post-op Date",
    pdt.eGFRgt1yrPostopDate               AS "eGFR >1yr Post-op Date",
    
    PreOPUC.Preopurineculture as "PreOP Urine Culture",

    /* Urinalysis pre-op */
    UrinePreOP.Clarityurine                    AS "Preop Clarity, urine",
    UrinePreOP.Glucoseurine                    AS "Preop Glucose, urine",
    UrinePreOP.Ketonesurine                    AS "Preop Ketones, urine",
    UrinePreOP.pHurine                         AS "Preop pH, urine",
    UrinePreOP.Bloodurine                      AS "Preop Blood, urine",
    UrinePreOP.Nitritesurine                   AS "Preop Nitrites, urine",
    UrinePreOP.LeukocyteEsteraseurine          AS "Preop Leukocyte Esterase, urine",
    UrinePreOP.Proteinurine                    AS "Preop Protein, urine",
    UrinePreOP.Bilirubinurine                  AS "Preop Bilirubin, urine",
    UrinePreOP.Urobilinogenurine               AS "Preop Urobilinogen, urine",
    UrinePreOP.SpecificGravityurine            AS "Preop Specific Gravity, urine",
    UrinePreOP.RBCsurine                       AS "Preop RBCs, urine",
    UrinePreOP.WBCsurine                       AS "Preop WBCs, urine",
    UrinePreOP.Bacteriaurine                   AS "Preop Bacteria, urine",
    UrinePreOP.SquamousEpithelialCellsurine    AS "Preop Squamous Epithelial Cells, urine",
    UrinePreOP.Crystalsurine                   AS "Preop Crystals, urine",
    UrinePreOP.HyalineCastsurine               AS "Preop Hyaline Casts, urine",
    UrinePreOP.Mucusurine                      AS "Preop Mucus, urine",

    /* Urinalysis post-op */
    UrinePostOP.Clarityurine                   AS "PostOP Clarity, urine",
    UrinePostOP.Glucoseurine                   AS "PostOP Glucose, urine",
    UrinePostOP.Ketonesurine                   AS "PostOP Ketones, urine",
    UrinePostOP.pHurine                        AS "PostOP pH, urine",
    UrinePostOP.Bloodurine                     AS "PostOP Blood, urine",
    UrinePostOP.Nitritesurine                  AS "PostOP Nitrites, urine",
    UrinePostOP.LeukocyteEsteraseurine         AS "PostOP Leukocyte Esterase, urine",
    UrinePostOP.Proteinurine                   AS "PostOP Protein, urine",
    UrinePostOP.Bilirubinurine                 AS "PostOP Bilirubin, urine",
    UrinePostOP.Urobilinogenurine              AS "PostOP Urobilinogen, urine",
    UrinePostOP.SpecificGravityurine           AS "PostOP Specific Gravity, urine",
    UrinePostOP.RBCsurine                      AS "PostOP RBCs, urine",
    UrinePostOP.WBCsurine                      AS "PostOP WBCs, urine",
    UrinePostOP.Bacteriaurine                  AS "PostOP Bacteria, urine",
    UrinePostOP.SquamousEpithelialCellsurine   AS "PostOP Squamous Epithelial Cells, urine",
    UrinePostOP.Crystalsurine                  AS "PostOP Crystals, urine",
    UrinePostOP.HyalineCastsurine              AS "PostOP Hyaline Casts, urine",
    UrinePostOP.Mucusurine                     AS "PostOP Mucus, urine",

    /* Pathology narrative text */
    PathologyCaseReport.PathologyCaseReport    AS "Pathology CaseReport",
    PathologyFinalReport.PathologyFinalReport  AS "Pathology FinalReport",
  PathologySynopticReport.SynopticReport   AS "Pathology SynopticReport",
    CalculiDescription.CalculiDescription       AS "Calculi Description",
    CalculiComposition.CalculiComposition       AS "Calculi Composition",

    /* Imaging */
    preopsimg.PreOPCTImagingDate               AS "Preop Imaging CT  Date",
    preopsimg.PreOPMRImagingDate               AS "Preop Imaging  MRI Date",
    preopsimg.PreOPCTImagingResults            AS "Preop CT Imaging Results",
    preopsimg.PreOPCTImpression 				AS "Preop CT Impression",

    preopsimg.PreOPMRIImagingResults           AS "Preop MRI Imaging Results",
     preopsimg.PreOPMRIImpression          AS "Preop MRI Impression ",
    
    posimg.PostOPCTImagingDate                 AS "PostOP Imaging CT  Date",
    posimg.PostOPMRImagingDate                 AS "PostOP Imaging  MRI Date",
    posimg.PostOPCTImagingResults              AS "PostOP CT Imaging Results",
    posimg.PostOPCTImpression 				   AS "PostOP CT Impression",
    posimg.PostOPMRIImagingResults             AS "PostOP MRI Imaging Results",
      posimg.PostOPMRIImpression          AS "Postop MRI Impression "
    
    
    /*KidneyCancer*/ 
    
    ---kc.SolitaryKidneyStatus as "Solitary Kidney Status"
   
    /* End of selection  */

FROM base_population AS bp
LEFT JOIN PREOPLabs           AS preops
    ON bp.PatientDurableKey         = preops.PatientDurableKey
    
   AND preops.ProcedureStartInstant = bp.ProcedureStartInstant
   
LEFT JOIN PostOPLabs          AS postops
    ON bp.PatientDurableKey         = postops.PatientDurableKey
   AND postops.ProcedureCompleteInstant = bp.ProcedureCompleteInstant
   
LEFT JOIN ReadmitDates        AS rdd
    ON rdd.PatientDurableKey        = bp.PatientDurableKey
   AND bp.DischargeDateKey          = rdd.DischargeDateKey
   
LEFT JOIN ReadmitDx           AS rdx
    ON rdx.PatientDurableKey        = bp.PatientDurableKey
   AND bp.DischargeDateKey          = rdx.DischargeDateKey
   
LEFT JOIN ReadmitStatus       AS rs
    ON rs.PatientDurableKey         = bp.PatientDurableKey
    
LEFT JOIN PostOpLabsWide      AS pow
    ON bp.PatientDurableKey         = pow.PatientDurableKey
   AND pow.ProcedureCompleteInstant = bp.ProcedureCompleteInstant
   
LEFT JOIN PostOpLabsDates     AS pdt
    ON bp.PatientDurableKey         = pdt.PatientDurableKey
   AND pdt.ProcedureCompleteInstant = bp.ProcedureCompleteInstant
   
LEFT JOIN ClinicalNotes       AS cn
    ON bp.PatientDurableKey         = cn.PatientDurableKey
   AND bp.Surg_Date                 = cn.Note_date 
    and cn.ProcedureStartInstant = bp.ProcedureStartInstant
    
LEFT JOIN UrinePreOP          AS UrinePreOP
    ON bp.PatientDurableKey         = UrinePreOP.PatientDurableKey
   AND UrinePreOP.ProcedureStartInstant = bp.ProcedureStartInstant
   
   
   LEFT JOIN PreOPUrineCulture  AS PreOPUC
    ON bp.PatientDurableKey         = PreOPUC.PatientDurableKey
      AND PreOPUC.ProcedureStartInstant = bp.ProcedureStartInstant
   
   
LEFT JOIN UrinePostOP         AS UrinePostOP
    ON bp.PatientDurableKey         = UrinePostOP.PatientDurableKey
   AND UrinePostOP.ProcedureCompleteInstant = bp.ProcedureCompleteInstant
   
   
LEFT JOIN PathologyCaseReport AS PathologyCaseReport
    ON bp.PatientDurableKey         = PathologyCaseReport.PatientDurableKey
   AND PathologyCaseReport.ProcedureStartInstant = bp.ProcedureStartInstant
  
  
   
   
LEFT JOIN PathologyFinalReport AS PathologyFinalReport
    ON bp.PatientDurableKey          = PathologyFinalReport.PatientDurableKey
   AND PathologyFinalReport.ProcedureStartInstant = bp.ProcedureStartInstant
   
    
   LEFT JOIN PathologySynopticReport AS PathologySynopticReport
    ON bp.PatientDurableKey         = PathologySynopticReport.PatientDurableKey
   AND PathologySynopticReport.ProcedureStartInstant = bp.ProcedureStartInstant
   and PathologyFinalReport.laborderepicid=PathologySynopticReport.laborderepicid
   
   
LEFT JOIN CalculiDescription AS CalculiDescription
    ON bp.PatientDurableKey          = CalculiDescription.PatientDurableKey
   AND CalculiDescription.ProcedureStartInstant = bp.ProcedureStartInstant 
LEFT JOIN CalculiComposition AS CalculiComposition
    ON bp.PatientDurableKey          = CalculiComposition.PatientDurableKey
   AND CalculiComposition.ProcedureStartInstant = bp.ProcedureStartInstant 
LEFT JOIN PreImagingData     AS preopsimg
    ON bp.PatientDurableKey         = preopsimg.PatientDurableKey 
   AND preopsimg.ProcedureStartInstant = bp.ProcedureStartInstant
   
LEFT JOIN PostOPImagingData  AS posimg
    ON bp.PatientDurableKey         = posimg.PatientDurableKey
   AND posimg.ProcedureCompleteInstant = bp.ProcedureCompleteInstant

---WHERE bp.PrimaryMrn = 11846974;   -- MRN filter---1926;

