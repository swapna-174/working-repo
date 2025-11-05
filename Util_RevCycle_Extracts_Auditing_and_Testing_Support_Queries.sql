-- ETL servers:  SWBDEPICBISQL1v (DEV) ; SWBPEPICBISQL1v (PROD)

USE BI_META
GO


------------------------------------------------------------------------------------------------------------------------
-- Show the configuration rows for the 'RevCycle_PB_Extracts_to_DSS' set of extracts
------------------------------------------------------------------------------------------------------------------------
SELECT * 
FROM dbo.ACONFIG_PROCESNG_RULES
WHERE 1=1
AND TASK_GRP_NM = 'RevCycle_PB_Extracts_to_DSS'



------------------------------------------------------------------------------------------------------------------------
-- Get ALL group/batch executions
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.PROCESS_GET_ALL_GROUPS_EXEC_VW
WHERE 0=0
AND PRCS_GRP_NM = 'RevCycle_PB_Extracts_to_DSS'
AND PRCS_ID     = 7204


------------------------------------------------------------------------------------------------------------------------
-- Get ALL step executions for a given Group
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.AUDIT_GET_ALL_GROUP_STEPS_EXEC_VW
WHERE 1=1
AND PRCS_ID = 7204




------------------------------------------------------------------------------------------------------------------------
-- Get running groups
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.PROCESS_GET_RUNNING_GROUPS_VW



------------------------------------------------------------------------------------------------------------------------
-- Get failed groups
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.PROCESS_GET_FAILED_GROUPS_VW
WHERE 0=0



------------------------------------------------------------------------------------------------------------------------
-- Get SERVER roles
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.PROCESS_GET_SERVER_ROLES_VW

SELECT * 

FROM dbo.ACONFIG_PROCESNG_RULES

WHERE 1=1

AND TASK_GRP_NM = 'RevCycle_PB_Extracts_to_DSS'

ORDER BY TASK_PRIORITY_NBR, TASK_ID

SELECT * FROM dbo.AUDIT_GET_ALL_GROUP_STEPS_EXEC_VW
WHERE 1=1
AND PRCS_ID = 7238
 