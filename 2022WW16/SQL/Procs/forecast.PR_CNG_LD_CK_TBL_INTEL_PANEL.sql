CREATE OR ALTER PROCEDURE [forecast].[PR_CNG_LD_CK_TBL_INTEL_PANEL] (
	@in_Manual_Load CHAR(1) = 'N'
	,@in_No_of_Months INT = 6
	,@in_idsid VARCHAR(30) = ''
	,@in_SSIS_Load CHAR(1) = 'Y'
	)
	WITH EXECUTE AS 'forecast'
AS
/*****************************************************************************************************  
* Name          : PR_CNG_LD_CK_TBL_INTEL_PANEL  
* Author        : Nohin George  
* Purpose       : Load intel panel data from prod to ck tables
* View          :  
* Test			: EXEC forecast.PR_CNG_LD_CK_TBL_INTEL_PANEL 'y',0,'vchethax','y'
*******************************************************************************************************  
* Change Date   Change By		        Change DSC  
* -----------   -------------           -------------------------------------------  
* 09/09/2014    Nohin George		    Created
* 09/10/2014    Nohin George			Handled logic to run the load only on request or 22nd of each month
* 09/16/2014    Nohin George			Modified to include incremental load and SSIS package call
* 09/18/2014    Mehul Shah				Moved Email Notification inside, should run only if we are loading data
* 09/18/2014	Mehul Shah				Added @in_idsid to allow UI to pass idsid, so we can send email to that person
* 09/19/2014    Nohin George			Casting values to numeric(18,3)
* 09/22/2014    Nohin George			Modified to pass on the user id in the email.
* 09/25/2014    Nohin George			Consumer product id and product id logic added
* 10/02/2014    Nohin George			call to PR_TPCA_POST_DATA_LD_PRCS added
* 10/14/2014    Nohin George			Modified to include the Tech owner in the BCC list
* 10/15/2014    Nohin George			STG table column names modified
* 10/17/2014    Jayesh P,TCS			Renaming as per new naming convention
* 11/06/2014    Mehul Shah				Added filter of ALIGNED_TVE_DESC_VERSION = 'Published' on NAR view as per new requirement from Joe Mills (US2847)
* 01/12/2015    Jayesh P,TCS			Code freeze exception to change TPCA linked server to CNG
* 04/01/2015    Srivatsava PSV,TCS      Modified code run functionality from every month 22nd to weekly batch(saturday)
* 08/11/2015	Gopinath,Infy			Added raw attribute fields for OEM_Model_Number and System_ID for Intel Panel LAR.
* 02/03/2016    Marco Chacon			Intel Panel Load permanent fix
* 05/27/2016    Sandeep Gouda           Logic added to create columnstore index on CK_CNG_INTEL_PANEL_LAR and CK_CNG_INTEL_PANEL_NAR if its not already present
* 26/07/2016    Sandeep Gouda           Logic for checking existing columstore index is removed.
* 02/03/2017    Chitra Padmanaban       Added code for compressing table STG_LD_EXT_DATA_INTEL_PANEL_LAR
* 08/05/2021    Chethana V               SSIS package path change
* 10/02/2021    Heena Kousar H.N         Added delay and duplicate records changes     
*******************************************************************************************************/
BEGIN
	BEGIN TRY
		EXEC forecast.PR_PRINTD '--forecast.PR_CNG_LD_CK_TBL_INTEL_PANEL Started.....'
		Update forecast.cnst set CNST_VAL = 1, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
		DECLARE @l_reg_batch CHAR(1) = 'N'
			,@l_cre_dt DATETIME = GETDATE()
			,@l_cmd_syntax VARCHAR(4000) = ''
			,@l_relative_start_month INT = 0
			,@l_publish_status VARCHAR(100) = ''
			,@l_email_title_ld_status VARCHAR(200) = ''
			,@l_ld_status VARCHAR(MAX) = ''
			,@l_v2_subj_success VARCHAR(2000) = ''
			,@l_file_list VARCHAR(MAX) = 'Intel Panel Raw Data'
			,@l_dq_rcv_email_addr VARCHAR(500)
			,@l_dqmf_desc VARCHAR(2000) = ''
			,@l_SSIS_cnfg_path VARCHAR(500) = 'DTEXEC /ISServer "\SSISDB\CNG\CNG_DF_STG_SSIS_TPCA_IP\DF_STG_SSIS_TPCA_202_INTEL_PANEL.dtsx" /server FM7RDMDBCONS01.amr.corp.intel.com\RDMCON1,3180 /REP "E"'
			,@l_env VARCHAR(500) = forecast.FN_GET_ENV()
			,@l_server VARCHAR(500) = CONCAT (
				' Server: '
				,ISNULL(@@SERVERNAME, 'No Server Name')
				)
			,@l_upld_usr_nm VARCHAR(100) = ''
			,@l_upld_usr_email_addr VARCHAR(500) = ''
			,@l_status VARCHAR(100) = ''
			,@l_CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL VARCHAR(MAX) = ''
			,@l_CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL VARCHAR(MAX) = ''
			,@l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL VARCHAR(MAX) = ''
			,@l_SQL_String NVARCHAR(MAX) = ''
			,@l_dq_tech_ownr_email_addr VARCHAR(1000) = ''
		CREATE TABLE #tmpPeriod (PERIOD VARCHAR(10))
		CREATE TABLE #tmpLAR (
			ROW_ID INT
			,ALIGNED_DPM_DESC_MO VARCHAR(200)
			,ALIGNED_DPQ_DESC_QTR VARCHAR(200)
			,ALIGNED_DCO_NO_CTRY VARCHAR(200)
			,ALIGNED_DCA_DESC_CNSM_ACCT VARCHAR(200)
			,ALIGNED_DPL_NM_PLTFRM VARCHAR(200)
			,ALIGNED_DAP_DESC_AIO_PLTFRM VARCHAR(200)
			,ALIGNED_DOP_NM_OEM_PARNT VARCHAR(200)
			,ALIGNED_DOB_DESC_OEM_BRND VARCHAR(200)
			,ALIGNED_DPB_NM_PCSR_BRND VARCHAR(200)
			,ALIGNED_DPS_DESC_PCSR_SKU VARCHAR(200)
			,ALIGNED_DHS_NO_HDD_SZ_PARNT VARCHAR(200)
			,ALIGNED_DOS_NO_OPRTNG_SYS_PARNT VARCHAR(200)
			,ALIGNED_SCP_DSPLY_SZ VARCHAR(200)
			,ALIGNED_DSM_NO_SYS_MEM_PARNT VARCHAR(200)
			,ALIGNED_DTC_TOUCH_NM VARCHAR(200)
			,ALIGNED_D21_2_IN_1_NM VARCHAR(200)
			,ALIGNED_FSO_TOT_UN NUMERIC(38, 0)
			,ALIGNED_FSO_GRS_SYS_REV NUMERIC(38, 2)
			,PANEL VARCHAR(200)
			,OEM_MODEL_NUMBER VARCHAR(200)
			,SYSTEM_ID VARCHAR(200)
			,CRE_DT DATETIME
			,CONSUMER_PRODUCT_VAL VARCHAR(MAX)
			,RAW_PRODUCT_VAL VARCHAR(MAX)
			)
		CREATE TABLE #tmpNAR (
			ROW_ID INT
			,ALIGNED_DPM_DESC_MO VARCHAR(200)
			,ALIGNED_DPQ_DESC_QTR VARCHAR(200)
			,ALIGNED_DCO_NO_CTRY VARCHAR(200)
			,ALIGNED_DCA_DESC_CNSM_ACCT VARCHAR(200)
			,ALIGNED_DPL_NM_PLTFRM VARCHAR(200)
			,ALIGNED_DAP_DESC_AIO_PLTFRM VARCHAR(200)
			,ALIGNED_DOP_NM_OEM_PARNT VARCHAR(200)
			,ALIGNED_DOB_DESC_OEM_BRND VARCHAR(200)
			,ALIGNED_DPB_NM_PCSR_BRND VARCHAR(200)
			,ALIGNED_DPS_DESC_PCSR_SKU VARCHAR(200)
			,ALIGNED_TVE_DESC_VER VARCHAR(200)
			,ALIGNED_DTE_DESC_TECH VARCHAR(200)
			,ALIGNED_FSO_MDL_DESC VARCHAR(200)
			,ALIGNED_TGT_GFX_TYPE VARCHAR(200)
			,ALIGNED_D21_2_IN_1_NM VARCHAR(200)
			,ALIGNED_DTC_TOUCH_NM VARCHAR(200)
			,ALIGNED_GRS_SYS_REV NUMERIC(38, 2)
			,ALIGNED_UN NUMERIC(38, 0)
			,PANEL VARCHAR(200)
			,CRE_DT DATETIME
			,CONSUMER_PRODUCT_VAL VARCHAR(MAX)
			,RAW_PRODUCT_VAL VARCHAR(MAX)
			)
		IF @in_Manual_Load = 'N'
			SELECT @l_reg_batch = CASE 
					WHEN DATEPART(DW, GETDATE()) = 7
						THEN 'Y'
					ELSE 'N'
					END --Saturday 
		IF (
				@in_Manual_Load = 'Y'
				OR @l_reg_batch = 'Y'
				)
		BEGIN
			IF @l_reg_batch = 'Y'
				SET @l_upld_usr_nm = 'Automated Weekly Batch'
			ELSE IF COALESCE(@in_idsid, '') <> ''
			BEGIN
				SELECT @l_upld_usr_nm = cdis.ccMailName
					,@l_upld_usr_email_addr = cdis.DomainAddress
				FROM forecast.vw_STG_WorkerPublicExtended cdis(NOLOCK)
				WHERE cdis.upperIDSID = @in_idsid
			END
			SELECT @l_dq_rcv_email_addr = IIF(forecast.FN_GET_ENV() = 'CONS', DUCM.RCV_EMAIL_ADDR, DUCM.PRE_PRD_RCV_EMAIL_ADDR)
				,@l_dqmf_desc = CONCAT (
					'Intel Panel'
					,' '
					,DUC.dq_use_case_dsc
					)
				,@l_dq_tech_ownr_email_addr = CONCAT (
					DUCM.DQ_TECH_OWNR_EMAIL_ADDR
					,';'
					,ISNULL(@l_upld_usr_email_addr, '')
					)
			FROM DF_DQMF.dbo.DQ_USE_CASE_MDATA DUCM
			INNER JOIN DF_DQMF.dbo.dq_use_case DUC ON DUCM.DQ_USE_CASE_CD = DUC.DQ_USE_CASE_CD
			WHERE DUCM.DQ_USE_CASE_CD = 'DQ_CNG_STAGING_INTEL_PANEL'
			--Truncate stg tables
			TRUNCATE TABLE df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR
			TRUNCATE TABLE df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_NAR
			--Remove compression before inserts
			EXEC [FORECAST].[PR_MANAGE_TBL_COMPRESSION] 'DF_STG.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR'
				,'NONE'
				,''
			IF (@in_SSIS_Load = 'Y')
			BEGIN
				EXEC forecast.PR_PRINTD '--SSIS Load Started.....'
				SELECT @l_cmd_syntax = CONCAT (
						'forecast.PR_RUN_EXTERNAL_BATCH '
						,''''
						,@l_SSIS_cnfg_path
						,''''
						)
				EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax
					,@l_cre_dt
					,'N'
				WAITFOR DELAY '00:10:00'
				EXEC forecast.PR_PRINTD '--SSIS Load Completed.....'
			END
			--ELSE
			--BEGIN
			--	EXEC forecast.PR_PRINTD '--Insertion in STG_LD_EXT_DATA_INTEL_PANEL_LAR Started.....'
			--	INSERT INTO df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR
			--	(
			--		 ALIGNED_DPM_DESC_MO
			--		,ALIGNED_DPQ_DESC_QTR
			--		,ALIGNED_DCO_NO_CTRY
			--		,ALIGNED_DCA_DESC_CNSM_ACCT
			--		,ALIGNED_DPL_NM_PLTFRM
			--		,ALIGNED_DAP_DESC_AIO_PLTFRM
			--		,ALIGNED_DOP_NM_OEM_PARNT
			--		,ALIGNED_DOB_DESC_OEM_BRND
			--		,ALIGNED_DPB_NM_PCSR_BRND
			--		,ALIGNED_DPS_DESC_PCSR_SKU
			--		,ALIGNED_DHS_NO_HDD_SZ_PARNT
			--		,ALIGNED_DOS_NO_OPRTNG_SYS_PARNT
			--		,ALIGNED_SCP_DSPLY_SZ
			--		,ALIGNED_DSM_NO_SYS_MEM_PARNT
			--		,ALIGNED_DTC_TOUCH_NM
			--		,ALIGNED_D21_2_IN_1_NM
			--		,ALIGNED_FSO_TOT_UN
			--		,ALIGNED_FSO_GRS_SYS_REV
			--		,PANEL
			--		,OEM_MODEL_NUMBER
			--		,SYSTEM_ID
			--		)
			--	SELECT  ALIGNED_DPM_DESC_MONTH
			--		,ALIGNED_DPQ_DESC_QUARTER
			--		,ALIGNED_DCO_NO_COUNTRY
			--		,ALIGNED_DCA_DESC_CONSUMER_ACCOUNT
			--		,ALIGNED_DPL_NM_PLATFORM
			--		,ALIGNED_DAP_DESC_AIO_PLATFORM
			--		,ALIGNED_DOP_NM_OEM_PARENT
			--		,ALIGNED_DOB_DESC_OEM_BRAND
			--		,ALIGNED_DPB_NM_PROCESSOR_BRAND
			--		,ALIGNED_DPS_DESC_PROCESSOR_SKU
			--		,ALIGNED_DHS_NO_HDD_SIZE_PARENT
			--		,ALIGNED_DOS_NO_OPERATING_SYSTEM_PARENT
			--		,ALIGNED_SCP_DISPLAY_SIZE
			--		,ALIGNED_DSM_NO_SYSTEM_MEMORY_PARENT
			--		,ALIGNED_DTC_TOUCH_NM
			--		,ALIGNED_D21_2_IN_1_NM
			--		,CAST(ALIGNED_FSO_TOTAL_UNITS AS NUMERIC(18,3))
			--		,CAST(ALIGNED_FSO_GROSS_SYSTEM_REVENUE AS NUMERIC(18,3))
			--		,PANEL
			--		,OEM_MODEL_NUMBER
			--		,ISNULL(SYSTEM_ID,'')
			--	FROM CNG_NAR_RETAIL_PROD.NARRetail.dbo.vw_fact_sales_out_intel_panel_integration_lar
			--	EXEC forecast.PR_PRINTD '--Insertion in STG_LD_EXT_DATA_INTEL_PANEL_LAR Completed.....'
			--	EXEC forecast.PR_PRINTD '--Insertion in STG_LD_EXT_DATA_INTEL_PANEL_NAR Started.....'
			--	INSERT INTO df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_NAR
			--	(
			--		 ALIGNED_DPM_DESC_MO
			--		,ALIGNED_DPQ_DESC_QTR
			--		,ALIGNED_DCO_NO_CTRY
			--		,ALIGNED_DCA_DESC_CNSM_ACCT
			--		,ALIGNED_DPL_NM_PLTFRM
			--		,ALIGNED_DAP_DESC_AIO_PLTFRM
			--		,ALIGNED_DOP_NM_OEM_PARNT
			--		,ALIGNED_DOB_DESC_OEM_BRND
			--		,ALIGNED_DPB_NM_PCSR_BRND
			--		,ALIGNED_DPS_DESC_PCSR_SKU
			--		,ALIGNED_TVE_DESC_VER
			--		,ALIGNED_DTE_DESC_TECH
			--		,ALIGNED_FSO_MDL_DESC
			--		,ALIGNED_TGT_GFX_TYPE
			--		,ALIGNED_D21_2_IN_1_NM
			--		,ALIGNED_DTC_TOUCH_NM
			--		,ALIGNED_GRS_SYS_REV
			--		,ALIGNED_UN
			--		,PANEL
			--	)
			--	SELECT  ALIGNED_DPM_DESC_MONTH
			--		,ALIGNED_DPQ_DESC_QUARTER
			--		,ALIGNED_DCO_NO_COUNTRY
			--		,ALIGNED_DCA_DESC_CONSUMER_ACCOUNT
			--		,ALIGNED_DPL_NM_PLATFORM
			--		,ALIGNED_DAP_DESC_AIO_PLATFORM
			--		,ALIGNED_DOP_NM_OEM_PARENT
			--		,ALIGNED_DOB_DESC_OEM_BRAND
			--		,ALIGNED_DPB_NM_PROCESSOR_BRAND
			--		,ALIGNED_DPS_DESC_PROCESSOR_SKU
			--		,ALIGNED_TVE_DESC_VERSION
			--		,ALIGNED_DTE_DESC_TECHNOLOGY
			--		,ALIGNED_FSO_MODEL_DESCRIPTION
			--		,ALIGNED_TGT_GFX_TYPE
			--		,ALIGNED_D21_2_IN_1_NM
			--		,ALIGNED_DTC_TOUCH_NM
			--		,CAST(ALIGNED_GROSS_SYSTEM_REVENUE AS NUMERIC(18,3))
			--		,CAST(ALIGNED_UNITS AS NUMERIC(18,3))
			--		,PANEL
			--	FROM CNG_NAR_RETAIL_PROD.NARRetail.dbo.vw_fact_sales_out_intel_panel_integration_nar
			--	WHERE ALIGNED_TVE_DESC_VERSION = 'Published'
			--	EXEC forecast.PR_PRINTD '--Insertion in STG_LD_EXT_DATA_INTEL_PANEL_NAR Completed.....'
			--END
			--Add compression after insert 
			IF EXISTS (
					SELECT 1
					FROM DF_STG.FORECAST.STG_LD_EXT_DATA_INTEL_PANEL_NAR_HIST
					)
			BEGIN
				UPDATE NAR_HIST
				SET NAR_HIST.ALIGNED_DOP_NM_OEM_PARNT = NAR.ALIGNED_DOP_NM_OEM_PARNT
				FROM DF_STG.FORECAST.STG_LD_EXT_DATA_INTEL_PANEL_NAR_HIST NAR_HIST
				INNER JOIN DF_STG.FORECAST.STG_LD_EXT_DATA_INTEL_PANEL_NAR NAR ON NAR_HIST.ALIGNED_DOB_DESC_OEM_BRND=NAR.ALIGNED_DOB_DESC_OEM_BRND
			END
			IF EXISTS (
					SELECT 1
					FROM DF_STG.FORECAST.STG_LD_EXT_DATA_INTEL_PANEL_LAR_HIST
					)
			BEGIN
				UPDATE LAR_hist
				SET LAR_hist.ALIGNED_DOP_NM_OEM_PARNT = lar.ALIGNED_DOP_NM_OEM_PARNT
				FROM df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR_hist LAR_hist
				INNER JOIN df_stg.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR lar on LAR_hist.ALIGNED_DOB_DESC_OEM_BRND=lar.ALIGNED_DOB_DESC_OEM_BRND
			END
			EXEC [FORECAST].[PR_MANAGE_TBL_COMPRESSION] 'DF_STG.forecast.STG_LD_EXT_DATA_INTEL_PANEL_LAR'
				,'PAGE'
				,''
			IF (@in_No_of_Months = 0)
			BEGIN
				TRUNCATE TABLE DF_DENORM.forecast.CK_CNG_INTEL_PANEL_LAR
				TRUNCATE TABLE DF_DENORM.forecast.CK_CNG_INTEL_PANEL_NAR
				EXEC forecast.PR_PRINTD '--Truncated ck tables completed'
			END
			ELSE
			BEGIN
				SET @l_relative_start_month = (- 1) * (@in_No_of_Months - 1)
				INSERT INTO #tmpPeriod
				SELECT LINK_TO_TIME
				FROM DF_DENORM.FORECAST.CK_DIM_TM
				WHERE RELATIVE_MONTH BETWEEN @l_relative_start_month
						AND 0
					AND WORKWEEK IS NULL
				DELETE LAR
				FROM df_denorm.forecast.CK_CNG_INTEL_PANEL_LAR LAR
				INNER JOIN #tmpPeriod TM ON CONCAT (
						YEAR(LAR.ALIGNED_DPM_DESC_MONTH)
						,RIGHT(CONCAT (
								0
								,MONTH(LAR.ALIGNED_DPM_DESC_MONTH)
								,0
								), 3)
						) = TM.PERIOD
				DELETE NAR
				FROM df_denorm.forecast.CK_CNG_INTEL_PANEL_NAR NAR
				INNER JOIN #tmpPeriod TM ON CONCAT (
						YEAR(NAR.ALIGNED_DPM_DESC_MONTH)
						,RIGHT(CONCAT (
								0
								,MONTH(NAR.ALIGNED_DPM_DESC_MONTH)
								,0
								), 3)
						) = TM.PERIOD
				EXEC forecast.PR_PRINTD '--Deletion for ck tables completed'
			END
			SELECT @l_CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL = MAX(CASE 
						WHEN CNST_NM IN ('CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL')
							THEN CNST_DEF
						ELSE ''
						END)
				,@l_CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL = MAX(CASE 
						WHEN CNST_NM IN ('CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL')
							THEN CNST_DEF
						ELSE ''
						END)
				,@l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL = MAX(CASE 
						WHEN CNST_NM = 'CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL'
							THEN CNST_DEF
						ELSE ''
						END)
			FROM [forecast].[CNST] WITH (NOLOCK)
			WHERE CNST_NM IN (
					'CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL'
					,'CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL'
					,'CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL'
					)
			SET @l_CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL = CONCAT (
					',CONCAT('
					,REPLACE(@l_CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL, ',', ',''~'',')
					,') AS CONSUMER_PRODUCT_VAL'
					)
			SET @l_CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL = CONCAT (
					',CONCAT('
					,REPLACE(@l_CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL, ',', ',''~'',')
					,') AS CONSUMER_PRODUCT_VAL'
					)
			SET @l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL = CONCAT (
					',CONCAT('
					,REPLACE(@l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL, ',', ',''~'',')
					,') AS RAW_PRODUCT_VAL'
					)
			SET @l_SQL_String = CONCAT (
					'
			DECLARE @in_No_of_Months INT = '
					,@in_No_of_Months
					,'
			 INSERT INTO #tmpLAR
			 SELECT LAR.ROW_ID  
				,LAR.ALIGNED_DPM_DESC_MO
				,LAR.ALIGNED_DPQ_DESC_QTR
				,LAR.ALIGNED_DCO_NO_CTRY
				,LAR.ALIGNED_DCA_DESC_CNSM_ACCT
				,LAR.ALIGNED_DPL_NM_PLTFRM
				,LAR.ALIGNED_DAP_DESC_AIO_PLTFRM
				,LAR.ALIGNED_DOP_NM_OEM_PARNT
				,LAR.ALIGNED_DOB_DESC_OEM_BRND
				,LAR.ALIGNED_DPB_NM_PCSR_BRND
				,LAR.ALIGNED_DPS_DESC_PCSR_SKU
				,LAR.ALIGNED_DHS_NO_HDD_SZ_PARNT
				,LAR.ALIGNED_DOS_NO_OPRTNG_SYS_PARNT
				,LAR.ALIGNED_SCP_DSPLY_SZ
				,LAR.ALIGNED_DSM_NO_SYS_MEM_PARNT
				,LAR.ALIGNED_DTC_TOUCH_NM
				,LAR.ALIGNED_D21_2_IN_1_NM
				,LAR.ALIGNED_FSO_TOT_UN
				,LAR.ALIGNED_FSO_GRS_SYS_REV
				,LAR.PANEL
				,LAR.OEM_MODEL_NUMBER
				,LAR.SYSTEM_ID
				,LAR.CRE_DT
				'
					,@l_CNG_IP_LAR_MAPPING_ID_CONSUMER_PRODUCT_VAL
					,@l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL
					,'
			FROM df.forecast.VW_CNG_INTEL_PANEL_LAR_STG LAR
			LEFT JOIN #tmpPeriod TM ON CONCAT(YEAR(LAR.ALIGNED_DPM_DESC_MO),RIGHT(CONCAT(0,MONTH(LAR.ALIGNED_DPM_DESC_MO),0),3)) =TM.PERIOD
			WHERE @in_No_of_Months = 0 OR
				( @in_No_of_Months !=0 AND TM.PERIOD IS NOT NULL )
			INSERT INTO #tmpNAR
			SELECT NAR.ROW_ID  
				,NAR.ALIGNED_DPM_DESC_MO
				,NAR.ALIGNED_DPQ_DESC_QTR
				,NAR.ALIGNED_DCO_NO_CTRY
				,NAR.ALIGNED_DCA_DESC_CNSM_ACCT
				,NAR.ALIGNED_DPL_NM_PLTFRM
				,NAR.ALIGNED_DAP_DESC_AIO_PLTFRM
				,NAR.ALIGNED_DOP_NM_OEM_PARNT
				,NAR.ALIGNED_DOB_DESC_OEM_BRND
				,NAR.ALIGNED_DPB_NM_PCSR_BRND
				,NAR.ALIGNED_DPS_DESC_PCSR_SKU
				,NAR.ALIGNED_TVE_DESC_VER
				,NAR.ALIGNED_DTE_DESC_TECH
				,NAR.ALIGNED_FSO_MDL_DESC
				,NAR.ALIGNED_TGT_GFX_TYPE
				,NAR.ALIGNED_D21_2_IN_1_NM
				,NAR.ALIGNED_DTC_TOUCH_NM
				,NAR.ALIGNED_GRS_SYS_REV
				,NAR.ALIGNED_UN
				,NAR.PANEL
				,NAR.CRE_DT
				'
					,@l_CNG_IP_NAR_MAPPING_ID_CONSUMER_PRODUCT_VAL
					,@l_CNG_IP_MAPPING_ID_RAW_PRODUCT_VAL
					,'
			FROM df.forecast.VW_CNG_INTEL_PANEL_NAR_STG NAR
			LEFT JOIN #tmpPeriod TM ON CONCAT(YEAR(NAR.ALIGNED_DPM_DESC_MO),RIGHT(CONCAT(0,MONTH(NAR.ALIGNED_DPM_DESC_MO),0),3)) =TM.PERIOD
			WHERE @in_No_of_Months = 0 OR
				( @in_No_of_Months !=0 AND TM.PERIOD IS NOT NULL )'
					)
			EXEC SP_EXECUTESQL @l_SQL_String
			--EXEC forecast.PR_PRINTMAX @l_SQL_String
			IF EXISTS (
					SELECT 1
					FROM #tmpLAR
					)
			BEGIN
				EXEC forecast.PR_PRINTD '--LAR Mapping ID proc call Started.....'
				EXEC forecast.[PR_CNG_UPD_IP_DIM_ID_MAPPING] 'LAR'
				EXEC forecast.PR_PRINTD '--LAR Mapping ID proc call Ended.....'
				EXEC forecast.PR_PRINTD '--Insert into LAR ck table Started.....'
				INSERT INTO df_denorm.forecast.CK_CNG_INTEL_PANEL_LAR (
					ALIGNED_DPM_DESC_MONTH
					,ALIGNED_DPQ_DESC_QUARTER
					,ALIGNED_DCO_NO_COUNTRY
					,ALIGNED_DCA_DESC_CONSUMER_ACCOUNT
					,ALIGNED_DPL_NM_PLATFORM
					,ALIGNED_DAP_DESC_AIO_PLATFORM
					,ALIGNED_DOP_NM_OEM_PARENT
					,ALIGNED_DOB_DESC_OEM_BRAND
					,ALIGNED_DPB_NM_PROCESSOR_BRAND
					,ALIGNED_DPS_DESC_PROCESSOR_SKU
					,ALIGNED_DHS_NO_HDD_SIZE_PARENT
					,ALIGNED_DOS_NO_OPERATING_SYSTEM_PARENT
					,ALIGNED_SCP_DISPLAY_SIZE
					,ALIGNED_DSM_NO_SYSTEM_MEMORY_PARENT
					,ALIGNED_DTC_TOUCH_NM
					,ALIGNED_D21_2_IN_1_NM
					,ALIGNED_FSO_TOTAL_UNITS
					,ALIGNED_FSO_GROSS_SYSTEM_REVENUE
					,PANEL
					,OEM_MODEL_NUMBER
					,SYSTEM_ID
					,CONSUMER_PRODUCT_ID
					,RAW_PRODUCT_ID
					,CRE_DT
					)
				SELECT COALESCE(LAR.ALIGNED_DPM_DESC_MO, '')
					,COALESCE(LAR.ALIGNED_DPQ_DESC_QTR, '')
					,COALESCE(LAR.ALIGNED_DCO_NO_CTRY, '')
					,COALESCE(LAR.ALIGNED_DCA_DESC_CNSM_ACCT, '')
					,COALESCE(LAR.ALIGNED_DPL_NM_PLTFRM, '')
					,COALESCE(LAR.ALIGNED_DAP_DESC_AIO_PLTFRM, '')
					,COALESCE(LAR.ALIGNED_DOP_NM_OEM_PARNT, '')
					,COALESCE(LAR.ALIGNED_DOB_DESC_OEM_BRND, '')
					,COALESCE(LAR.ALIGNED_DPB_NM_PCSR_BRND, '')
					,COALESCE(LAR.ALIGNED_DPS_DESC_PCSR_SKU, '')
					,COALESCE(LAR.ALIGNED_DHS_NO_HDD_SZ_PARNT, '')
					,COALESCE(LAR.ALIGNED_DOS_NO_OPRTNG_SYS_PARNT, '')
					,COALESCE(LAR.ALIGNED_SCP_DSPLY_SZ, '')
					,COALESCE(LAR.ALIGNED_DSM_NO_SYS_MEM_PARNT, '')
					,COALESCE(LAR.ALIGNED_DTC_TOUCH_NM, '')
					,COALESCE(LAR.ALIGNED_D21_2_IN_1_NM, '')
					,LAR.ALIGNED_FSO_TOT_UN
					,LAR.ALIGNED_FSO_GRS_SYS_REV
					,COALESCE(LAR.PANEL, '')
					,COALESCE(LAR.OEM_MODEL_NUMBER, '')
					,COALESCE(LAR.SYSTEM_ID, '')
					,CPRD.CONSUMER_PRODUCT_ID
					,PRD.RAW_PRODUCT_ID
					,LAR.CRE_DT
				FROM #tmpLAR LAR
				LEFT JOIN DF_DENORM.forecast.CK_CNG_DIM_ID_MAPPING CPRD(NOLOCK) ON CPRD.CONCAT_VAL = LAR.CONSUMER_PRODUCT_VAL
					AND CPRD.VNDR = 'IP_LAR'
				LEFT JOIN DF_DENORM.forecast.CK_CNG_DIM_ID_MAPPING PRD(NOLOCK) ON PRD.CONCAT_VAL = LAR.RAW_PRODUCT_VAL
					AND PRD.VNDR = 'IP'
				EXEC forecast.PR_PRINTD '--Insert into LAR ck table Completed.....'
			END
			EXEC DF.FORECAST.PR_CRE_UPD_COLUMNSTORE_INDEX 'DF_DENORM.forecast.CK_CNG_INTEL_PANEL_LAR'
			EXEC forecast.PR_PRINTD '--Columnstore index created on CK_CNG_INTEL_PANEL_LAR .....'
			IF EXISTS (
					SELECT 1
					FROM #tmpNAR
					)
			BEGIN
				EXEC forecast.PR_PRINTD '--NAR Mapping ID proc call Started.....'
				EXEC forecast.[PR_CNG_UPD_IP_DIM_ID_MAPPING] 'NAR'
				EXEC forecast.PR_PRINTD '--NAR Mapping ID proc call Ended.....'
				EXEC forecast.PR_PRINTD '--Insert into NAR ck table Started.....'
				INSERT INTO df_denorm.forecast.CK_CNG_INTEL_PANEL_NAR (
					ALIGNED_DPM_DESC_MONTH
					,ALIGNED_DPQ_DESC_QUARTER
					,ALIGNED_DCO_NO_COUNTRY
					,ALIGNED_DCA_DESC_CONSUMER_ACCOUNT
					,ALIGNED_DPL_NM_PLATFORM
					,ALIGNED_DAP_DESC_AIO_PLATFORM
					,ALIGNED_DOP_NM_OEM_PARENT
					,ALIGNED_DOB_DESC_OEM_BRAND
					,ALIGNED_DPB_NM_PROCESSOR_BRAND
					,ALIGNED_DPS_DESC_PROCESSOR_SKU
					,ALIGNED_TVE_DESC_VERSION
					,ALIGNED_DTE_DESC_TECHNOLOGY
					,ALIGNED_FSO_MODEL_DESCRIPTION
					,ALIGNED_TGT_GFX_TYPE
					,ALIGNED_D21_2_IN_1_NM
					,ALIGNED_DTC_TOUCH_NM
					,ALIGNED_GROSS_SYSTEM_REVENUE
					,ALIGNED_UNITS
					,PANEL
					,CONSUMER_PRODUCT_ID
					,RAW_PRODUCT_ID
					,CRE_DT
					)
				SELECT COALESCE(NAR.ALIGNED_DPM_DESC_MO, '')
					,COALESCE(NAR.ALIGNED_DPQ_DESC_QTR, '')
					,COALESCE(NAR.ALIGNED_DCO_NO_CTRY, '')
					,COALESCE(NAR.ALIGNED_DCA_DESC_CNSM_ACCT, '')
					,COALESCE(NAR.ALIGNED_DPL_NM_PLTFRM, '')
					,COALESCE(NAR.ALIGNED_DAP_DESC_AIO_PLTFRM, '')
					,COALESCE(NAR.ALIGNED_DOP_NM_OEM_PARNT, '')
					,COALESCE(NAR.ALIGNED_DOB_DESC_OEM_BRND, '')
					,COALESCE(NAR.ALIGNED_DPB_NM_PCSR_BRND, '')
					,COALESCE(NAR.ALIGNED_DPS_DESC_PCSR_SKU, '')
					,COALESCE(NAR.ALIGNED_TVE_DESC_VER, '')
					,COALESCE(NAR.ALIGNED_DTE_DESC_TECH, '')
					,COALESCE(NAR.ALIGNED_FSO_MDL_DESC, '')
					,COALESCE(NAR.ALIGNED_TGT_GFX_TYPE, '')
					,COALESCE(NAR.ALIGNED_D21_2_IN_1_NM, '')
					,COALESCE(NAR.ALIGNED_DTC_TOUCH_NM, '')
					,NAR.ALIGNED_GRS_SYS_REV
					,NAR.ALIGNED_UN
					,COALESCE(NAR.PANEL, '')
					,CPRD.CONSUMER_PRODUCT_ID
					,PRD.RAW_PRODUCT_ID
					,NAR.CRE_DT
				FROM #tmpNAR NAR
				LEFT JOIN DF_DENORM.forecast.CK_CNG_DIM_ID_MAPPING CPRD(NOLOCK) ON CPRD.CONCAT_VAL = NAR.CONSUMER_PRODUCT_VAL
					AND CPRD.VNDR = 'IP_NAR'
				LEFT JOIN DF_DENORM.forecast.CK_CNG_DIM_ID_MAPPING PRD(NOLOCK) ON PRD.CONCAT_VAL = NAR.RAW_PRODUCT_VAL
					AND PRD.VNDR = 'IP'
				EXEC forecast.PR_PRINTD '--Insert into NAR ck table Completed.....'
			END
			EXEC DF.FORECAST.PR_CRE_UPD_COLUMNSTORE_INDEX 'DF_DENORM.forecast.CK_CNG_INTEL_PANEL_NAR'
			EXEC forecast.PR_PRINTD '--Columnstore index created on CK_CNG_INTEL_PANEL_NAR .....'
			SELECT @l_cmd_syntax = CONCAT (
					'forecast.PR_CNG_POST_DATA_LD_PRCS '
					,QUOTENAME('IP', '''')
					,','
					,QUOTENAME(@l_cre_dt, '''')
					)
			EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax
				,@l_cre_dt
			SET @l_publish_status = 'Raw Data Upload Status for Intel Panel'
			SET @l_status = 'Intel Panel - Raw Data Upload Successful.'
			SET @l_email_title_ld_status = 'Raw Data Upload Successful. Latest data is now available in reporting.'
			SET @l_ld_status = 'Raw Data Upload Successful.'
			SET @l_v2_subj_success = CONCAT (
					@l_env
					,' : CNG : '
					,@l_email_title_ld_status
					,' for Intel Panel : '
					,REPLACE(CONVERT(VARCHAR(10), GETDATE(), 110), '-', '/')
					,' '
					,CONVERT(VARCHAR(8), GETDATE(), 114)
					,@l_server
					)
			EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION] @in_subj = @l_v2_subj_success
				,@in_status = @l_status
				,@in_msg = @l_ld_status
				,@in_file_list = @l_file_list
				,@in_dq_email_addr = @l_dq_rcv_email_addr
				,@in_usr_email_addr = @l_upld_usr_email_addr
				,@in_usr_nm = @l_upld_usr_nm
				,@in_publish_status = @l_publish_status
				,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr
		END
		ELSE IF @l_reg_batch = 'N'
			EXEC forecast.PR_PRINTD 'Procedure was called in batch mode and today is not 22nd of the month. This procedure only runs on 22nd of the month in batch mode.....'
		Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
		EXEC forecast.PR_PRINTD '--forecast.PR_CNG_LD_CK_TBL_INTEL_PANEL Completed.....'
	END TRY
	BEGIN CATCH
		SET @l_publish_status = 'Raw Data Upload Status for Intel Panel'
		SET @l_status = 'Intel Panel - Raw Data Upload Failed'
		SET @l_email_title_ld_status = 'Raw Data Upload Failed'
		SET @l_ld_status = CONCAT (
				'Raw File Upload Failed. Latest raw file data is NOT available in reporting.<BR><BR>'
				,ERROR_MESSAGE()
				,' at Line# '
				,ERROR_LINE()
				,' & Error# '
				,ERROR_NUMBER()
				)
		SET @l_v2_subj_success = CONCAT (
				@l_env
				,' : CNG : '
				,@l_email_title_ld_status
				,' for Intel Panel : '
				,REPLACE(CONVERT(VARCHAR(10), GETDATE(), 110), '-', '/')
				,' '
				,CONVERT(VARCHAR(8), GETDATE(), 114)
				,@l_server
				)
		EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION] @in_subj = @l_v2_subj_success
			,@in_status = @l_status
			,@in_msg = @l_ld_status
			,@in_file_list = @l_file_list
			,@in_dq_email_addr = @l_dq_rcv_email_addr
			,@in_usr_email_addr = @l_upld_usr_email_addr
			,@in_usr_nm = @l_upld_usr_nm
			,@in_publish_status = @l_publish_status
			,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr
		Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
	END CATCH
END