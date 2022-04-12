CREATE OR ALTER PROCEDURE [forecast].[PR_CNG_WRAPPER_LD_PROFISEE] 
(
	 @in_idsid		VARCHAR(30) = '' 
	,@in_frst_ld		CHAR(1) = 'N'
	,@int_cprd_ld		INT = 0
	,@int_oem_ld		INT = 0
	,@int_mseg_ld		INT = 0
	,@int_prd_ld		INT = 0
	,@int_chnl_ld		INT = 0
	,@int_geo_ld		INT = 0
	,@int_pbnd_ld		INT = 0
	,@int_cepb_ld		INT = 0
	,@int_gpum_ld		INT = 0
	,@int_full_ld		INT = 0)
AS  
/*****************************************************************************************************  
* Name          : PR_CNG_WRAPPER_LD_PROFISEE  
* Author        : Prithvi B R 
* Purpose       : Wrapper procedure to load Profisee Master Data
* View          :  
* Test			: EXEC forecast.PR_CNG_WRAPPER_LD_PROFISEE '', 'N'
*******************************************************************************************************  
* Change Date   Change By			Change DSC  
* -----------   -------------		-------------------------------------------  
* 10/28/2014    Prithvi B R		    Created
* 11/07/2014    Nohin George		Mofified for Profisee ld calls
* 11/11/2014	Prithvi B R			added idsid of the person who triggers load
* 12/05/2014	Prithvi B R			Error Msg Handling
									Test for Link Server
* 12/09/2014	Nohin George		@in_cprd_atrb_return_code added to Cprd call	
* 02/10/2015    Prithvi B R		    US3169 / TA10834 : Load procedure made dynamic
* 06/04/2015	Chaitanya Reddy		Added GPU Model
* 08/17/2015	Jayesh P,TCS		Removed dynamic call for GPU OEM
* 11/17/2015	Srivatsava PSV		Removed dynamic calls 
* 11/25/2015	Jayesh P,TCS		Added Sort calls and screen size group
* 12/09/2015	Srivatsava PSV		Reverted back dynamic calls 
* 12/28/2015	Jayesh P,TCS		Added execute calls for sort keys 
* 7/19/2018		Vishwanath B S		Modified the stored procedure so that dimension load happens selectively through UI selections.
*******************************************************************************************************/   	
BEGIN 
	BEGIN TRY 
		EXEC forecast.PR_PRINTD 'forecast.PR_CNG_WRAPPER_LD_PROFISEE...Started'
		Update forecast.cnst set CNST_VAL = 1, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
		DECLARE  @l_cntnr_tmstmp			DATETIME = GETDATE()
				,@l_pkg_strt_tm				DATETIME
				,@l_cmd_syntax				VARCHAR(2000) = ''
				,@l_machine_nm				VARCHAR(100) 
				,@JOB_ID					INT
				,@l_ff_cat_return_code		INT = 1
				,@l_gpu_oem_return_code		INT = 1
				,@l_hdd_sz_return_code		INT = 1
				,@l_os_return_code			INT = 1
				,@l_ram_return_code			INT = 1
				,@l_scrn_sz_return_code		INT = 1
				,@l_tchscn_return_code		INT = 1
				,@l_oem_return_code			INT = 1
				,@l_mseg_return_code		INT = 1
				,@l_prd_return_code			INT = 1
				,@l_chnl_return_code		INT = 1
				,@l_geo_return_code			INT = 1
				,@l_pbnd_return_code		INT = 1
				,@l_cepb_return_code		INT = 1
				,@l_gpum_return_code		INT = 1
				,@l_cprd_return_code		INT = 1
				,@l_cprd_atrb_return_code	INT = 1
				,@l_msg_bdy					VARCHAR(max) = ''
				,@l_ld_status				VARCHAR(300) = ''
				,@l_email_title_ld_status	VARCHAR(100) = '' 
				,@l_email_hdr_nm			VARCHAR(30) = ''
				,@l_dq_rcv_email_addr		VARCHAR(300) = ''
				,@l_dq_tech_ownr_email_addr VARCHAR(300) = ''
				,@l_upld_usr_nm				VARCHAR(100) = ''
				,@l_upld_usr_email_addr		VARCHAR(100) = ''
				,@l_ENV						VARCHAR(30) = forecast.FN_GET_ENV()
				,@l_server					VARCHAR(100) = CONCAT(' Server: ' , ISNULL(@@SERVERNAME,'No Server Name'))
		IF(COALESCE(@in_idsid, '') <> '')
				SELECT
					 @l_upld_usr_nm = cdis.ccMailName
					,@l_upld_usr_email_addr = cdis.DomainAddress
				FROM forecast.vw_STG_WorkerPublicExtended cdis (NOLOCK)
				WHERE cdis.upperIDSID = @in_idsid
			ELSE
				BEGIN
					SET	 @l_upld_usr_nm = 'Automated Profisee Data Load'
					SET  @l_upld_usr_email_addr = 'demand.forecasting@intel.com'
				END
			SELECT 
				 @l_dq_rcv_email_addr =  CONCAT(IIF(forecast.FN_GET_ENV() = 'PROD',DUCM.RCV_EMAIL_ADDR,DUCM.PRE_PRD_RCV_EMAIL_ADDR),@l_upld_usr_email_addr , ';')
				,@l_dq_tech_ownr_email_addr = DUCM.DQ_TECH_OWNR_EMAIL_ADDR 
			FROM 
				DF_DQMF.dbo.DQ_USE_CASE_MDATA DUCM
			INNER JOIN DF_DQMF.dbo.dq_use_case DUC
			ON DUCM.DQ_USE_CASE_CD = DUC.DQ_USE_CASE_CD	
			WHERE
				DQ_USE_CASE_ID in ('954', '955', '992')
		/*CCPRD load*/
		if(@int_cprd_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_FF_CAT]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_FF_CAT ',QUOTENAME(@in_frst_ld,'''')) ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'			
			EXEC @l_ff_cat_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_FF_CAT', @in_frst_ld	
			EXEC    DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_FF_CAT]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_GPU_OEM]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_GPU_OEM ',QUOTENAME(@in_frst_ld,'''')) ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'
			--EXEC @l_gpu_oem_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_GPU_OEM', @in_frst_ld
			SET @l_gpu_oem_return_code = 1
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_GPU_OEM]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_HDD_SZ]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_HDD_SZ ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		 			
			EXEC @l_hdd_sz_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_HDD_SZ', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_HDD_SZ]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for HDD...Started...'
			SELECT @l_cmd_syntax = 'forecast.PR_CNG_UPD_CCPRD_ATRB_SORT_KEY ''HDD'', ''forecast.VW_CNG_PROFISEE_CCPRD_HDD_SZ'',''HDD_Size'',''HDD_Size_sort'''
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC (@l_cmd_syntax) 			
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for HDD...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_OS]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_OS ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_os_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_OS', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_OS]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_RAM]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_RAM ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_ram_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_RAM', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_RAM]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for RAM...Started...'
			SELECT @l_cmd_syntax = 'forecast.PR_CNG_UPD_CCPRD_ATRB_SORT_KEY ''RAM'', ''forecast.VW_CNG_PROFISEE_CCPRD_RAM'',''System_Memory_RAM'',''System_Memory_RAM_sort'''
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		 	
			EXEC (@l_cmd_syntax)		
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for RAM...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_SCRN_SZ]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_SCRN_SZ ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_scrn_sz_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_SCRN_SZ', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_SCRN_SZ]...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for SCREEN SIZE...Started...'
			SELECT @l_cmd_syntax = 'forecast.PR_CNG_UPD_CCPRD_ATRB_SORT_KEY ''SCREEN SIZE'', ''forecast.VW_CNG_PROFISEE_CCPRD_SCRN_SZ'',''Screen_Size'',''Screen_Size_sort'''
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC (@l_cmd_syntax) 			
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for SCREEN SIZE...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for SCREEN SIZE GROUP...Started...'
			SELECT @l_cmd_syntax = 'forecast.PR_CNG_UPD_CCPRD_ATRB_SORT_KEY ''SCREEN SIZE GROUP'', ''DF_DENORM.FORECAST.CK_CNG_PROFISEE_CCPRD_SCRN_SZ'',''SCREEN_SIZE_GROUP'''
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'	
			EXEC (@l_cmd_syntax) 	 			
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_UPD_CCPRD_ATRB_SORT_KEY] for SCREEN SIZE GROUP...Completed...'
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_TOUCH]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCPRD_TOUCH ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_tchscn_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCPRD_TOUCH', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCPRD_TOUCH]...Completed...'
		END
		/*OEM Dimension load*/
		if(@int_oem_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_COEM]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_COEM ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'					
			EXEC @l_oem_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_COEM', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_COEM]...Completed...'
		END
		/*Market segmenet dimension load*/
		if(@int_mseg_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CMSEG]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CMSEG ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_mseg_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CMSEG', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CMSEG]...Completed...'
		END
		/*Product dimension load*/
		if(@int_prd_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CPPRD]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CPPRD ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'				
			EXEC @l_prd_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CPPRD', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CPPRD]...Completed...'
		END
		/*Channel dimension load*/
		if(@int_chnl_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCHNL]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CCHNL ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_chnl_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CCHNL', @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CCHNL]...Completed...'
		END
		/*GEO dimension load*/
		if(@int_geo_ld = 1 or @int_full_ld = 1)
		begin
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CGEO]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CGEO ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_geo_return_code = forecast.PR_CNG_LD_PROFISEE_CGEO @in_frst_ld
			EXEC   DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CGEO]...Completed...'
		end
		/*Price band dimension load*/
		if(@int_pbnd_ld = 1 or @int_full_ld = 1)
		begin
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CPBND]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CPBND ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING'		
			EXEC @l_pbnd_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CPBND',  @in_frst_ld
			EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CPBND]...Completed...'
		end
		/*Euro Price band dimension load*/
		if(@int_cepb_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CEPB]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CEPB ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING' 			
			EXEC @l_cepb_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CEPB', @in_frst_ld
			EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CEPB]...Completed...'
		END
		/*CGPU dimension load*/
		if(@int_gpum_ld = 1 or @int_full_ld = 1)
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CGPU]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_LD_PROFISEE_CGPU ',QUOTENAME(@in_frst_ld,''''))  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING' 			
			EXEC @l_gpum_return_code = forecast.PR_CNG_LD_PROFISEE 'CK_CNG_PROFISEE_CGPU', @in_frst_ld
			EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_LD_PROFISEE_CGPU]...Completed...'
		END
		IF(@l_gpu_oem_return_code = 2 OR @l_hdd_sz_return_code = 2 OR @l_os_return_code = 2 OR @l_ram_return_code = 2 OR @l_scrn_sz_return_code = 2 OR @l_tchscn_return_code = 2 )
		BEGIN
			--Atrbs are modified 
			SET @l_cprd_atrb_return_code = 0
		END
		IF (@l_ff_cat_return_code = 0 OR @l_cprd_atrb_return_code = 0)
		BEGIN
			--CCPRD has to be processed as some changes
			SET @l_cprd_return_code = 0
		END
		EXEC forecast.PR_PRINTD 'Sending Profisee Master Data Load Status Mail ...STARTED...'		
		SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_publish_profisee_ld_notification @in_idsid = ', QUOTENAME(@in_idsid,''''),
						',@int_cprd_ld =',QUOTENAME(@int_cprd_ld,''''),
						',@int_oem_ld =',QUOTENAME(@int_oem_ld,''''),
						',@int_mseg_ld =',QUOTENAME(@int_mseg_ld,''''),
						',@int_prd_ld =',QUOTENAME(@int_prd_ld,''''),
						',@int_chnl_ld =',QUOTENAME(@int_chnl_ld,''''),
						',@int_geo_ld =',QUOTENAME(@int_geo_ld,''''),
						',@int_pbnd_ld =',QUOTENAME(@int_pbnd_ld,''''),
						',@int_cepb_ld =',QUOTENAME(@int_cepb_ld,''''),
						',@int_gpum_ld =',QUOTENAME(@int_gpum_ld,''''),
						',@int_full_ld =',QUOTENAME(@int_full_ld,''''))
		EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@l_cntnr_tmstmp
        EXEC forecast.PR_PRINTD 'Sending Profisee Master Data Load Status Mail ...COMPLETED...'
		IF @in_frst_ld = 'N'
		BEGIN
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_WRAPPER_DIM_LD_PROFISEE]...Started...'
			SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_WRAPPER_DIM_LD_PROFISEE @in_is_cprd_ld = ',QUOTENAME(@l_cprd_return_code,''''),',@in_is_oem_ld =',QUOTENAME(@l_oem_return_code,''''),',@in_is_mseg_ld =',QUOTENAME(@l_mseg_return_code,''''),',@in_is_prd_ld = ',QUOTENAME(@l_prd_return_code,''''),',@in_is_chnl_ld =',QUOTENAME(@l_chnl_return_code,''''),',@in_is_geo_ld =',QUOTENAME(@l_geo_return_code,''''),',@in_is_pbnd_ld =',QUOTENAME(@l_pbnd_return_code,''''),',@in_is_cepb_ld =',QUOTENAME(@l_cepb_return_code,''''),', @in_is_gpum_ld=',QUOTENAME(@l_gpum_return_code,''''),',@in_cprd_atrb_return_code =',QUOTENAME(@l_cprd_atrb_return_code,''''),', @in_cntnr_tmstmp=',QUOTENAME(@l_cntnr_tmstmp,''''))
				  ,@l_pkg_strt_tm = GETDATE()
			EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   @l_cmd_syntax,@l_MACHINE_NM,@l_cntnr_tmstmp,@l_pkg_strt_tm,NULL,NULL,NULL,NULL,NULL,@JOB_ID OUTPUT,'RUNNING' 			
			EXEC forecast.PR_CNG_WRAPPER_DIM_LD_PROFISEE @l_cprd_return_code, @l_oem_return_code, @l_mseg_return_code, @l_prd_return_code , @l_chnl_return_code, @l_geo_return_code, @l_pbnd_return_code, @l_cepb_return_code,@l_gpum_return_code,@l_cprd_atrb_return_code, @l_cntnr_tmstmp
			EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID
			EXEC forecast.PR_PRINTD 'Call [FORECAST].[PR_CNG_WRAPPER_DIM_LD_PROFISEE]...Completed...'	
		END
-- SENDING FINAL NOTIFICATION
		SET @l_email_title_ld_status =  CONCAT(@l_env , ' : CNG : Profisee Master Data Refresh Success : ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)
	EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION] 
			@l_email_title_ld_status 
			, 'Profisee Data Refresh Successful'
			, 'The Profisee data has been refreshed and is ready for mapping'
			, ''
			,@l_dq_rcv_email_addr
			,@l_upld_usr_email_addr
			,@l_upld_usr_nm
			,'Profisee Master Data Refresh Status'
			,@l_dq_tech_ownr_email_addr
			,'N'
	Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
	EXEC forecast.PR_PRINTD 'forecast.PR_CNG_WRAPPER_LD_PROFISEE...Completed'
	END TRY
	BEGIN CATCH
		SET @l_ld_status = CONCAT('Profisee master data refresh Failed due to System Failure. Latest data is NOT available in reporting.<BR><BR>',ERROR_MESSAGE(),' at Line# ',ERROR_LINE(),' & Error# ',ERROR_NUMBER())
		SET @l_email_title_ld_status =  CONCAT(@l_env , ' : CNG : Profisee Master Data Refresh Failed : ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)
				EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION] 
					@l_email_title_ld_status --'Subject line - Profisee Data load complete - ready for Mapping'
					, 'Profisee Data Refresh failed'
					, @l_ld_status
					, ''
					,@l_dq_rcv_email_addr
					,@l_upld_usr_email_addr
					,@l_upld_usr_nm
					,'Profisee Master Data Refresh Status'
					,@l_dq_tech_ownr_email_addr
					,'N'
		EXEC forecast.PR_CUSTOM_ERRMSG @Exit_or_continue= 'CONTINUE';  
		Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
	END CATCH
END