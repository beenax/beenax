CREATE OR ALTER PROCEDURE [FORECAST].[PR_CNG_PUBLISH_MAPPING_DATA]
(
	@in_file_typ	VARCHAR(100),
	@in_idsid		VARCHAR(30) = '',
	@in_debug		CHAR(1) = 'N'
)
AS  
/*****************************************************************************************************  
* NAME          : PR_CNG_PUBLISH_MAPPING_DATA
* AUTHOR        : SRI  
* PURPOSE       : UPDATE PUBLISH FLAG AND DIMENSIONS
* VIEW          :  
* TEST			: EXEC FORECAST.PR_CNG_PUBLISH_MAPPING_DATA 'CONS REV','ngeorgex'
******************************************************************************************************* 
* Change Date   Change By		Change DSC  
* -----------   -------------   -------------------------------------------  
* 07/17/2014	Sri				Created
* 07/21/2014	Sri				Cube call
* 07/22/2014	Sri				Added proc call to dump data into CK ATRB FALLOUTS and VENDOR FALLOUTS	
* 07/24/2014	Nohin George	Added Call for Send Email thru PR_TPCA_SEND_PUBLISH_NOTIFICATION 
* 07/28/2014	Mehul Shah		In BEGIN CATCH, changed success variable to failure variable for email 
* 08/22/2014	Mehul Shah		Added IDSID as input param and using that to get the email of the person submitting publish.
* 10/02/2014	Vijay Srivatsa	Removed file type validation
* 10/14/2014	Nohin George	Modified to include the Tech owner in the BCC list
* 10/17/2014    Jayesh P,TCS	Renaming as per new naming convention
* 11/14/2014	Vijay Srivatsa	Consortia file type will be passed as CNSRT to dim update procs
* 12/08/2014	Vijay Srivatsa	EMEA Discounter Changes
* 01/21/2015	Nohin George	Modified to include CONS_REV
* 04/09/2015   Randy Salas Morris Modified to indclude IDC Forecast
* 07/29/2015	Chaitanya Reddy	Added IDC Server file type conversion 
* 10/20/2015	Srivatsava PSV	Added IDC X86 file type conversion
* 10/29/2015	Srivatsava PSV	Effort for removing hard coding. 
* 01/14/2016	Dayana Varela	Added proc call PR_CNG_INSERT_CNG_AUDIT_PROCESS_ENTRY and PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY
*******************************************************************************************************/ 
BEGIN
	BEGIN TRY
	EXEC forecast.PR_PRINTD 'Procedure forecast.PR_CNG_PUBLISH_MAPPING_DATA Started.....'
	Update forecast.cnst set CNST_VAL = 1, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
		DECLARE
			@l_env				 VARCHAR(500) = forecast.FN_GET_ENV(),
			@l_server			 VARCHAR(500) = CONCAT(' Server: ' , ISNULL(@@SERVERNAME,'No Server Name')),
			@l_err_msg			 VARCHAR(MAX) = '',
			@l_dq_rcv_email_addr VARCHAR(500) = '',
			@l_dqmf_desc		 VARCHAR(2000)= '',
			@l_status			 VARCHAR(100) = '',
			@l_msg				 VARCHAR(MAX) = '',
			@l_v2_subj_failure	 VARCHAR(2000)= '',
			@l_v2_subj_success	 VARCHAR(2000)= '',
			@l_upld_usr_nm		 VARCHAR(100) = '',
			@l_upld_usr_email_addr VARCHAR(100) = '',
			@l_file_typ			 VARCHAR(100) ='',
			@l_return			 INT = 0,
			@l_subj				 VARCHAR(100) = '',
			@l_dq_tech_ownr_email_addr VARCHAR(1000) ='',
			@JOBID				 INT,
			@END_TM				 DATETIME,
			@ELAPSE_TM			 TIME(7), 
			@FILE_TYPE			 VARCHAR(100) = '',
			@PROCES_TYPE		 VARCHAR(50)  = 'Publish',
			@STATUS				 VARCHAR(10)  = 'RUNNING',
			@ERR_MSG			 VARCHAR(MAX) = ''
		--INSERT NEW VALUE IN TABLE CNG_AUDIT_PROCESS 
			SET @FILE_TYPE	= @in_file_typ
			EXEC [FORECAST].[PR_CNG_INSERT_CNG_AUDIT_PROCESS_ENTRY] 
							 @JOBID output
							,@END_TM
							,@ELAPSE_TM
							,@FILE_TYPE
							,@PROCES_TYPE
							,@STATUS
							,@ERR_MSG
			SELECT @l_file_typ = CODED_SRC_NM FROM FORECAST.VW_CNG_CSRC_DYN_ATRB WHERE ALIGNED_SRC = @IN_FILE_TYP
		IF (@in_debug = 'N')
		BEGIN
			SELECT
				@l_upld_usr_nm = cdis.ccMailName
				,@l_upld_usr_email_addr = cdis.DomainAddress
			FROM forecast.vw_STG_WorkerPublicExtended cdis (NOLOCK)
			WHERE cdis.upperIDSID = @in_idsid
			IF(@l_file_typ = 'CONS_REV')
			BEGIN
				SELECT 
					@l_dq_rcv_email_addr =  IIF(forecast.FN_GET_ENV() = 'PROD',DUCM.RCV_EMAIL_ADDR,DUCM.PRE_PRD_RCV_EMAIL_ADDR),								
					@l_dqmf_desc = CONCAT(@in_file_typ,' ',DUC.dq_use_case_dsc),
					@l_dq_tech_ownr_email_addr = CONCAT(DUCM.DQ_TECH_OWNR_EMAIL_ADDR , ';' , ISNULL(@l_upld_usr_email_addr,''))
				FROM 
					DF_DQMF.dbo.DQ_USE_CASE_MDATA DUCM
				INNER JOIN DF_DQMF.dbo.dq_use_case DUC
				ON DUCM.DQ_USE_CASE_CD = DUC.DQ_USE_CASE_CD	 
				WHERE
					DUCM.DQ_USE_CASE_CD = 'DQ_CNG_CONS_REV_PUBLISH'  
			END
			ELSE
			BEGIN
				SELECT 
					@l_dq_rcv_email_addr =  IIF(forecast.FN_GET_ENV() = 'PROD',DUCM.RCV_EMAIL_ADDR,DUCM.PRE_PRD_RCV_EMAIL_ADDR),								
					@l_dqmf_desc = CONCAT(@in_file_typ,' ',DUC.dq_use_case_dsc),
					@l_dq_tech_ownr_email_addr = CONCAT(DUCM.DQ_TECH_OWNR_EMAIL_ADDR , ';' , ISNULL(@l_upld_usr_email_addr,''))
				FROM 
					DF_DQMF.dbo.DQ_USE_CASE_MDATA DUCM
				INNER JOIN DF_DQMF.dbo.dq_use_case DUC
				ON DUCM.DQ_USE_CASE_CD = DUC.DQ_USE_CASE_CD	 
				WHERE
					DUCM.DQ_USE_CASE_CD = 'DQ_CNG_PUBLISH'  
			END
			SET @l_v2_subj_failure = CONCAT(@l_env ,' : CNG : ',@in_file_typ,' Publish Failed : ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)
			SET @l_v2_subj_success =  CONCAT(@l_env , ' : CNG :  ',@in_file_typ,' Publish Successful : ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)
			IF(@l_file_typ = 'CONS_REV')
			BEGIN
				EXEC @l_return = FORECAST.[PR_CNG_CONS_REV_UPD_FROM_MAPPING] @l_file_typ
			END
			ELSE
			BEGIN
				EXEC [forecast].[PR_CNG_WRAPPER_DIM_UPD_FROM_MAPPING] @l_file_typ
			END
			IF @l_return = 0
			BEGIN
				SET @l_msg = @l_dqmf_desc
				SET @l_status = CONCAT(@in_file_typ,' Publish Successful')
				SET @l_subj = @l_v2_subj_success
			END
			ELSE
			BEGIN
				SET @l_msg = 'Publish successful in DSS but failed to push the data into CONS REV'
				SET @l_status = CONCAT(@in_file_typ,' Publish Failed')
				SET @l_subj = @l_v2_subj_failure
			END
			EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION]
							 @in_subj = @l_subj
							,@in_status = @l_status
							,@in_msg = @l_msg
							,@in_dq_email_addr = @l_dq_rcv_email_addr
							,@in_usr_email_addr = @l_upld_usr_email_addr
							,@in_usr_nm = @l_upld_usr_nm
							,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr
		END
		--SET @ERR_MSG = ERROR_MESSAGE()
		SET @STATUS	 = 'SUCCESFULL'
		--UPDATE VALUE IN TABLE CNG_AUDIT_PROCESS WHEN PUBLISH COMPLETED 
		EXEC [FORECAST].[PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY] 
						 @JOBID
						,@STATUS
						,@ERR_MSG
		Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
		EXEC forecast.PR_PRINTD 'Procedure forecast.PR_CNG_PUBLISH_MAPPING_DATA Completed.....'
	END TRY
	BEGIN CATCH
		  -- Send publish failed message.....
		SET @l_msg = '<span style="font-size:10.0pt;font-weight:bold;font-family:arial;color:white"><i>Publish Failed...</i></span><BR><BR>' +
					ERROR_MESSAGE()  + ' at Line# ' + CAST(ERROR_LINE() AS VARCHAR(10)) + ' & Error# ' + cast(ERROR_NUMBER() as Varchar(10))
		SET @l_status = CONCAT(@in_file_typ,' Publish Failed')
		IF @@TRANCOUNT > 0 
			ROLLBACK
		IF @in_debug = 'N'
		BEGIN 
			EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION]
							 @in_subj = @l_v2_subj_failure
							,@in_status = @l_status
							,@in_msg = @l_msg
							,@in_dq_email_addr = @l_dq_rcv_email_addr
							,@in_usr_email_addr = @l_upld_usr_email_addr
							,@in_usr_nm = @l_upld_usr_nm
							,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr
		END
		SET @ERR_MSG	= ERROR_MESSAGE()
		SET @STATUS	=	'FAILED'
		--UPDATE VALUE IN TABLE CNG_AUDIT_PROCESS WHEN PUBLISH FAILED 
		EXEC [FORECAST].[PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY] 
						 @JOBID
						,@STATUS
						,@ERR_MSG
		Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
	    EXEC forecast.PR_CUSTOM_ERRMSG @Exit_or_continue= 'CONTINUE';  
	END CATCH
END
