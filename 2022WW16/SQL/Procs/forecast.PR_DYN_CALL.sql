Alter PROCEDURE [forecast].[PR_DYN_CALL]    
    @cmd_syntax VARCHAR(1000) = ''
    ,@in_cntnr_strt_tm DATETIME = NULL
	,@in_do_smcln_chk CHAR(1) = 'Y'
	,@MODULE VARCHAR(20) = NULL
AS   
/*************************************************************************   
  * Name          : PR_DYN_CALL   
  * Author        : A.V.Farci    
  * Purpose       : Used by Perl ExecMSProc to execute SP   
  * View          :    
  ***************************************************************************   
  * Change Date   Change By           Change DSC   
  * -----------   -------------       -------------------------------------------   
  * 01/16/2007   A.V.Farci     Original   
  * 01/30/2007   Arvind Ranganath    Add set concat_null_yields_null off   
  * 02/05/2007    A.V.Farci     Added the config file name     
  * 02/22/2007    A.V.Farci     Added Transaction support/rollback   
  * 06/04/2007    A.V.Farci     Added check for <TRAN> to start a transaction or not  
  * 11/11/2010    Kevin Amato   Added Retry for deadlocks
  * 04/07/2011		Jayesh,TCS		Bug fix (QC defect# 5440) for rerunning in case of deadlocks
  * 01/16/2011  Ken Ambrose, TCS	Removed debug code causing excessive lines printed in log file
									Use alter with stub call instead of drop and create
  * 03/30/2012	Sarkunan H, TCS		Add run_sts and err_msg columns to df_stg.forecast.stg_ld_job to store 
									the error and information output during a package execution. 	
  * 08/20/2012   Ajit Vaidya,TCS    Added optional input parameter @in_cntnr_strt_tm to pass to DF.[forecast].[PR_STG_LD_JOB_INSERT] 
									for @CONTAINER_STRT_TM
  * 08/29/2012   Ajit Vaidya,TCS    Set @PACKAGE_STRT_TM = GetDate()	
  * 08/31/2012   Jayesh Prakash,TCS    Use variable instead of getdate directly in procedure call
  * 09/05/2012   Ajit Vaidya           Replace getdate() with @pkgtime in COALESCE function	
  * 09/17/2014   Jayesh Prakash,TCS    Added one additional paramter ,@in_do_smcln_chk CHAR(1) = 'Y'
										to ignore the semi-colon check whene needed
  * 07/19/2017   Chitra Padmanaban    Removed explicitly setting CONCAT_NULL_YIELDS_NULL OFF,By default will be ON.
  ***************************************************************************/   
  /* test cases
	exec [forecast].[PR_DYN_CALL]   'sp_who2 ''active''';
	exec [forecast].[PR_DYN_CALL]   'sp_who2 ''active''<tran>';
	exec [forecast].[PR_DYN_CALL]   'sp_who2 ''xxx''';
	exec [forecast].[PR_DYN_CALL]   'sp_who2 ''xxx''<tran>';
*/
BEGIN   
	DECLARE
		@retry_number AS int, 
		@l_err_msg  VARCHAR(max),   
		@SQLString  NVARCHAR(2000),   
		@JOB_ID   int,   
		@CurDate  datetime,  
		@UseTrans  char(1),
		@pkgtime datetime=GETDATE(),
		@l_file_name NVARCHAR(max);
	select 
		@retry_number = 1
		,@UseTrans = 'N'  
		--,@SQLString = CONVERT(nvarchar(2000),@cmd_syntax)  
		,@CurDate  = COALESCE(@in_cntnr_strt_tm,@pkgtime);   
	WHILE @retry_number BETWEEN 1 AND 5
		BEGIN
			SET @SQLString = CONVERT(nvarchar(2000),@cmd_syntax)
			BEGIN TRY 
			IF  (CHARINDEX(';',@cmd_syntax) > 0) and @in_do_smcln_chk = 'Y' 
				BEGIN   
					SET @l_err_msg = 'Command: ' + @cmd_syntax + ': is not a valid Stored Proc'   
					RAISERROR (@l_err_msg ,15,1);    
				END   
			IF CHARINDEX('<TRAN>',@SQLString) > 0   
				BEGIN  
					SET @UseTrans = 'Y'  
					SET @SQLString = REPLACE(@SQLString,'<TRAN>','')  
				END  
			 EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]   
				 @PKG_NM = @cmd_syntax,   
				 @MACHINE_NM = @@SERVERNAME,    
				 @CONTAINER_STRT_TM = @CurDate, 
				 @PACKAGE_STRT_TM = @pkgtime,   
				 @PACKAGE_END_TM = NULL,   
				 @DATA_DIR = NULL,   
				 @ERR_FILE_DIR = NULL,   
				 @ROWS_INSERTED = NULL,   
				 @ROWS_REJECTED = NULL,  
				 @JOB_ID = @JOB_ID OUTPUT,
				 @RUN_STS = 'RUNNING';   
			-- remove the config file name which is delimited by ::   
			 IF CHARINDEX('::',@SQLString) > 0   
				SET @SQLString = SUBSTRING(@SQLString,ABS(CHARINDEX('::',@SQLString)+2),2000)   
				SET @SQLString = ''   
					 + char(10) + char(13) + 'exec '   
					 + @SQLString   
				IF @UseTrans = 'Y'   
				 BEGIN TRANSACTION PR_DYN_CALL_TRAN WITH MARK N'Executing PR_DYN_CALL';  
				EXEC sp_executesql @SQLString;   
				UPDATE  DF_STG.forecast.STG_LD_JOB   
					 SET PKG_END_TM   = GETDATE(),    
					LAST_MOD_DT   = GETDATE(),   
					LAST_MOD_IDSID  = SUSER_NAME()  
					,RUN_STS= 'COMPLETED'  
				 WHERE  JOB_ID = @JOB_ID  
				IF @UseTrans = 'Y'  
				 COMMIT TRANSACTION PR_DYN_CALL_TRAN;   
				SET @retry_number = 0
			END TRY
			BEGIN CATCH                 
			IF ERROR_NUMBER()= 1205 or ERROR_MESSAGE() like '%Rerun the transaction%'                 
			BEGIN
				 if @@trancount > 0 ROLLBACK TRAN   -- increment retry number        
				 SET @retry_number = @retry_number + 1
				 IF @retry_number <= 5
					BEGIN
						WAITFOR DELAY '00:00:30'
						PRINT 'Deadlock detected. Attempting try number: ' + CAST(@retry_number AS varchar(10)) + '.'											
					END
				 ELSE
					EXEC DF.forecast.PR_CUSTOM_ERRMSG; 
			END
			ELSE
			BEGIN
				if @@trancount > 0 ROLLBACK TRAN 
				SET @retry_number = 0
				IF(@MODULE='CNG')
				BEGIN
					Select @l_file_name = CNST_VAL from forecast.cnst where CNST_NM = 'CNG_FILE_LOAD_FileName';
					UPDATE  DF_STG.forecast.STG_LD_JOB   
					SET PKG_END_TM   = GETDATE(),
						PKG_NM = Concat(pkg_nm,@l_file_name),
						LAST_MOD_DT   = GETDATE(),   
						LAST_MOD_IDSID  = SUSER_NAME(),
                        ERR_MSG  =  ERROR_MESSAGE(),
                        RUN_STS= 'FAILED' 
				WHERE  JOB_ID = @JOB_ID
				END
				ELSE
					UPDATE  DF_STG.forecast.STG_LD_JOB   
						SET PKG_END_TM   = GETDATE(),    
							LAST_MOD_DT   = GETDATE(),   
							LAST_MOD_IDSID  = SUSER_NAME(),
							ERR_MSG  =  ERROR_MESSAGE(),
							RUN_STS= 'FAILED' 
					WHERE  JOB_ID = @JOB_ID  
				EXEC DF.forecast.PR_CUSTOM_ERRMSG;
			END 
			END CATCH
	END -- while loop ends here
END;   
