Alter PROC [forecast].[PR_STG_LD_JOB_UPDATE]
		@JOB_ID INT  
		,@IN_V2_PKG_NM VARCHAR(255) = NULL
		,@IN_V2_STS VARCHAR(255) = NULL
		,@IN_DB_NM VARCHAR(20) = 'DF_STG'
		,@IN_ERR_MSG VARCHAR(500) = NULL
		,@MODULE VARCHAR(20) = NULL
AS
/*************************************************************************
  * Name          : PR_STG_LD_JOB_UPDATE
  * Author        : Arvind 
  * Purpose       : update load status of staging tables
  * View          : 
  ***************************************************************************
  * Change Date   Change By           Change DSC
  * -----------   -------------       -------------------------------------------
  * 11/30/2005    Arvind            Original
  * 07/20/2009	  Jaswinder Singh	SCR# 6181; Changed the width of @PKG_NM from VARCHAR(100) to VARCHAR(255)
  * 03/30/2012	Sarkunan H, TCS		Add run_sts and err_msg columns to df_stg.forecast.stg_ld_job to store 
									the error and information output during a package execution. 
  * 06/22/2012    Jayesh,TCS		Added optional paramter @in_v2_pkg_nm to support ai based pre-prod load	
									and logic to support count from non-forecast tables	
  * 06/25/2012    Varun, TCS		To update status of the ai based pre-prod load failed Packages
  * 01/28/2014	  Raj, Ayush A		Added DB_NM param for doing the record count on a different database as well
  * 08/08/2014	  Jayesh,TCS		Synced up with PROD DB version as the TFS PROD version was outdated
  * 08/08/2014	  Jayesh,TCS		Made runnable for non-interface calls
  * 12/15/2014	  Jayesh,TCS		Bug fix
***************************************************************************/
DECLARE @ROWS_INSERTED INT = 0,
@ROWS_REJECTED INT = 0,
@PKG_NM		   VARCHAR(255) ,
@TSQL		NVARCHAR(500),
@DEF		NVARCHAR(200),
@l_file_name Nvarchar(100);
BEGIN
BEGIN TRY
		SELECT	@ROWS_REJECTED =	COUNT(*)
		  FROM	DF_STG.forecast.STG_LD_ERR
		 WHERE	JOB_ID = @JOB_ID
		SELECT	@PKG_NM	= COALESCE(@IN_V2_PKG_NM,'FORECAST.'+PKG_NM)
		  FROM	DF_STG.forecast.STG_LD_JOB
		 WHERE  JOB_ID	= @JOB_ID
	IF CHARINDEX('.dtsx',@PKG_NM) <> 0
	BEGIN		
		SET @TSQL = 'SELECT	@ROWS_INSERTED	= COUNT(*) FROM	'+ @IN_DB_NM +'.' + SUBSTRING(@PKG_NM,1,CHARINDEX('.dtsx',@PKG_NM)-1)
		SET @DEF = '@ROWS_INSERTED INT output'
		EXEC SP_EXECUTEsql @TSQL,@DEF,@ROWS_INSERTED=@ROWS_INSERTED OUTPUT
	END	
	SET @IN_V2_STS= COALESCE(@IN_V2_STS,'COMPLETED')
	IF(@IN_V2_STS='FAILED')
	BEGIN
	SET @ROWS_INSERTED=0
	END
	IF(@IN_V2_STS='FAILED' AND @MODULE = 'CNG')
	BEGIN
		Select @l_file_name = CNST_VAL from forecast.cnst where CNST_NM = 'CNG_FILE_LOAD_FileName'
		UPDATE 	DF_STG.forecast.STG_LD_JOB
		SET	PKG_END_TM			= GETDATE(),
			PKG_NM				= Concat(@PKG_NM,@l_file_name),
			ROWS_REJECTED		= @ROWS_REJECTED,
			ROWS_INSERTED		= @ROWS_INSERTED,
			LAST_MOD_DT			= GETDATE(),
			LAST_MOD_IDSID		= SUSER_NAME(),
			RUN_STS= @IN_V2_STS,
			ERR_MSG = @IN_ERR_MSG
		WHERE 	JOB_ID = @JOB_ID
	END
	ELSE
		UPDATE 	DF_STG.forecast.STG_LD_JOB
			SET	PKG_END_TM			= GETDATE(),
				ROWS_REJECTED		= @ROWS_REJECTED,
				ROWS_INSERTED		= @ROWS_INSERTED,
				LAST_MOD_DT			= GETDATE(),
				LAST_MOD_IDSID		= SUSER_NAME(),
				RUN_STS= @IN_V2_STS,
				ERR_MSG = @IN_ERR_MSG
			WHERE 	JOB_ID = @JOB_ID
END TRY
BEGIN CATCH
	IF(@MODULE='CNG')
	BEGIN
		Select @l_file_name = CNST_VAL from forecast.cnst where CNST_NM = 'CNG_FILE_LOAD_FileName'
		UPDATE  DF_STG.forecast.STG_LD_JOB   
			SET PKG_END_TM   = GETDATE(), 
				PKG_NM		= CONCAT(@PKG_NM, @l_file_name),   
				LAST_MOD_DT   = GETDATE(),   
				LAST_MOD_IDSID  = SUSER_NAME(),
				ERR_MSG  =  ERROR_MESSAGE(),
				RUN_STS = 'FAILED' 
		WHERE  JOB_ID = @JOB_ID  
	END
	ELSE
		UPDATE  DF_STG.forecast.STG_LD_JOB   
			SET PKG_END_TM   = GETDATE(),    
				LAST_MOD_DT   = GETDATE(),   
				LAST_MOD_IDSID  = SUSER_NAME(),
				ERR_MSG  =  ERROR_MESSAGE(),
				RUN_STS = 'FAILED' 
		WHERE  JOB_ID = @JOB_ID  
	EXEC DF.forecast.PR_CUSTOM_ERRMSG
END CATCH
END
