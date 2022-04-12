Alter PROC [forecast].[PR_STG_LD_JOB_INSERT]
		@PKG_NM	VARCHAR(255) ,
		@MACHINE_NM VARCHAR(50),
		@CONTAINER_STRT_TM DATETIME,
		@PACKAGE_STRT_TM DATETIME,
		@PACKAGE_END_TM DATETIME,
		@DATA_DIR	VARCHAR(50) ,
		@ERR_FILE_DIR VARCHAR(50) ,
		@ROWS_INSERTED INT ,
		@ROWS_REJECTED INT ,
		@JOB_ID INT OUTPUT,
		@RUN_STS VARCHAR(50)=NULL
		,@MODULE VARCHAR(20) = NULL
		AS
/*************************************************************************
  * Name          : PR_STG_LD_JOB
  * Author        : Arvind 
  * Purpose       : update load status of staging tables
  * View          : 
  ***************************************************************************
  * Change Date   Change By           Change DSC
  * -----------   -------------       -------------------------------------------
  * 11/30/2006    Arvind            Original
  * 01/16/2007	  A.V.Farci			Added @@Identity return
  * 01/24/2007    A.V.Farci         bumpoed the pkg_nm to 100
  * 07/20/2009	  Jaswinder Singh	SCR# 6181; Changed the width of @PKG_NM from VARCHAR(100) to VARCHAR(255)
  * 04/02/2012	Sarkunan H, TCS		Add run_sts and err_msg columns to df_stg.forecast.stg_ld_job to store 
									the error and information output during a package execution. 
  * 08/22/2014	Nohin George		when machine nm is '' use @@SERVERNAME
  ***************************************************************************/
	BEGIN
	DECLARE @l_file_name Nvarchar(100);
		BEGIN TRY
			IF(@RUN_STS='FAILED'  AND @MODULE = 'CNG')
			BEGIN
				Select @l_file_name = CNST_VAL from forecast.cnst where CNST_NM = 'CNG_FILE_LOAD_FileName'
				SELECT @PKG_NM = CONCAT(@PKG_NM, @l_file_name)
			END
			
			INSERT INTO 	DF_STG.forecast.STG_LD_JOB
			(	PKG_NM, 
				MACHINE_NM, 
				CONTAINER_STRT_TM, 
				PKG_STRT_TM,
				PKG_END_TM, 
				DATA_DIR,
				ERR_FILE_DIR,
				ROWS_INSERTED,
				ROWS_REJECTED,
				RUN_STS
			)
			VALUES (@PKG_NM,
					IIF(COALESCE(@MACHINE_NM,'')='',@@SERVERNAME,@MACHINE_NM),
					@CONTAINER_STRT_TM,
					@PACKAGE_STRT_TM,
					@PACKAGE_END_TM,
					@DATA_DIR,
					@ERR_FILE_DIR,
					@ROWS_INSERTED,
					@ROWS_REJECTED,
					@RUN_STS )
		 SELECT @JOB_ID = @@IDENTITY
		 END TRY
		BEGIN CATCH
			  	EXEC DF.forecast.PR_CUSTOM_ERRMSG
		  END CATCH
	END
