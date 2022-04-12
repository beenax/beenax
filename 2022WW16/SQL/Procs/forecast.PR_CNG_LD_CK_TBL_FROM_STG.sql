CREATE OR ALTER PROCEDURE [forecast].[PR_CNG_LD_CK_TBL_FROM_STG]   
(  
 @in_file_type VARCHAR(500) = '',  
 @in_debug  CHAR(1) = 'N'  
)  
AS    
/*****************************************************************************************************    
* Name          : PR_CNG_LD_CK_TBL_FROM_STG    
* Author        : Mehul Shah    
* Purpose       : Create View for CNG Staging Data and load data into CK table  
* View          :    
* Test   : EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'GFK','n'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_WEEKLY_BIZ','Y'       
           EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_WEEKLY','Y'     
         EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_WEEKLY_TAB','N'     
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_COM_RES','Y'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_COM_DIS','Y'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'US','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'IDC','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'IDC_SVR','Y'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'NPD_PC_CN','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'IDC_FCST','Y'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'APMSC','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'EMS','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'AMSC_US','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'AMSC_CANADA','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'AMSC_LAR','N'  
     EXEC forecast.PR_CNG_LD_CK_TBL_FROM_STG 'EMEA_DISCOUNTER','N'  
********************************************************************************************************************    
* Change Date   Change By   Change DSC    
* -----------   -------------  -------------------------------------------    
* 05/29/2014    Mehul Shah   Created  
* 05/30/2014 Nohin George,TCS Modified to implement cursor for processing multiple files  
* 06/03/2014 Mehul Shah   Removed additional parameters from [forecast].[PR_CNG_SEND_NOTIFICATION] proc  
* 06/03/2014 Nohin George,TCS CK table creation taken out and included after a STG table is created  
* 06/03/2014 Mehul Shah   Added call to forecast.PR_CNG_DQ_CRTCL_COLS_CHK  
* 06/04/2014 Mehul Shah   Updated DQ logic  
         Passing Error Message Header to the Create HTML proc  
* 06/09/2014 Nohin George,TCS Modified to build CK table as per the datatype specified  
* 06/10/2014 Mehul Shah   Added IDC load  
         Removed unwated params and cleaned up obsolete code    
* 06/13/2014 Nohin George,TCS To include the COPY_HIST proc calls                
* 06/16/2014 Mehul Shah   modified logic to continue processing file even on DQs  
* 06/17/2014 Nohin George  Logic added to skip warning records insertion in cook final which are logged in the DQ table.  
* 06/17/2014 Nohin George  Added call to PR_CNG_DQ_CHK_GFK_CRITICAL & PR_CNG_DQ_CHK_IDC_CRITICAL  
         Removed unwanted input params from GFK & IDC DQ procs  
* 06/19/2014 Mehul Shah   Rearranged the flow for Critical DQ checks  
* 06/30/2014 Nohin George  DQ execution control modified & FORECAST.PR_CNG_DEL_STG_DATA @in_file_type  
* 07/01/2014 Mehul Shah   Changed the flow after critical dqs are detected  
* 07/02/2014 Mehul Shah   Added SP_REFRESH on 3 views (DQ, STG, FNL)  
         Added ROLLBACK  
* 07/11/2014 Nohin George  Modified to load the CONSUMER_PRODUCT_ID,RAW_CHANNEL_ID,RAW_PRODUCT_ID into Cook Table  
* 07/12/2014 Nohin George  ID logic modified for performance improvement  
* 07/16/2014 Nohin George  Dimension Load Wrapper Proc Call  
* 07/21/2014 Prithiv BR   Added (NOLOCK)   
* 07/21/2014 Vijay Srivatsa  Added call to PR_CNG_UPD_VAL_TXT_MAP  
* 07/22/2014 Mehul Shah   Rearranged the flow. Added call to NUM_FACT  
         Added Deletion of CK final table in batch of 100000, instead of all together  
* 07/22/2014 Vijay Srivatsa  Added call to create fallouts CK table for dashboard view  
* 07/24/2014 Nohin George  Review comments from Kevin incorporated  
* 07/24/2014 Prithvi BR   Changed logic to get the column list  
* 07/25/2014 Mehul Shah   Added creation of #tmpCountryPeriod which will be used in NUM_FACT to reduce the number of records we process  
* 07/25/2014 Sahana Soans  Changed IDC and GFK Concat Mapping Strings to take column names from forecast.CNST Table  
* 07/24/2014 Prithvi BR   Email Notification Changes  
* 07/24/2014 Prithvi BR   Included success mail within IF @l_continue = 'Y' clause  
* 07/26/2014 Mehul Shah   Updated Email Logic & HTML  
* 07/26/2014 Mehul Shah   Fixed #tmpCountryPeriod population based on IDC or GFK  
* 07/27/2014 Mehul Shah   Updated #tmpCountryPeriod logic to get LINK_TO_TIME for Quarter and Month for IDC and GFK file  
* 07/29/2014 Mehul Shah   Moved call to FORECAST.PR_CNG_UPD_VAL_TXT_MAP before DIM Load  
         Added PRINT statements  
* 07/31/2014 Mehul Shah   Removed Parsing of Period / Quarter, and just pulling raw data. Parsing will happen in NUM_FACT  
* 08/06/2014 Mehul Shah   Changed logic for CONCAT_VAL pull of MAPPINGS from CNST table  
* 08/08/2014 Nohin George  Execution Logging  
* 08/20/2014 Mehul Shah   Populating Country & Region into different CK table.   
         This CK table will be used later in Dimensions for ATRBS.  
* 08/21/2014 Mehul Shah   Removed "Populating Country & Region into different CK table" the code and moved it to a separate proc.   
         Added call to that proc.  
* 08/21/2014 Mehul Shah   Added CCPRD cook tables  
* 08/22/2014 Mehul Shah   Updated call to Fallout proc, as new parameters were added  
* 09/15/2014 Nohin George  Critical Error : Upload Failed  added for catch block exception status  
* 09/16/2014 Nohin George  IDC Truncate Logic instead of Delete  
* 09/16/2014 Vijay Srivatsa  CNG 0.75 - NPD PC TAB load logic  
* 09/18/2014 Vijay Srivatsa  QuoteName for dq_rcv_email_addr in CNG_SEND_NOFIF proc returned NULL param because length of parameter is more than 128 chars.  
         Changed to ''''  
* 09/19/2014 Vijay Srivatsa  Dynamic sql for truncating GFK,IDC, NPD PC and TAB DQ and STG tables, Create extended property only when debug is "N"  
* 09/23/2014 Vijay Srivatsa  Removed NPD Dq Code, changed err_msg variable  
* 09/24/2014 Vijay Srivatsa  Changed NPD DQ proc Name to NPD_PC_TAB_CRITICAL  
         Separate variable to check common crtcl_cols_chk  
* 09/24/2014 Prithvi BR   Added logic for NPD detailed DQ checks  
* 09/24/2014 Vijay Srivatsa  Changed success variable to failure in catch block  
* 09/25/2014 Vijay Srivatsa  Changed DYN_CALL of PR_CNG_SEND_NOTIFICATION to normal call. Set file type for GFK,IDC and custom name for NPD% to send in email  
* 09/29/2014 Vijay Srivatsa  Changed USA to United States.  
* 10/02/2014 Vijay Srivatsa  Subject change and removed mail send from catch block. Just send sql exception.  
* 10/02/2014 Vijay Srivatsa  call post load procs only for gfk and idc  
* 10/06/2014 Vijay Srivatsa  logic change to check data type of timeper columns when creating row id   
* 10/07/2014 Vijay Srivatsa  Logic Change for missing columns.  
* 10/09/2014 Nohin George  Consumer Product,product and channel ID generation logic for NPD files  
* 10/14/2014 Nohin George  Modified to include the Tech owner in the BCC list  
* 10/15/2014 Mehul Shah   Added IDC_TAB logic  
* 10/16/2014 Mehul Shah   Added FILE_REC_CNT to #tmp_file_nm table which contains the record count for that file  
* 10/17/2014 Dhanu    Change all TPCA and T<DIM_SHRT_NM> reference to CNG and C<DIM_SHRT_NM>  
* 10/20/2014 Mehul Shah   Changed 3PCA to CNG in email header  
         Updated call to forecast.PR_CNG_DQ_CHK_IDC_TAB  
* 10/20/2014 Nohin George  Consumer Product and product ID generation logic for IDC_TAB files  
* 10/22/2014 Mehul Shah   Added IDC_FCST logic  
* 10/23/2014 Mehul Shah   Added logic to pull bypass DQ flag from CNST table along with User details  
* 10/24/2014 Mehul Shah   Added call to PR_CNG_POST_DATA_LD_PRCS for IDC_TAB  
* 10/28/2014 Nohin George  Consumer Product ID logic added for IDC_FCST  
* 10/30/2014 Mehul Shah   Added Source Format Validation Bypass  
         Passing Bypass Flag to email procedures  
         Added Source Format Bypass for New Column Check  
* 11/03/2014 Nohin George  Added call to PR_CNG_POST_DATA_LD_PRCS for IDC_FCST  
* 11/11/2014 Mehul Shah   Added Consortia File Load Logic (APMSC, EMS, AMSC US, AMSC CANADA & AMSC LAR)  
* 11/12/2014 Nohin George  Consumer Product ID logic added for Consortia files (APMSC, EMS, AMSC US, AMSC CANADA & AMSC LAR)  
* 11/12/2014 Mehul Shah   Added Consortia DQ Logic (APMSC, EMS, AMSC US, AMSC CANADA & AMSC LAR)  
* 11/13/2014 Nohin George  Net Book Logic for consortia  
* 11/14/2014 Nohin George  Net Book Logic for IDC  
* 11/14/2014 Nohin George  Net Book Logic for IDC  
* 11/18/2014 Nohin George  PR_CNG_POST_DATA_LD_PRCS for (APMSC, EMS, AMSC US, AMSC CANADA & AMSC LAR)  
* 11/18/2014 Nohin George  NETBK_FF for IDC consumer ID cal  
* 11/20/2014 Nohin George  ROW_ID generation for CNSRT (APMSC, EMS, AMSC US, AMSC CANADA & AMSC LAR)  
* 11/26/2014 Mehul Shah   Added EMEA Discounter File Load Logic  
* 12/03/2014 Mehul Shah   Added EMEA Discounter Checks  
* 12/05/2014 Prithvi BR   NETBK_tm and NETBK_FF logic added for GFK and NPD; updated for IDC and Consortia  
* 12/08/2014 Nohin George  Consumer product & raw Product ID logic added for emea discounter file  
* 12/10/2014 Nohin George  Corrected Row_ID generation for EMS  
* 12/12/2014 Nohin George  NetFF logic change for GFK and NPD  
* 12/16/2014 Nohin George  PR_CNG_POST_DATA_LD_PRCS for EMEA_DISCOUNTER  
* 02/04/2015 Nohin George  Data Correction Rule Call for GFK  
* 02/04/2015 Nohin George  Email modification for Consortia Duplicate Errors  
* 02/11/2015 Chandra Tej   Modifications for implementing Data Correction for other vendors  
* 02/12/2015 Nohin george  Call STG hist load after the data correction call  
* 03/02/2015 Nohin george  implemented DELETE logic using temp(tmp_CNG_DELTE_SET) as per KEVIN's review  
* 03/23/2015 Marco Leon   Format Quarter and Month columns in Consortia file (EMS)  
* 03/25/2015 Nohin george  US3053 :IDC FCST - ETL EOL story related changes to handle the new file columns  
* 04/07/2015 Jayesh P,TCS  US3547: Emea discounter remove limit on incoming country data  
* 04/08/2015 Chaitanya Reddy  US3052: Added ROUND Function for IDC_TAB  
* 04/08/2015 Jayesh P,TCS  US3052: Moved above change to function - FN_CNG_GET_COL_CASTING  
* 05/26/2015 Chaitanya Reddy  Removed DQ check on Quarter for IDC_TAB to load IDC Tablet Forecast Data  
* 07/15/2015 Chaitanya Reddy  Added IDC ServerWW Logic  
* 08/06/2015 Chaitanya Reddy  Added NPD Commercial Logic  
* 10/08/2015 Chaitanya Reddy  Added IDC Serverx86 Logic  
* 11/24/2015 Chaitanya Reddy  Added IDC ServerX86 Consumer Product change  
* 01/14/2016 Dayana Varela  Added proc call PR_CNG_INSERT_CNG_AUDIT_PROCESS_ENTRY and PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY  
* 03/03/2016    Vallejos Alons      Modified parse for NPD raw screen sizes  as it was incorrect  
* 03/07/2016 Gopinath,Infy  Modified Year to Annual, Month to Timeper and Quarter to HQuarter for NPD PC CN file loads.  
* 10/06/2017    shubhi khare        changes for Npd Com Dis and Npd Com Res   
* 02/26/2018   Aritra M              Changes for IDC PC incremental logic  
* 03/22/2018    Madhuri M           Fixing the EMS file load issue  
* 11/2/2018    shubhi khare        changes for Npd_weekly(pc)  
* 11/2/2018    Chethana V        changes for Npd_weekly(business npd_weekly_BIZ)  
* 11/2/2018    Chethana V        changes for Npd_weekly(tablets npd_weekly_TAB) 
*2/12/2019     Chethana V        isolated NPD_TAB_CN from NPD_PC_CN 
* 5/28/2019    Chethana V        added  new source-GFK_brand  
* 10/22/2020    Chethana V        Changes added for NPD weekly PC and Tablet according new file format
* 11/16/2020   Chethana V        Changes added for NPD  monthly (npd_pc_cn,npd_pc_us,npd_tab_cn,npd_tab_us) according new file format
*11/25/2020    Sushravya KS      Inclusion of NPD Mexico File changes
*3/23/2021     Chethana V         changes added for NPD com dis and NPD com res  according to new file format
********************************************************************************************************************/   
DECLARE   
 @l_COLS_NM VARCHAR(MAX) = '',  
 @l_COLS_Final VARCHAR(MAX) = '',  
 @l_SQL_String NVARCHAR(MAX) = '',  
@l_SQL_String1 NVARCHAR(MAX) = '',      
 --@l_CNT INT = 0,  
 @l_total_col_cnt INT = 100,  
 @l_crtcl_cols_chk_return_code INT = 0,  
 @l_return_code int = 0,  
 @l_list_of_cols VARCHAR(8000) = '',  
 @l_file_type VARCHAR(100) = '',  
 @l_file_name VARCHAR(100) = '',-- '201001_AE_FUSION_GFK_INTEL',  
 @l_v2_eml_body VARCHAR(MAX)= '',  
 @l_dq_rcv_email_addr VARCHAR(2000),  
 @l_err_msg VARCHAR(MAX) = '',  
 @l_v2_subj_success VARCHAR(2000) = '',  
 @l_v2_subj_failure VARCHAR(2000) = '',  
 @l_env VARCHAR(500) = forecast.FN_GET_ENV(),  
 @l_server VARCHAR(500) = CONCAT(' Server: ' , ISNULL(@@SERVERNAME,'No Server Name')),  
 @l_stg_source VARCHAR(100) = '',  
 @l_ld_status VARCHAR(MAX) = '',  
 @l_usr_upld_file_nm VARCHAR(100) = '',  
 @l_upld_usr_nm VARCHAR(100) = '',  
 @l_upld_usr_email_addr VARCHAR(100) = '',  
 @l_email_title_ld_status VARCHAR(200) = '',  
 @l_dqmf_desc VARCHAR(2000) = '',  
 @l_SQL_Generate_ID_String VARCHAR(MAX) = '',  
 @AUTOCOMMENT VARCHAR(MAX) = '',  
 @l_fnl_src_vw_nm VARCHAR(500) = '',  
 @l_fnl_src_vw_nm_wo_schema VARCHAR(500) = '',  
 @l_stg_src_vw_nm VARCHAR(500) = '',  
 @l_stg_src_vw_nm_wo_schema VARCHAR(500) = '',  
 @l_dq_vw_nm VARCHAR(500) = '',  
 @l_ck_stg_nm_w_schema VARCHAR(500) = '',  
 @l_ck_stg_final_nm VARCHAR(500) = '',  
 @l_ck_stg_nm VARCHAR(500) = '',  
 @l_err_msg_header VARCHAR(100) = '',  
 @l_ck_dq_nm_w_schema VARCHAR(500) = '',  
 @l_crtical_error_flag CHAR(1) = 'N',  
 @l_load_data_flag_N INT = 0,  
 @l_load_data_flag_Y INT = 0,  
 @l_continue CHAR(1) = 'Y',  
 @l_publish_status VARCHAR(50) = '',  
 @l_file_list VARCHAR(MAX) = '',  
 @l_load_raw_files CHAR(1) = 'N',  
 @l_CNG_MAPPING_ID_CONSUMER_PRODUCT_VAL VARCHAR(MAX) = '',  
 @l_CNG_MAPPING_ID_RAW_PRODUCT_VAL VARCHAR(MAX) = '',  
 @l_CNG_MAPPING_ID_RAW_CHANNEL_VAL VARCHAR(MAX) = '',  
 @l_cmd_syntax VARCHAR(MAX) = '',  
    @in_cntnr_strt_tm DATETIME = GETDATE(),  
 @l_pkg_strt_tm DATETIME = GETDATE(),  
 @l_machine_nm varchar(100) = @@SERVERNAME,  
 @JOB_ID   int,  
 @l_vndr_file_nm VARCHAR(50) = '',   
 @l_email_hdr_nm VARCHAR(50) = '',  
 @l_del_Cnst VARCHAR(4000) = '',  
 @l_del_stmt varchar(4000) = '',  
 @l_cnst_nm varchar(50)='',  
 @l_del_col_nm varchar(50)='' ,  
 @l_dim_mapping_cprd_vndr varchar(50) = @in_file_type,  
 @l_dim_mapping_prd_vndr varchar(50)  = @in_file_type,  
 @l_dim_mapping_chnl_vndr varchar(50) = @in_file_type,  
 @l_dq_tech_ownr_email_addr VARCHAR(1000),  
 @l_dq_cnst_nm VARCHAR(100) = CONCAT('CNG_' , @in_file_type , '_DQ_CHK_ATRBS'),  
 @l_bypass_src_frmt_chk CHAR(1) = 'Y',  
 @l_ParmDefinition NVARCHAR(500) = '',  
 @l_rtn_val INT = 0,  
 @l_data_quality_check CHAR(1) = 'N',  
 @l_del_col VARCHAR(4000) = '',  
 @l_dummy_date varchar(20) = '01 1999',  
 @JOBID     INT,  
 @END_TM     DATETIME,  
 @ELAPSE_TM    TIME(7),   
 @FILE_TYPE    VARCHAR(100) = '',  
 @PROCES_TYPE   VARCHAR(50)  = 'Load',  
 @STATUS     VARCHAR(10)  = 'RUNNING',  
 @ERR_MSG    VARCHAR(MAX),  
 @GFK_SRC              VARCHAR(100)  
 IF @in_file_type IN ('GFK' ,'NPD_PC_CN')
 BEGIN  
 SELECT   
     @GFK_SRC = COALESCE(MAX(CASE WHEN ATRB.POS = 3 AND ATRB.CNST = @l_dq_cnst_nm THEN ATRB.ELEMENT END),' ')  
FROM   
 (  
  SELECT   
   POS,  
   ELEMENT,  
   @l_dq_cnst_nm CNST  
  FROM [forecast].[FN_SPLITCLR_MAX]  
  (  
   (  
    SELECT CNST_VAL   
    FROM forecast.CNST (NOLOCK)  
    WHERE CNST_NM = @l_dq_cnst_nm  
   ),  
   '|'  
  ) DQ_CHK_ATRB  
 ) ATRB    
LEFT OUTER JOIN forecast.vw_STG_WorkerPublicExtended cdis (NOLOCK)  
ON (cdis.upperIDSID = ATRB.ELEMENT AND ATRB.POS = 1 AND ATRB.CNST = @l_dq_cnst_nm)  
 IF @GFK_SRC='GFK-BRAND'  
       BEGIN   
          SET @IN_FILE_TYPE='GFK_BRAND'  
       END  
 ELSE IF  @GFK_SRC='GFK'  
      BEGIN
          SET @IN_FILE_TYPE='GFK'  
       END  
ELSE IF   @GFK_SRC='NPD_PC_MXN'  
      BEGIN
           SET @IN_FILE_TYPE='NPD_PC_MXN'  
     END 
ELSE  
     BEGIN
     SET  @IN_FILE_TYPE='NPD_PC_CN'  
     END
END
IF @in_file_type LIKE ('NPD%')   
 OR @in_file_type IN ('IDC_TAB','IDC_FCST','IDC_SVR','IDC_X86','EMEA_DISCOUNTER','CTX')  
 OR @in_file_type LIKE ('AMSC%')  
  OR @in_file_type LIKE('NPD_WEEKLY_BIZ')
  OR @in_file_type LIKE('GFK_BRAND')    
BEGIN  
 SELECT   
  @l_vndr_file_nm = CNST_VAL,  
  @l_email_hdr_nm = CNST_DEF  
 FROM   
  DF.FORECAST.CNST with (NOLOCK)  
 WHERE   
  CNST_NM  = CONCAT('CNG_',@in_file_type,'_FILE_NM_ATRBS')  
 SELECT @l_dim_mapping_cprd_vndr = CASE WHEN @in_file_type like 'NPD_PC%' THEN @in_file_type  
             WHEN @in_file_type = 'NPD_TAB_US' THEN 'NPD_TAB_US'  
              WHEN @in_file_type = 'NPD_TAB_CN' THEN 'NPD_TAB_CN'  
             WHEN @in_file_type IN ('AMSC_US','AMSC_CANADA') THEN 'APMSC_AMSC'  
             WHEN @in_file_type = 'AMSC_LAR' THEN 'AMSC_EMS'  
           WHEN @in_file_type IN('NPD_WEEKLY_BIZ')THEN 'NPD_WEEKLY'      
            WHEN @in_file_type like 'NPD_WEEKLY_TAB' THEN 'NPD_WEEKLY_TAB'    
            ELSE @in_file_type  
           END  
 SELECT @l_dim_mapping_prd_vndr =  CASE WHEN @in_file_type like 'NPD_PC%' THEN 'NPD_PC'  
                        WHEN @in_file_type = 'NPD_TAB_US' THEN 'NPD_TAB_US'  
              WHEN @in_file_type = 'NPD_TAB_CN' THEN 'NPD_TAB_CN'  
               WHEN @in_file_type IN('NPD_weekly_biz') THEN 'NPD_weekly'   
            WHEN @in_file_type like 'NPD_WEEKLY_TAB' THEN 'NPD_WEEKLY_TAB'    
             ELSE @in_file_type  
           END  
 SELECT @l_dim_mapping_chnl_vndr =  CASE when  @in_file_type IN ('NPD_WEEKLY_BIZ') then 'npd_weekly'    
          WHEN @in_file_type like 'NPD_WEEKLY_TAB' THEN 'NPD_WEEKLY_TAB'    
          WHEN  @in_file_type like 'NPD_%' AND @in_file_type <> 'NPD_COM_DIS' AND @in_file_type <> 'NPD_COM_RES'     
           AND @in_file_type <> 'NPD_WEEKLY'   
            THEN 'NPD'  
            ELSE @in_file_type END  
END  
ELSE  
BEGIN  
 SET @l_email_hdr_nm = @in_file_type  
 SELECT @l_dim_mapping_cprd_vndr = CASE WHEN @in_file_type = 'APMSC' THEN 'APMSC_AMSC'  
           WHEN @in_file_type = 'EMS' THEN 'AMSC_EMS'  
           ELSE @in_file_type  
          END  
END  
--INSERT VALUE IN TABLE CNG_AUDIT_PROCESS  
SET @FILE_TYPE= @in_file_type  
EXEC [FORECAST].[PR_CNG_INSERT_CNG_AUDIT_PROCESS_ENTRY]   
     @JOBID output  
    ,@END_TM  
    ,@ELAPSE_TM  
    ,@FILE_TYPE  
    ,@PROCES_TYPE  
    ,@STATUS  
    ,@ERR_MSG  
EXEC forecast.PR_PRINTD 'Procedure forecast.PR_CNG_LD_CK_TBL_FROM_STG Started.....'  
SET @l_fnl_src_vw_nm = CONCAT('forecast.VW_CNG_' , @in_file_type)  
SET @l_stg_src_vw_nm = CONCAT( @l_fnl_src_vw_nm , '_STG')  
SET @l_fnl_src_vw_nm_wo_schema = REPLACE(@l_fnl_src_vw_nm,'forecast.','')  
SET @l_stg_src_vw_nm_wo_schema = REPLACE(@l_stg_src_vw_nm,'forecast.','')  
SET @l_ck_stg_nm_w_schema = CONCAT('[DF_DENORM].[forecast].[CK_CNG_' , @in_file_type , '_STG]')  
SET @l_ck_stg_nm = CONCAT('CK_CNG_' , @in_file_type , '_STG')  
SET @l_ck_stg_final_nm =  CONCAT('[DF_DENORM].[forecast].[CK_CNG_' , @in_file_type , ']')  
SET @l_ck_dq_nm_w_schema = CONCAT('[DF_DENORM].[forecast].[CK_CNG_' , @in_file_type , '_DQ]')  
SET @l_dq_vw_nm = CONCAT('forecast.VW_CNG_' , @in_file_type , '_DQ')  
SET  @l_cnst_nm  = CONCAT(@in_file_type,'_FNL_STG_CK_DEL_JOIN')  
SET    @l_del_col_nm = CONCAT(@in_file_type,'_FNL_STG_CK_DEL_COL')  
-- This gets VNDR Attributes  
-- 1 -- User Name & Email who uploaded the file  
-- 2 -- Bypass DQ Flag  
SELECT   
 @l_bypass_src_frmt_chk = COALESCE(MAX(CASE WHEN ATRB.POS = 2 AND ATRB.CNST = @l_dq_cnst_nm THEN ATRB.ELEMENT END),'N')  
 ,@l_upld_usr_nm = COALESCE(MAX(CASE WHEN ATRB.POS = 1 AND ATRB.CNST = @l_dq_cnst_nm THEN cdis.ccMailName END),'')  
 ,@l_upld_usr_email_addr = COALESCE(MAX(CASE WHEN ATRB.POS = 1 AND ATRB.CNST = @l_dq_cnst_nm THEN cdis.DomainAddress END),'')  
FROM   
 (  
  SELECT   
   POS,  
   ELEMENT,  
   @l_dq_cnst_nm CNST  
  FROM [forecast].[FN_SPLITCLR_MAX]  
  (  
   (  
    SELECT CNST_VAL   
    FROM forecast.CNST (NOLOCK)  
    WHERE CNST_NM = @l_dq_cnst_nm  
   ),  
   '|'  
  ) DQ_CHK_ATRB  
 ) ATRB    
LEFT OUTER JOIN forecast.vw_STG_WorkerPublicExtended cdis (NOLOCK)  
ON (cdis.upperIDSID = ATRB.ELEMENT AND ATRB.POS = 1 AND ATRB.CNST = @l_dq_cnst_nm)  
SET @l_upld_usr_nm = IIF(COALESCE(@l_upld_usr_nm,'') = '','CNG',@l_upld_usr_nm)  
SET @l_upld_usr_email_addr = IIF(COALESCE(@l_upld_usr_email_addr,'') = '','cng@intel.com',@l_upld_usr_email_addr)  
-- This gets Email Addresses from DQMF to send out success / failure message  
SELECT   
 @l_dq_rcv_email_addr = IIF(forecast.FN_GET_ENV() = 'PROD',DUCM.RCV_EMAIL_ADDR,DUCM.PRE_PRD_RCV_EMAIL_ADDR),  
 @l_dqmf_desc = DUC.dq_use_case_dsc ,  
 @l_dq_tech_ownr_email_addr = CONCAT(DUCM.DQ_TECH_OWNR_EMAIL_ADDR , ';' , ISNULL(@l_upld_usr_email_addr,''))  
FROM   
 DF_DQMF.dbo.DQ_USE_CASE_MDATA DUCM  
INNER JOIN DF_DQMF.dbo.dq_use_case DUC  
ON DUCM.DQ_USE_CASE_CD = DUC.DQ_USE_CASE_CD   
WHERE  
 DUCM.DQ_USE_CASE_CD = 'DQ_CNG_STAGING_' + @in_file_type  
SET @l_v2_subj_failure = CONCAT(@l_env , ' : CNG : Data issues info: ' , @l_dqmf_desc , ': ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)  
CREATE TABLE #tmpIncludeInSuccessEmail  
(  
 MSG_TXT VARCHAR(MAX)  
)  
CREATE TABLE #tmp_LIST_OF_COLS  
(  
 COL_NUM VARCHAR(10),   
 COL_NM VARCHAR(400),  
 COL_NM_REPLACE_QUOTE VARCHAR(400),  
 COL_NM_FOR_KEY_COL_CHK VARCHAR(400),  
 COL_NM_WITH_ALIAS VARCHAR(400),  
 COL_NM_WITH_CAST_AND_CNVRSN VARCHAR(400)  
)     
CREATE TABLE #tmp_FILE_NM  
(  
 FILE_NM VARCHAR(100)  
 ,FILE_REC_CNT INT  
)    
BEGIN TRY  
 SET @l_total_col_cnt = 100  
 SET @l_stg_source = 'DF_STG.FORECAST.STG_LD_EXT_DATA_CONS'  
 SET @l_SQL_String = CONCAT('TRUNCATE TABLE [DF_DENORM].[FORECAST].[CK_CNG_',@in_file_type,'_DQ]',CHAR(13))  
 SET @l_SQL_String += REPLACE(@l_SQL_String,'_DQ','_STG')  
 IF @in_debug = 'Y'  
  EXEC forecast.PR_PRINTMAX @l_SQL_String  
 ELSE   
  EXEC SP_EXECUTESQL @l_SQL_String  
 SELECT    
  @l_list_of_cols = CONCAT(@l_list_of_cols, ',COL' , CAST(N AS VARCHAR(3)))  
 FROM   
  [dbo].[Nums]  
 WHERE   
  N <= @l_total_col_cnt  
 SET @l_list_of_cols = STUFF(@l_list_of_cols,1,1,'')  
 IF @in_debug = 'Y' BEGIN  
  PRINT 'List of Columns....'  
  EXEC forecast.PR_PRINTMAX @l_list_of_cols  
  PRINT '..............................................................................'  
 END  
 SET @l_SQL_String = CONCAT( 'SELECT FILE_NM, COUNT(*) FILE_REC_CNT FROM ',@l_stg_source , ' (NOLOCK) WHERE ROW_TYPE = ' , '''' , 'D' ,'''',' AND FILE_NM like ','''','%',  
        IIF(@in_file_type IN ('GFK','IDC','APMSC','EMS'),@in_file_type,@l_vndr_file_nm),'%','''', CHAR(13) , CHAR(9) , 'GROUP BY FILE_NM' ,CHAR(13),CHAR(9) )  
 IF @in_debug = 'Y'  
  EXEC forecast.PR_PRINTMAX @l_SQL_String  
 INSERT INTO #tmp_FILE_NM  
 EXEC sp_executesql @l_SQL_String  
 DECLARE CUR_C1 CURSOR FAST_FORWARD FOR   
 SELECT FILE_NM FROM #tmp_FILE_NM  
 OPEN CUR_C1  
 WHILE(1=1)      
 BEGIN    
  FETCH NEXT FROM CUR_C1 INTO  @l_file_name  
   IF (@@FETCH_STATUS <> 0)   
   BREAK     
   PRINT '-----------------------------------------------------------------------------------------'  
   PRINT 'File Name = ' + @l_file_name
   Update forecast.cnst set CNST_VAL=@l_file_name  where CNST_NM = 'CNG_FILE_LOAD_FileName'  
   PRINT '-----------------------------------------------------------------------------------------'  
   IF @in_debug = 'Y'  
    SELECT @l_file_name  
   SET @l_COLS_Final = ''  
   SET @l_COLS_NM = ''  
   SET @l_usr_upld_file_nm = @l_file_name  
   SET @l_SQL_String = CONCAT('   
    SELECT   
     Cols COL_NUM,  
     CONCAT(''['',LTRIM(RTRIM(COL_NM)),'']'') COL_NM,  
     forecast.FN_CNG_GET_COL_CASTING(''' , @in_file_type , ''',Cols,LTRIM(RTRIM(COL_NM)),''COL_NM_WITH_REPLACE_QUOTENAME'') COL_NM_REPLACE_QUOTE,  
     forecast.FN_CNG_GET_COL_CASTING(''' , @in_file_type , ''',Cols,LTRIM(RTRIM(COL_NM)),''REPLACE_ONLY'') COL_NM_FOR_KEY_COL_CHK,  
     forecast.FN_CNG_GET_COL_CASTING(''' , @in_file_type , ''',Cols,LTRIM(RTRIM(COL_NM)),''COL_NM_WITH_ALIAS'') COL_NM_WITH_ALIAS  
    FROM   
       (  
     SELECT ' , @l_list_of_cols , '  
     FROM ' , @l_stg_source , '  
     WHERE   
      ROW_TYPE = ''H''  
     AND FILE_NM = ''' , @l_file_name , '''  
     ) PVT  
    UNPIVOT  
       (COL_NM FOR Cols IN   
       ( ' , @l_list_of_cols , '  
       )  
    ) AS UNPVT;')  
  IF @in_debug = 'Y'  
    EXEC forecast.PR_PRINTMAX @l_SQL_String  
   INSERT INTO #tmp_LIST_OF_COLS(COL_NUM,COL_NM,COL_NM_REPLACE_QUOTE,COL_NM_FOR_KEY_COL_CHK,COL_NM_WITH_ALIAS)  
   EXEC sp_executesql @l_SQL_String  
   --SELECT * FROM #tmp_LIST_OF_COLS  
   -- delete any column header if it only empty space  
   DELETE FROM #tmp_LIST_OF_COLS   
   WHERE COL_NM = ''  
   IF @in_debug = 'Y'   
    SELECT * FROM #tmp_LIST_OF_COLS  
   ELSE IF @in_debug = 'N'  
   BEGIN  
    SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CRTCL_COLS_CHK ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_file_name,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''))  
       ,@l_pkg_strt_tm = GETDATE()  
    EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
     @PKG_NM = @l_cmd_syntax,     
     @MACHINE_NM = @l_machine_nm,      
     @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
     @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
     @PACKAGE_END_TM = NULL,     
     @DATA_DIR = NULL,     
     @ERR_FILE_DIR = NULL,     
     @ROWS_INSERTED = NULL,     
     @ROWS_REJECTED = NULL,    
     @JOB_ID = @JOB_ID OUTPUT,  
     @RUN_STS = 'RUNNING',
	 @MODULE ='CNG';     
    EXECUTE @l_crtcl_cols_chk_return_code = forecast.PR_CNG_DQ_CRTCL_COLS_CHK @in_file_type, @l_file_name, @l_bypass_src_frmt_chk  
    EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG' 
   END  
   --PR_CNG_DQ_CRTCL_COLS_CHK WILL RETURN CODE 2 FOR MISSING COLUMNS AND WE WILL DELETE COLS AND STG_EXT_DATA TABLE SO THAT ROW_ID GENERATION WONT THROW ERROR BELOW.  
   IF (@l_crtcl_cols_chk_return_code = 2)  
   BEGIN  
    TRUNCATE TABLE #tmp_LIST_OF_COLS  
    SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DEL_STG_DATA ',QUOTENAME(@l_file_name,''''))  
    EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   END  
   ELSE  
   BEGIN   
    SELECT   
     @l_COLS_Final +=  CONCAT(',' , COL_NM_WITH_ALIAS , CHAR(13) , CHAR(9) , CHAR(9)),  
     @l_COLS_NM   += CONCAT(',' , COL_NM_REPLACE_QUOTE , CHAR(13) , CHAR(9) , CHAR(9))  
    FROM #tmp_LIST_OF_COLS  
    SELECT @l_COLS_Final = STUFF(@l_COLS_Final,1,1,'')  
    , @l_COLS_Final = CONCAT(CHAR(13) , CHAR(9) , CHAR(9) , @l_COLS_Final)  
    , @l_COLS_NM = STUFF(@l_COLS_NM,1,1,'')  
    , @l_COLS_NM = CONCAT (CHAR(13) , CHAR(9) , CHAR(9) , @l_COLS_NM)  
    IF @in_debug = 'Y'   
    BEGIN  
     PRINT '@l_COLS_Final = _____________________________________'  
     PRINT '-----------------------------------------------------'   
     EXECUTE forecast.PR_PRINTMAX @l_COLS_Final  
     PRINT '@l_COLS_NM = _____________________________________'  
     PRINT '-----------------------------------------------------'   
     EXECUTE forecast.PR_PRINTMAX @l_COLS_NM  
    END  
    SET @l_sql_string = CONCAT('  
    DECLARE   
     @l_stg_tbl_nm VARCHAR(200) = ','''',@l_ck_stg_nm_w_schema,'''')  
    SET @l_SQL_String += CONCAT('SELECT FILE_NM,ROW_ID,',case WHEN @in_file_type LIKE 'NPD_WEEKLY%' then'cast(col1  as varchar(200))as weeks,'end
     ,@l_COLS_Final   
     ,CASE WHEN @in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_CN%'  
   THEN ',CRE_DT,cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,CAST(''CANADA'' AS VARCHAR(200)) AS COUNTRY'  
       WHEN @in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_Retail%'  
       THEN ',CRE_DT,cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,CAST(''United States'' AS VARCHAR(200)) AS COUNTRY'  
       WHEN @in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_MXRetail%'  
   THEN ',CRE_DT,cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,CAST(''MEXICO'' AS VARCHAR(200)) AS COUNTRY,CAST(Col8 as NUMERIC(18,2)) AS DOLLARS' 
       WHEN @in_file_type = 'NPD_COM_DIS' AND @l_file_name LIKE '%MO_DIS_PC%'  
	 THEN  ',cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,
	   CRE_DT,CAST(''United States'' AS VARCHAR(200)) AS COUNTRY'  
       WHEN @in_file_type = 'NPD_COM_RES' AND @l_file_name LIKE '%MO_CR_PC%'  
       THEN ',cast(''Not Applicable'' as varchar(200))as DISCHANN,
	   cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,
	   CRE_DT,CAST(''United States'' AS VARCHAR(200)) AS COUNTRY'  
       WHEN @in_file_type LIKE 'NPD_WEEKLY_BIZ' AND @l_file_name  LIKE '%WK_BIZ%'    
              THEN ',CRE_DT,CAST(''USA'' AS VARCHAR(200)) AS COUNTRY'      
         WHEN @in_file_type ='NPD_WEEKLY'      
       THEN ',CRE_DT,cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,CAST(''USA'' AS VARCHAR(200)) AS COUNTRY' 
      WHEN @in_file_type LIKE 'NPD_WEEKLY_TAB' AND @l_file_name  LIKE '%Wk_RetailTablets%'      
            THEN ',CRE_DT,cast( CASE WHEN LEFT(COL1,3) IN (''Jan'',''Feb'',''Mar'')   
            THEN  CONCAT(''Quarter 1 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Apr'', ''May'', ''Jun'')
		    THEN CONCAT(''Quarter 2 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Jul'', ''Aug'', ''Sep'')
		    THEN CONCAT(''Quarter 3 '',RIGHT(COL1,4))
			WHEN LEFT(COL1,3) In (''Oct'', ''Nov'', ''Dec'')
		    THEN CONCAT(''Quarter 4 '',RIGHT(COL1,4))
	   ELSE ''INVALID QUARTER''
	   END AS varchar(200)) as  HQUARTER,cast(RIGHT(COL1,4)AS varchar(200)) as  ANNUAL,CAST(''USA'' AS VARCHAR(200)) AS COUNTRY' 
      ELSE ',CRE_DT'  
     END,'  
     INTO #tmp_CNG_HANDSET_FOR_FNL_CK_TBL  
     FROM ' , @l_stg_source , '   
     WHERE ROW_TYPE = ''D''  
     AND FILE_NM = ''' , @l_file_name , '''' , CHAR(13) , CHAR(9) , CHAR(9) )  
  SET @l_SQL_String += IIF((@in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_CN%') OR (@in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_Retail%')OR (@in_file_type LIKE 'NPD%' AND @l_file_name LIKE '%MO_MXRetail%') 
  OR (@in_file_type LIKE 'NPD_COM%' AND @l_file_name LIKE '%MO_DIS_PC%') OR (@in_file_type LIKE 'NPD_COM%' AND @l_file_name LIKE '%MO_CR_PC%'),
CONCAT('UPDATE #tmp_CNG_HANDSET_FOR_FNL_CK_TBL
	  SET TIMEPER=cast(CASE WHEN LEFT(TIMEPER,3)= ''JAN'' THEN CONCAT(''January'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Feb'' THEN CONCAT(''February'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Mar'' THEN CONCAT(''March'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Apr'' THEN CONCAT(''April'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''May'' THEN CONCAT(''May'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Jun'' THEN CONCAT(''June'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Jul'' THEN CONCAT(''July'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Aug'' THEN CONCAT(''August'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Sep'' THEN CONCAT(''September'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Oct'' THEN CONCAT(''October'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Nov'' THEN CONCAT(''November'','' '',RIGHT(TIMEPER,4))
	WHEN LEFT(TIMEPER,3)= ''Dec'' THEN CONCAT(''December'','' '',RIGHT(TIMEPER,4))
	ELSE TIMEPER END AS VARCHAR(200))
	WHERE LEFT(TIMEPER,3) in (''JAN'',''FEB'',''MAR'',''APR'',''MAY'',''Jun'',''Jul'',''Aug'',''Sep'',''Oct'',''Nov'',''DEC'')',CHAR(13),CHAR(9)),'')
    SET @l_SQL_String += IIF(@in_file_type LIKE'NPD%' OR @in_file_type LIKE ('AMSC%') OR @in_file_type IN ('APMSC','EMS','EMEA_DISCOUNTER'),CONCAT('ALTER TABLE #tmp_CNG_HANDSET_FOR_FNL_CK_TBL ALTER COLUMN ROW_ID BIGINT NOT NULL ',CHAR(13) , CHAR(9)),'')  
	SET @l_SQL_String += IIF(@in_file_type LIKE'NPD_PC_MXN%',CONCAT('UPDATE #tmp_CNG_HANDSET_FOR_FNL_CK_TBL SET DOLLARS=a.PESOS*b.exchange_rate_avg from #tmp_CNG_HANDSET_FOR_FNL_CK_TBL a INNER JOIN 
	DF_STG.[forecast].[VW_CNG_CUR_EXCHG_RTE] b ON a.TIMEPER=b.TIMEPER',CHAR(13) , CHAR(9)),'') 
     SET @l_SQL_String += IIF(@in_file_type LIKE'NPD_weekly%' ,CONCAT('update  tmp  set WEEKS=concat(right(cng.month,02),''/'',substring(tmp.timeper,5,2),''/'',substring(tmp.timeper,8,4)),timeper=concat(cng.month_name,'' '',substring(tmp.timeper,8,4)) from #tmp_CNG_HANDSET_FOR_FNL_CK_TBL tmp inner join
df.forecast.v_cng_time cng on cng.year=substring(tmp.timeper,8,4) and left(cng.month_name,03)=left(tmp.timeper,03)',CHAR(13), CHAR(9)),'')  
    SET @l_SQL_String += CONCAT('declare @l_recreate varchar(1) = ''N'' ' , CHAR(13) , CHAR(9) , CHAR(9) ,  
         'IF OBJECT_ID(' , '''', @l_ck_stg_nm_w_schema ,'''', ') IS NULL ' , CHAR(13) , CHAR(9) , CHAR(9) ,  
          ' SET @l_recreate =''Y'' ')  
    SET @l_SQL_String += 'EXEC forecast.PR_CREATE_CK_TBL''#tmp_CNG_HANDSET_FOR_FNL_CK_TBL'',@l_stg_tbl_nm,NULL,NULL,''N'',@l_recreate '  
    SET @l_SQL_String += CONCAT('   
    INSERT INTO ',@l_ck_stg_nm_w_schema   
    ,'(FILE_NM, ROW_ID,'
      ,iif(@in_file_type like 'npd_weekly%','weeks,','')
      ,@l_COLS_NM,' ,CRE_DT'  
    ,IIF(@in_file_type = 'IDC_FCST', ',PERIOD','')  
	,IIF(@in_file_type ='NPD_COM_RES', ',DISCHANN','')
    ,IIF(@in_file_type in ('NPD_pc_cn','npd_pc_us','npd_tab_cn','npd_tab_us','NPD_COM_RES','NPD_COM_DIS'), ',HQUARTER,ANNUAL,COUNTRY','')  
	,IIF(@in_file_type in ('npd_pc_mxn'), ',HQUARTER,ANNUAL,COUNTRY,DOLLARS','') 
    ,IIF(@in_file_type LIKE'NPD_weekly%', ',HQUARTER,ANNUAL,COUNTRY','')
    ,IIF(@in_file_type IN ('IDC','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR', 'GFK', 'GFK_BRAND','NPD_PC_US', 'NPD_PC_CN', 'NPD_TAB_US', 'NPD_TAB_CN','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB'), ',NETBK_TM','')  
    ,IIF(@in_file_type IN ( 'IDC', 'GFK','GFK_BRAND', 'NPD_PC_US', 'NPD_PC_CN', 'NPD_TAB_US', 'NPD_TAB_CN','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES' ,'NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB'), ',NETBK_FF','') ,')')  
	SET @l_SQL_String += CONCAT('  
      SELECT   
       FILE_NM,'  
       ,CASE  WHEN @in_file_type IN ('NPD_PC_US','NPD_TAB_CN','NPD_TAB_US','NPD_COM_DIS','NPD_COM_RES')  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL),  
              IIF(ISDATE(TIMEPER) = 0,''99'',RIGHT(CONCAT(''0'',MONTH(CONCAT(''01 '',TIMEPER))),2)),  
              ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL),  
              IIF(ISDATE(TIMEPER) = 0,''DECEMBER 99'',TIMEPER) ORDER BY IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL))  
             ) ROW_ID,'  
          WHEN @in_file_type IN ('NPD_PC_CN','NPD_PC_MXN')  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL),  
              IIF(ISDATE(TIMEPER) = 0,''99'',RIGHT(CONCAT(''0'',MONTH(CONCAT(''01 '',TIMEPER))),2)),  
              ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL),  
              IIF(ISDATE(TIMEPER) = 0,''DECEMBER 99'',TIMEPER) ORDER BY IIF(TRY_CONVERT(INT, ANNUAL) IS NULL,''9999'',ANNUAL))  
             ) ROW_ID,'  
          WHEN @in_file_type IN ('APMSC','AMSC_US','AMSC_CANADA')  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(ISDATE(CONCAT(MONTH,'' 9999'')) =0 ,''99'',RIGHT(CONCAT(''0'',MONTH(CONCAT(MONTH,'' 9999''))),2))  
              ,ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(ISDATE(CONCAT(MONTH,'' 9999'')) =0 ,''99'',RIGHT(CONCAT(''0'',MONTH(CONCAT(MONTH,'' 9999''))),2))  
              ORDER BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR))  
             ) ROW_ID,'  
          WHEN @in_file_type ='EMS'  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(MONTH != CONCAT(YEAR,''M'',SUBSTRING(MONTH,6,7)),''99'',SUBSTRING(MONTH,6,7))  
              ,ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(MONTH != CONCAT(YEAR,''M'',SUBSTRING(MONTH,6,7)),''99'',SUBSTRING(MONTH,6,7))  
              ORDER BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR))  
             ) ROW_ID,'  
          WHEN @in_file_type IN ('AMSC_LAR')  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(REPLACE(QUARTER,''Q'','''') BETWEEN 1 AND 4,REPLACE(QUARTER,''Q'',''''),9)  
              ,ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR)  
              ,IIF(REPLACE(QUARTER,''Q'','''') BETWEEN 1 AND 4,REPLACE(QUARTER,''Q'',''''),9)  
              ORDER BY IIF(TRY_CONVERT(INT, YEAR) IS NULL,''9999'',YEAR))  
             ) ROW_ID,'  
          WHEN @in_file_type IN ('EMEA_DISCOUNTER')  
          THEN 'CONCAT(  
              IIF(TRY_CONVERT(INT, LEFT(PERIOD,4)) IS NULL,''9999'',LEFT(PERIOD,4))  
              ,IIF(ISDATE(CONCAT(REPLACE(PERIOD,''-'',''''),''01'')) = 0,''99'',RIGHT(PERIOD,2))  
              ,ROW_NUMBER() OVER (PARTITION BY IIF(TRY_CONVERT(INT, LEFT(PERIOD,4)) IS NULL,''9999'',LEFT(PERIOD,4))  
              ,IIF(ISDATE(CONCAT(REPLACE(PERIOD,''-'',''''),''01'')) = 0,''99'',RIGHT(PERIOD,2))  
              ORDER BY IIF(TRY_CONVERT(INT, LEFT(PERIOD,4)) IS NULL,''9999'',LEFT(PERIOD,4)))  
             ) ROW_ID,'  
          ELSE 'ROW_ID,'  
       END,  
       IIF(@in_file_type LIKE 'NPD_weekly%', 'weeks,','') , 
         @l_COLS_NM  
         ,', CRE_DT'  
         ,IIF(@in_file_type = 'IDC_FCST',  ',CAST(CASE WHEN LTRIM(RTRIM(QUARTER)) ='''' THEN YEAR ELSE QUARTER END AS VARCHAR(200))','')  
		 ,IIF(@in_file_type ='NPD_COM_RES', ',DISCHANN','')
         ,IIF(@in_file_type in ('NPD_pc_cn','npd_pc_us','npd_tab_cn','npd_tab_us','NPD_COM_RES','NPD_COM_DIS'), ',HQUARTER,ANNUAL,COUNTRY','')  
		 ,IIF(@in_file_type in ('NPD_PC_MXN'), ',HQUARTER,ANNUAL,COUNTRY,DOLLARS','')  
         ,IIF(@in_file_type LIKE 'NPD_weekly%', ',HQUARTER,ANNUAL,COUNTRY','')
          ,CASE WHEN @in_file_type = 'IDC'   
         THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, SUBSTRING(QUARTER,1,4)),''9999'') ,COALESCE(TRY_CONVERT(INT,SUBSTRING(QUARTER,6,1)),''99'')) <= 20142,''Y'', ''N'') AS CHAR(1)) AS NETBK_TM'  
         WHEN @in_file_type IN ('APMSC','AMSC_US','AMSC_CANADA','AMSC_LAR')  
         THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, YEAR),''9999'') ,COALESCE(TRY_CONVERT(INT,SUBSTRING(QUARTER,2,1)),''99'')) <= 20142,''Y'', ''N'') AS CHAR(1)) AS NETBK_TM'  
         WHEN @in_file_type IN ('EMS')  
         THEN ',CAST(IIF(COALESCE(TRY_CONVERT(INT, REPLACE(QUARTER, ''Q'', '''')),''9999.99'') <= 201402,''Y'', ''N'') AS CHAR(1)) AS NETBK_TM'  
         WHEN @in_file_type IN ('GFK','GFK_BRAND')  
         THEN ',CAST(CASE WHEN CONCAT(COALESCE(TRY_CONVERT (INT , SUBSTRING(PERIOD, 1,4)), 9999), CHOOSE(COALESCE(TRY_CONVERT (INT , SUBSTRING(PERIOD, 6,2)), 12),''1'',''1'',''1'',''2'',''2'',''2'',''3'',''3'',''3'',''4'',''4'',''4'') ) <= 20142  THEN ''Y'' ELSE ''N'' END AS CHAR(1)) AS NETBK_TM'  
         WHEN @in_file_type IN ('NPD_PC_US', 'NPD_TAB_US', 'NPD_TAB_CN','NPD_COM_DIS','NPD_COM_RES')  
         THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, ANNUAL), 9999) ,COALESCE(TRY_CONVERT(INT, SUBSTRING(HQUARTER,9,1)), 5))<= 20142,''Y'', ''N'' ) AS CHAR(1)) AS NETBK_TM'  
         WHEN @in_file_type IN ('NPD_PC_CN','NPD_PC_MXN')  
         THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, ANNUAL), 9999) ,COALESCE(TRY_CONVERT(INT, SUBSTRING(HQUARTER,9,1)), 5))<= 20142,''Y'', ''N'' ) AS CHAR(1)) AS NETBK_TM'           
         WHEN @in_file_type IN ('NPD_WEEKLY_BIZ')      
          THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, ANNUAL), 9999) ,COALESCE(TRY_CONVERT(INT, SUBSTRING(HQUARTER,9,1)), 5))<= 20142,''Y'', ''N'' ) AS CHAR(1)) AS NETBK_TM'      
          WHEN @in_file_type IN ('NPD_WEEKLY')    
         THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, ANNUAL), 9999) ,COALESCE(TRY_CONVERT(INT, SUBSTRING(HQUARTER,9,1)), 5))<= 20142,''Y'', ''N'' ) AS CHAR(1)) AS NETBK_TM'    
         WHEN @in_file_type IN ('NPD_WEEKLY_TAB')    
           THEN ',CAST(IIF(CONCAT(COALESCE(TRY_CONVERT(INT, ANNUAL), 9999) ,COALESCE(TRY_CONVERT(INT, SUBSTRING(HQUARTER,9,1)), 5))<= 20142,''Y'', ''N'' ) AS CHAR(1)) AS NETBK_TM'    
         ELSE ''   
       END  
       ,CASE WHEN @in_file_type = 'IDC'   
        THEN ',CAST(CASE WHEN (PRODUCT_CATEGORY=''Portable PC'' AND PROCESSOR_VENDOR = ''INTEL'' AND PROCESSOR_BRAND = ''ATOM'') OR  
             (PRODUCT_CATEGORY=''Portable PC'' AND PROCESSOR_VENDOR != ''INTEL'' AND PRODUCT = ''Mini Notebook PC'')   
         THEN ''Netbook'' ELSE '''' END AS VARCHAR(200)) AS NETBK_FF'  
        WHEN @in_file_type IN('GFK','GFK_BRAND')   
        THEN ',CAST(IIF(COALESCE(TRY_CONVERT(DECIMAL(5,1),DISPLAY_SIZE), 99.9) <= 12.1 AND PROCESSOR IN (''C-SERIES'',''ATOM''), ''Netbook'' , '''') AS VARCHAR(200)) AS NETBK_FF'  
        WHEN @IN_FILE_TYPE LIKE 'NPD_PC%' OR @IN_FILE_TYPE in ( 'NPD_COM_DIS' , 'NPD_COM_RES')  
        THEN ',CAST(IIF(COALESCE(TRY_CONVERT(DECIMAL(5,1) , [forecast].[FN_CNG_CONVERT_UNITS_2_NUMERIC](DISPSIZ )), 99.9) <= 12.1 AND PCTECFAM IN (''AMD C'',''ATOM'') , ''Netbook'' , '''') AS VARCHAR(200)) AS NETBK_FF'  
        WHEN @IN_FILE_TYPE LIKE 'NPD_TAB%' THEN  ',CAST('''' AS VARCHAR(200)) AS NETBK_FF'  
        WHEN @in_file_type IN('NPD_WEEKLY_BIZ')     
            THEN ',CAST(IIF(COALESCE(TRY_CONVERT(DECIMAL(5,1), SUBSTRING(DISPSIZ , 1, 5)), 99.9) <= 12.1 AND PCTECFAM IN (''AMD C'',''ATOM'') , ''Netbook'' , '''') AS VARCHAR(200))  AS       
                NETBK_FF'      
           WHEN @in_file_type IN('NPD_WEEKLY')     
       THEN ',CAST(IIF(COALESCE(TRY_CONVERT(DECIMAL(5,1), SUBSTRING(DISPSIZ , 1, 5)), 99.9) <= 12.1 AND PCTECFAM IN (''AMD C'',''ATOM'') , ''Netbook'' , '''') AS VARCHAR(200))  AS     
        NETBK_FF'   
       WHEN @in_file_type IN('NPD_WEEKLY_TAB')     
       THEN ',CAST(IIF(COALESCE(TRY_CONVERT(DECIMAL(5,1), SUBSTRING(DISPSIZ , 1, 5)), 99.9) <= 12.1  , ''Netbook'' , '''') AS VARCHAR(200))  AS     
       NETBK_FF'    
        END  
        ,' FROM #tmp_CNG_HANDSET_FOR_FNL_CK_TBL CK'  
        , CHAR(13) , CHAR(9) , CHAR(9)  )  
    SET @l_SQL_String += CONCAT('  
    IF OBJECT_ID(' , '''', 'tempdb..#tmp_CNG_HANDSET_FOR_FNL_CK_TBL','''', ') IS NOT NULL ' , CHAR(13) , CHAR(9) , CHAR(9) ,  
    'DROP TABLE #tmp_CNG_HANDSET_FOR_FNL_CK_TBL' , CHAR(13) , CHAR(9) , CHAR(9))  
    IF @in_debug = 'Y'  
     EXEC forecast.PR_PRINTMAX @l_SQL_String  
    ELSE   
     EXEC SP_EXECUTESQL @l_SQL_String  
if @in_file_type in ('npd_weekly_biz')           
 set @l_SQL_String1 += concat('delete  ck from df_denorm.forecast.ck_cng_npd_weekly ck inner join ',@l_ck_stg_nm_w_schema,'stg on     
 IIF(isdate(ck.weeks) =1 , CK.WEEKS,convert(varchar(10),dateadd(week,substring(CK.WEEKS,6,2)-1, DATEADD(wk, DATEDIFF(wk,-1,DATEADD(yy, DATEDIFF(yy,0,RIGHT(CK.WEEKS,4)), 0)), 0)) -1,1)) =  stg.weeks AND  CK.CHANNEL2=''Total Commercial Resellers''')        
else if @in_file_type in('npd_weekly')           
begin           
set @l_SQL_String1 += concat('delete ck from df_denorm.forecast.ck_cng_npd_weekly ck inner join ',@l_ck_stg_nm_w_schema,' stg     
on IIF(isdate(ck.weeks) =1 , CK.WEEKS,convert(varchar(10),dateadd(week,substring(CK.WEEKS,6,2)-1, DATEADD(wk, DATEDIFF(wk,-1,DATEADD(yy, DATEDIFF(yy,0,RIGHT(CK.WEEKS,4)), 0)), 0)) -1,1)) =  stg.weeks  AND   
 CK.CHANNEL2=''Total Retail'' AND CK.SUBCAT <> ''Tablets''')       
end         
   else if @in_file_type in('npd_weekly_tab')           
begin           
set @l_SQL_String1 += concat('delete ck from df_denorm.forecast.ck_cng_npd_weekly ck inner join ',@l_ck_stg_nm_w_schema,' stg     
on IIF(isdate(ck.weeks) =1 , CK.WEEKS,convert(varchar(10),dateadd(week,substring(CK.WEEKS,6,2)-1, DATEADD(wk, DATEDIFF(wk,-1,DATEADD(yy, DATEDIFF(yy,0,RIGHT(CK.WEEKS,4)), 0)), 0)) -1,1)) =  stg.weeks   
 AND  CK.CHANNEL2=''Total Retail'' AND CK.SUBCAT = ''Tablets''')       
end         
   IF @in_debug = 'Y'            
     EXEC forecast.PR_PRINTMAX @l_SQL_String1            
    ELSE             
     EXEC SP_EXECUTESQL @l_SQL_String1          
    TRUNCATE TABLE #tmp_LIST_OF_COLS  
   END  
 END  
 CLOSE CUR_C1   
 DEALLOCATE CUR_C1    
 IF NOT EXISTS(SELECT TOP 1 1 FROM DF_STG.FORECAST.STG_LD_EXT_DATA_CONS WHERE FILE_NM LIKE '%' + IIF(@in_file_type IN ('GFK','IDC','APMSC','EMS'),@in_file_type,@l_vndr_file_nm) + '%')  
 BEGIN  
  IF @in_debug = 'N'  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   SET @l_email_title_ld_status =  'Source Format Validation Fail'  
   SET @l_v2_subj_success = CONCAT(@l_env , ' : CNG : ' , @l_email_title_ld_status, ' for ', @l_email_hdr_nm , ': ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CREATE_HTML ',QUOTENAME(@in_file_type,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   EXEC [forecast].[PR_CNG_SEND_NOTIFICATION]  
     @in_subj = @l_v2_subj_success,  
     @in_file_type = @l_email_hdr_nm,  
     @in_stg_view_nm = @l_stg_src_vw_nm,  
     @in_dq_view_nm = @l_dq_vw_nm,  
     @in_ld_status = @l_email_title_ld_status,  
     @in_dq_rcv_email_addr = @l_dq_rcv_email_addr,  
     @in_upld_usr_email_addr = @l_upld_usr_email_addr,  
     @in_upld_usr_nm = @l_upld_usr_nm,  
     @in_email_title_status = @l_email_title_ld_status,  
     @in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr,  
     @in_bypass_src_frmt_chk = @l_bypass_src_frmt_chk  
  END  
  RETURN  
 END  
 EXEC forecast.PR_PRINTD 'All files loaded to Staging Cook table and Critical DQ Check Complete...'  
 SET @l_SQL_String = CONCAT( '  
 USE DF;  
 EXEC  FORECAST.PR_CREATE_VIEW_STUB ''' , @l_stg_src_vw_nm , '''')  
 IF @in_debug = 'Y'  
  EXEC forecast.PR_PRINTMAX @l_SQL_String  
 ELSE     
  EXEC SP_EXECUTESQL @l_SQL_String  
 /*FOR ADDING EXTENDED PROPERTY FOR THE AUTOGENERATED OBJECT END*/  
 IF @in_debug = 'N'  
 BEGIN  
  SET @AUTOCOMMENT =''  
  EXEC FORECAST.PR_PRINTD 'CREATING EXTENDED PROPERTY FOR AUTO GENERATED OBJECT'  
  EXEC [FORECAST].[PR_CRE_EXT_PROP_AUTOGEN] @l_stg_src_vw_nm_wo_schema, 'FORECAST','PR_CNG_LD_CK_TBL_FROM_STG','VIEW',@AUTOCOMMENT OUTPUT    
  EXEC FORECAST.PR_PRINTD 'CREATING EXTENDED PROPERTY FOR AUTO GENERATED OBJECT COMPLETED'   
 END  
 /*FOR ADDING EXTENDED PROPERTY FOR THE AUTOGENERATED OBJECT END*/  
 SET @l_SQL_String = CONCAT(CHAR(13) , '   
 ALTER VIEW ' , @l_stg_src_vw_nm , ' AS')  
 SET @l_SQL_String += CHAR(13)  
 SET @l_SQL_String += CONCAT('  
 /*************************************************************************  
 * Name          : ' , @l_stg_src_vw_nm , '  
 * Author        : Mehul Shah  
 * Purpose       : STG View for ' , @in_file_type , '  
 * View          :   
 * Test       : SELECT TOP 100 * FROM ' , @l_stg_src_vw_nm , '  
 ***************************************************************************  
 * Change Date Change By  Change DSC    
 * ----------- ------------- -------------------------------------------  
 * ' , CONVERT(VARCHAR(10), GetDate(), 101) , ' forecast.PR_CNG_LD_CK_TBL_FROM_STG  Created  
 ***************************************************************************/  
 ')  
 SET @l_SQL_String += CHAR(13)  
 SET @l_SQL_String += CONCAT('SELECT ' , CHAR(13) ,   
 'STG.* ' , CHAR(13) ,   
 'FROM ',@l_ck_stg_nm_w_schema,' STG ')  
 IF @in_debug = 'Y'  
  EXEC forecast.PR_PRINTMAX @l_SQL_String   
 ELSE   
  EXEC SP_EXECUTESQL @l_SQL_String  
 -- Apply Data Correction Rules call for GFK  
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DC_RULE_APPLY ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('CK_STG',''''),',',QUOTENAME(@in_debug,''''),',',QUOTENAME('N',''''))  
 EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
 --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'STG', @in_debug  
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('STG',''''),',',QUOTENAME(@in_debug,''''))  
 --Print @l_cmd_syntax --forecast.PR_CNG_COPY_TO_HIST call  
 EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  --To Perform DQ checks  
 IF @in_file_type = 'NPD_WEEKLY'    
 BEGIN    
  PRINT 'Calling DQ Proc for NPD_Weekly'    
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD_PC_TAB_CRITICAL ',QUOTENAME('NPD_Weekly',''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME('N','''')),    
  @l_pkg_strt_tm = GETDATE()    
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]       
   @PKG_NM    = @l_cmd_syntax,       
   @MACHINE_NM   = @l_machine_nm,        
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,     
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,       
   @PACKAGE_END_TM  = NULL,       
   @DATA_DIR   = NULL,       
   @ERR_FILE_DIR  = NULL,       
   @ROWS_INSERTED  = NULL,       
   @ROWS_REJECTED  = NULL,      
   @JOB_ID    = @JOB_ID OUTPUT,    
   @RUN_STS   = 'RUNNING',
   @MODULE ='CNG';       
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD_PC_TAB_CRITICAL 'NPD_WEEKLY', @l_bypass_src_frmt_chk;    
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'   
  IF (@l_return_code <> 0)     
  OR (@l_crtcl_cols_chk_return_code <> 0)    
  BEGIN    
   EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table for critical error file...'    
   SET @l_SQL_String = 'TRUNCATE TABLE df_denorm.forecast.CK_CNG_NPD_WEEKLY_STG'    
    
   EXEC sp_executesql @l_SQL_String    
   SET @l_crtical_error_flag = 'Y' 
   SET @l_continue = 'N'    
  END    
  ELSE    
  BEGIN    
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD ',QUOTENAME('NPD_WEEKLY',''''))    
      ,@l_pkg_strt_tm = GETDATE()    
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]       
    @PKG_NM = @l_cmd_syntax,       
    @MACHINE_NM = @l_machine_nm,        
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,     
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,       
    @PACKAGE_END_TM = NULL,       
    @DATA_DIR = NULL,       
    @ERR_FILE_DIR = NULL,       
    @ROWS_INSERTED = NULL,       
    @ROWS_REJECTED = NULL,      
    @JOB_ID = @JOB_ID OUTPUT,    
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';       
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD 'NPD_WEEKLY';    
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END    
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'    
  BEGIN     
   PRINT 'NPD_WEEKLY DQ Issues Found...'    
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME('NPD_WEEKLY',''''),',',QUOTENAME('DQ',''''),',',QUOTENAME('N',''''))      
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END      
end      
 ELSE IF @in_file_type ='GFK'   
 BEGIN  
  PRINT 'Calling DQ Proc for GFK...'  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_GFK_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = FORECAST.PR_CNG_DQ_CHK_GFK_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'  
  SET @l_crtical_error_flag = ''  
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'DELETE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('DELETE ',@l_ck_stg_nm_w_schema,' WHERE FILE_NM IN ( SELECT DISTINCT FILE_NM FROM ',@l_ck_dq_nm_w_schema,' WHERE SKIP_FILE_LOAD = ''Y'')')  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
  END  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_GFK ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_GFK @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'GFK DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_GFK_STG])  
   SET @l_continue = 'N'  
 END  
 ELSE IF @in_file_type IN ('GFK_BRAND')   
 BEGIN  
  PRINT 'Calling DQ Proc for GFK Brand...'  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_GFK_BRAND_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = FORECAST.PR_CNG_DQ_CHK_GFK_Brand_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  SET @l_crtical_error_flag = ''  
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'DELETE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('DELETE ',@l_ck_stg_nm_w_schema,' WHERE FILE_NM IN ( SELECT DISTINCT FILE_NM FROM ',@l_ck_dq_nm_w_schema,' WHERE SKIP_FILE_LOAD = ''Y'')')  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
  END  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_GFK_brand ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()     
       EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
       @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm, 
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_GFK_Brand @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'GFK_BRAND DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_GFK_BRAND_STG])  
   SET @l_continue = 'N'  
 END  
 --IDC Server DQ Started  
 ELSE IF @in_file_type = 'IDC_SVR'  
 BEGIN  
  PRINT 'Calling DQ Proc for IDC_SVR...'  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_SVR_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = FORECAST.PR_CNG_DQ_CHK_IDC_SVR_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
  END  
  ELSE  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_SVR ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_SVR @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'IDC ServerWW DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_IDC_SVR_STG])  
   SET @l_continue = 'N'  
 END  
 --IDC Server DQ Ended  
 --IDC Serverx86 DQ Started  
 ELSE IF @in_file_type = 'IDC_X86'  
 BEGIN  
  PRINT 'Calling DQ Proc for IDC_X86...'  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_X86_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = FORECAST.PR_CNG_DQ_CHK_IDC_X86_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
  END  
  ELSE  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_X86 ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_X86 @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'IDC Serverx86 DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_IDC_X86_STG])  
   SET @l_continue = 'N'  
 END  
 --IDC Serverx86 DQ Ended  
 ELSE IF @in_file_type = 'IDC'  
 BEGIN  
  PRINT 'Calling DQ Proc for IDC...'  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR =  NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
  END  
  ELSE  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'IDC DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_IDC_STG])  
   SET @l_continue = 'N'  
 END  
 ELSE IF @in_file_type IN ('NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN')  
 BEGIN  
  PRINT CONCAT('Calling DQ Proc for ',@in_file_type)  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD_PC_TAB_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,'''')),@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM    = @l_cmd_syntax,     
   @MACHINE_NM   = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM  = NULL,     
   @DATA_DIR   = NULL,     
   @ERR_FILE_DIR  = NULL,     
   @ROWS_INSERTED  = NULL,     
   @ROWS_REJECTED  = NULL,    
   @JOB_ID    = @JOB_ID OUTPUT,  
   @RUN_STS   = 'RUNNING',
   @MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD_PC_TAB_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
   SET @l_continue = 'N'  
  END  
  ELSE  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN   
   PRINT 'NPD DQ Issues Found...'  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
 END  
 ELSE IF @in_file_type in ('NPD_COM_DIS','NPD_COM_RES')  
 BEGIN  
  PRINT CONCAT('Calling DQ Proc for ',@in_file_type)  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD_COM_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,'''')),@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM    = @l_cmd_syntax,     
   @MACHINE_NM   = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM  = NULL,     
   @DATA_DIR   = NULL,     
   @ERR_FILE_DIR  = NULL,     
   @ROWS_INSERTED  = NULL,     
   @ROWS_REJECTED  = NULL,    
   @JOB_ID    = @JOB_ID OUTPUT,  
   @RUN_STS   = 'RUNNING',
	@MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD_COM_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
   SET @l_continue = 'N'  
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN   
   PRINT 'NPD Commercial DQ Issues Found...'  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
 END  
 ELSE IF @in_file_type = 'IDC_TAB'  
 BEGIN  
  PRINT 'Calling DQ Proc for IDC Tablet...'  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_TAB_CRITICAL ',QUOTENAME(@in_file_type,'''')  
                    ,','  
                    ,QUOTENAME(@l_bypass_src_frmt_chk,'''')  
                    ,','  
                    ,QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM = @l_cmd_syntax,     
   @MACHINE_NM = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM = NULL,     
   @DATA_DIR = NULL,     
   @ERR_FILE_DIR = NULL,     
   @ROWS_INSERTED = NULL,     
   @ROWS_REJECTED = NULL,    
   @JOB_ID = @JOB_ID OUTPUT,  
   @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_TAB_CRITICAL @in_file_type , @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('SELECT @l_rtn_val_output = COUNT(*) FROM ',@l_ck_dq_nm_w_schema,' WHERE SKIP_FILE_LOAD = ''Y''')  
   SET @l_ParmDefinition = N'@l_rtn_val_output int OUTPUT';  
   EXEC sys.sp_executesql @l_sql_string, @l_ParmDefinition, @l_rtn_val_output=@l_rtn_val OUTPUT;  
   IF @l_rtn_val > 0  
   BEGIN  
    EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table since we have atleast 1 issue with SKIP_FILE_LOAD = ''Y''...'  
    SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
    EXEC sp_executesql @l_SQL_String  
    SET @l_continue = 'N'  
   END   
   SET @l_crtical_error_flag = 'Y'  
  END  
  -- If we still have records in CK STG table, continue with next level of DQ checks  
  --IF @l_continue = 'Y'  
  --BEGIN  
  -- SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_TAB ',QUOTENAME(@in_file_type,''''))  
  --    ,@l_pkg_strt_tm = GETDATE()  
  -- EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
  --  @PKG_NM = @l_cmd_syntax,     
  --  @MACHINE_NM = @l_machine_nm,      
  --  @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
  --  @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
  --  @PACKAGE_END_TM = NULL,     
  --  @DATA_DIR = NULL,     
  --  @ERR_FILE_DIR = NULL,     
  --  @ROWS_INSERTED = NULL,     
  --  @ROWS_REJECTED = NULL,    
  --  @JOB_ID = @JOB_ID OUTPUT,  
  --  @RUN_STS = 'RUNNING',
  --  @MODULE ='CNG';     
  -- EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_TAB @in_file_type;  
  -- EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  --END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'IDC Tablet DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
 END  
 ELSE IF @in_file_type = 'IDC_FCST'  
 BEGIN  
  PRINT 'Calling DQ Proc for IDC Forecast...'  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_IDC_FCST_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM = @l_cmd_syntax,     
   @MACHINE_NM = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM = NULL,     
   @DATA_DIR = NULL,     
   @ERR_FILE_DIR = NULL,     
   @ROWS_INSERTED = NULL,     
   @ROWS_REJECTED = NULL,    
   @JOB_ID = @JOB_ID OUTPUT,  
   @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_IDC_FCST_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   PRINT 'IDC Forecast DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_IDC_FCST_STG])  
   SET @l_continue = 'N'  
 END  
 ELSE IF @in_file_type LIKE 'AMSC%' OR  @in_file_type IN ('APMSC','EMS')  
 BEGIN  
  PRINT 'Calling DQ Proc for Consortia Files...'  
  IF @in_file_type = 'EMS'  
    BEGIN  
    EXEC FORECAST.PR_PRINTD 'Format Quarter and Month columns for EMS...'  
    SET @l_SQL_String = CONCAT('UPDATE',SPACE(1),@l_ck_stg_nm_w_schema,SPACE(1),'SET [MONTH] = IIF(DATEPART(MM,CONCAT([MONTH], SPACE(1), ''',@l_dummy_date,''')) < 10,CONCAT([YEAR],''M0'',DATEPART(MM,CONCAT([MONTH], SPACE(1), ''',@l_dummy_date,'''))), CONCAT([YEAR],''M'',DATEPART(MM,CONCAT([MONTH], SPACE(1), ''',@l_dummy_date,''')))),[QUARTER] = CONCAT([YEAR],REPLACE([QUARTER], ''Q'',''Q0''))')  
    EXEC SP_EXECUTESQL @l_SQL_String  
    END  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_CNSRT_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_ck_stg_nm_w_schema,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = FORECAST.PR_CNG_DQ_CHK_CNSRT_CRITICAL @in_file_type, @l_ck_stg_nm_w_schema, @l_bypass_src_frmt_chk, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  SET @l_crtical_error_flag = ''  
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   SET @l_SQL_String = CONCAT('SELECT @l_rtn_val_output = COUNT(*) FROM ',@l_ck_dq_nm_w_schema,' WHERE SKIP_FILE_LOAD = ''Y''')  
   SET @l_ParmDefinition = N'@l_rtn_val_output int OUTPUT';  
   EXEC sys.sp_executesql @l_sql_string, @l_ParmDefinition, @l_rtn_val_output=@l_rtn_val OUTPUT;  
   IF @l_rtn_val > 0  
   BEGIN  
    EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table since we have atleast 1 issue with SKIP_FILE_LOAD = ''Y''...'  
    SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
    EXEC sp_executesql @l_SQL_String  
    SET @l_continue = 'N'  
   END   
   SET @l_crtical_error_flag = 'Y'  
  END  
  -- If we still have records in CK STG table, continue with next level of DQ checks  
  IF @l_continue = 'Y'  
  BEGIN   
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_CNSRT ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_ck_stg_nm_w_schema,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_CNSRT @in_file_type, @l_ck_stg_nm_w_schema, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN  
   PRINT 'Consortia DQ Issues Found...'  
   --EXEC forecast.PR_CNG_COPY_TO_HIST @in_file_type,'DQ', @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   SET @l_SQL_String = CONCAT('SELECT @l_rtn_val_output = COUNT(*) FROM ',@l_ck_dq_nm_w_schema,' WHERE SKIP_FILE_LOAD = ''Y''')  
   SET @l_ParmDefinition = N'@l_rtn_val_output int OUTPUT';  
   EXEC sys.sp_executesql @l_sql_string, @l_ParmDefinition, @l_rtn_val_output=@l_rtn_val OUTPUT;  
   IF @l_rtn_val > 0  
   BEGIN  
    EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table since we have atleast 1 DQ issue with SKIP_FILE_LOAD = ''Y''...'  
    SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
    EXEC sp_executesql @l_SQL_String  
    IF @l_return_code <> 0  
     SET @l_data_quality_check = 'Y'  
    SET @l_continue = 'N'  
   END   
  END  
 END  
 ELSE IF @in_file_type = 'EMEA_DISCOUNTER'  
 BEGIN  
  PRINT CONCAT('Calling DQ Proc for ',@in_file_type)  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_EMEA_DISCOUNTER_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,'''')),@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM    = @l_cmd_syntax,     
   @MACHINE_NM   = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM  = NULL,     
   @DATA_DIR   = NULL,     
   @ERR_FILE_DIR  = NULL,     
   @ROWS_INSERTED  = NULL,     
   @ROWS_REJECTED  = NULL,    
   @JOB_ID    = @JOB_ID OUTPUT,  
   @RUN_STS   = 'RUNNING',
	@MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_EMEA_DISCOUNTER_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
   SET @l_continue = 'N'  
   PRINT 'EMEA Discounter Critical DQ Issues Found...'  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   end  
  END  
ELSE IF  @in_file_type IN ('NPD_WEEKLY_BIZ','NPD_WEEKLY_TAB') -- added by chethax    
  BEGIN      
 PRINT 'Calling DQ Proc for NPD_Weekly_BIZ'    
SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD_WEEKLY_BIZ_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,'''')),      
  @l_pkg_strt_tm = GETDATE()      
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]         
 @PKG_NM    = @l_cmd_syntax,         
 @MACHINE_NM   = @l_machine_nm,          
 @CONTAINER_STRT_TM = @in_cntnr_strt_tm,       
 @PACKAGE_STRT_TM = @l_pkg_strt_tm,         
 @PACKAGE_END_TM  = NULL,         
  @DATA_DIR   = NULL,         
@ERR_FILE_DIR  = NULL,         
 @ROWS_INSERTED  = NULL,         
 @ROWS_REJECTED  = NULL,        
 @JOB_ID    = @JOB_ID OUTPUT,      
  @RUN_STS   = 'RUNNING',
	@MODULE ='CNG';         
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD_WEEKLY_BIZ_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;      
 EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)      
  BEGIN      
   EXEC forecast.PR_PRINTD 'DELETE CK_STG table for critical error file...'      
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)      
  EXEC sp_executesql @l_SQL_String      
  SET @l_crtical_error_flag = 'Y'      
   SET @l_continue = 'N'      
  END      
  ELSE      
  BEGIN      
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_NPD ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))         
   ,@l_pkg_strt_tm = GETDATE()      
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]         
    @PKG_NM = @l_cmd_syntax,         
    @MACHINE_NM = @l_machine_nm,          
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,       
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,         
    @PACKAGE_END_TM = NULL,         
   @DATA_DIR = NULL,         
   @ERR_FILE_DIR = NULL,         
   @ROWS_INSERTED = NULL,         
   @ROWS_REJECTED = NULL,        
  @JOB_ID = @JOB_ID OUTPUT,      
  @RUN_STS = 'RUNNING',
	@MODULE ='CNG';         
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_NPD @in_file_type, @in_debug;      
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END      
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'      
  BEGIN       
  PRINT 'NPD_WEEKLY_BIZ DQ Issues Found...'      
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))      
  EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END      
End      
 /****CONTEXT****/  
 ELSE IF @in_file_type = 'CTX'  
 BEGIN  
  PRINT CONCAT('Calling DQ Proc for ',@in_file_type)  
  SET @l_crtical_error_flag = ''  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_CTX_CRITICAL ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@l_bypass_src_frmt_chk,''''),',',QUOTENAME(@in_debug,'''')),@l_pkg_strt_tm = GETDATE()  
  EXEC DF.[FORECAST].[PR_STG_LD_JOB_INSERT]     
   @PKG_NM    = @l_cmd_syntax,     
   @MACHINE_NM   = @l_machine_nm,      
   @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
   @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
   @PACKAGE_END_TM  = NULL,     
   @DATA_DIR   = NULL,     
   @ERR_FILE_DIR  = NULL,     
   @ROWS_INSERTED  = NULL,     
   @ROWS_REJECTED  = NULL,    
   @JOB_ID    = @JOB_ID OUTPUT,  
   @RUN_STS   = 'RUNNING',
	@MODULE ='CNG';     
  EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_CTX_CRITICAL @in_file_type, @l_bypass_src_frmt_chk, @in_debug;  
  EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  IF (@l_return_code <> 0) OR (@l_crtcl_cols_chk_return_code <> 0)  
  BEGIN  
   EXEC forecast.PR_PRINTD 'TRUNCATE CK_STG table for critical error file...'  
   SET @l_SQL_String = CONCAT('TRUNCATE TABLE ',@l_ck_stg_nm_w_schema)  
   EXEC sp_executesql @l_SQL_String  
   SET @l_crtical_error_flag = 'Y'  
   SET @l_continue = 'N'  
  END  
  ELSE  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CHK_CTX ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
      ,@l_pkg_strt_tm = GETDATE()  
   EXEC DF.[forecast].[PR_STG_LD_JOB_INSERT]     
    @PKG_NM = @l_cmd_syntax,     
    @MACHINE_NM = @l_machine_nm,      
    @CONTAINER_STRT_TM = @in_cntnr_strt_tm,   
    @PACKAGE_STRT_TM = @l_pkg_strt_tm,     
    @PACKAGE_END_TM = NULL,     
    @DATA_DIR = NULL,     
    @ERR_FILE_DIR = NULL,     
    @ROWS_INSERTED = NULL,     
    @ROWS_REJECTED = NULL,    
    @JOB_ID = @JOB_ID OUTPUT,  
    @RUN_STS = 'RUNNING',
	@MODULE ='CNG';     
   EXECUTE @l_return_code = forecast.PR_CNG_DQ_CHK_CTX @in_file_type, @in_debug;  
   EXEC DF.[forecast].[PR_STG_LD_JOB_UPDATE] @JOB_ID = @JOB_ID ,@IN_V2_PKG_NM = @l_file_name, @MODULE = 'CNG'
  END  
  IF @l_return_code <> 0 OR @l_crtical_error_flag = 'Y'  
  BEGIN   
   PRINT 'CTX DQ Issues Found...'  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  IF NOT EXISTS (SELECT 1 FROM [DF_DENORM].[forecast].[CK_CNG_CTX_STG])  
   SET @l_continue = 'N'  
 END  
 ELSE  
  RETURN
  If @l_continue = 'N'  
	Update forecast.cnst set cnst_val = 0,LAST_MOD_DT = Getdate() where cnst_nm = 'CNG_FILE_LOAD_STATUS'
--##############################################  
--  code to send mail for DQs  
--##############################################  
 --EXEC forecast.PR_CNG_DQ_CREATE_HTML @in_file_type  
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DQ_CREATE_HTML ',QUOTENAME(@in_file_type,''''))  
 EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
 CREATE TABLE #tmpDQStatus  
 (  
  LOAD_DATA_FLG_N INT  
  ,LOAD_DATA_FLG_Y INT  
 )  
 SET @l_SQL_String =  CONCAT('  
 SELECT   
  SUM(CASE WHEN (''',@in_file_type ,''' NOT IN (''AMSC_US'',''AMSC_CANADA'',''AMSC_LAR'',''APMSC'',''EMS'') OR ERROR_TYPE != ''Duplicate Records'') AND LOAD_DATA_FLG = ''N'' THEN 1 ELSE 0 END)  
  ,SUM(CASE WHEN LOAD_DATA_FLG = ''Y'' THEN 1 ELSE 0 END)  
 FROM ' , @l_ck_dq_nm_w_schema)  
 INSERT INTO #tmpDQStatus  
 EXEC SP_EXECUTESQL @l_SQL_String  
 SELECT   
  @l_load_data_flag_N = LOAD_DATA_FLG_N  
  ,@l_load_data_flag_Y = LOAD_DATA_FLG_Y  
 FROM #tmpDQStatus  
 IF (@l_load_data_flag_Y > 0 AND @l_load_data_flag_N = 0)  
  SET @l_email_title_ld_status +=  'Data Quality Check Fail'  
 ELSE IF (@l_load_data_flag_Y > 0 AND @l_load_data_flag_N > 0) OR @l_data_quality_check = 'Y'  
  SET  @l_email_title_ld_status +=  'Data Quality Check Fail / Source Format Validation Fail'   
 ELSE IF @l_load_data_flag_Y = 0 AND @l_load_data_flag_N > 0  
  SET @l_email_title_ld_status +=  'Source Format Validation Fail'  
 ELSE   
  SET @l_email_title_ld_status =  'Data Quality Check Success'  
 --SET VALUE TO STATUS IN TABLE CNG_AUDIT_PROCESS  
    SET @STATUS = CASE WHEN (@l_load_data_flag_Y = 0 AND @l_load_data_flag_N > 0)   
  OR  (@l_load_data_flag_Y > 0 AND @l_load_data_flag_N > 0) OR @l_data_quality_check = 'Y' THEN 'FAILED' ElSE 'RUNNING' END   
 --UPDATE TABLE CNG_AUDIT_PROCESS   
 SET @ERR_MSG = ERROR_MESSAGE()  
 EXEC [FORECAST].[PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY]   
      @JOBID  
     ,@STATUS  
     ,@ERR_MSG  
 SET @l_v2_subj_success = CONCAT(@l_env , ' : CNG : ' , @l_email_title_ld_status, ' for ', @l_email_hdr_nm , ': ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)  
 IF @in_debug = 'N'  
 BEGIN  
  EXEC [forecast].[PR_CNG_SEND_NOTIFICATION]  
     @in_subj = @l_v2_subj_success,  
     @in_file_type = @l_email_hdr_nm,  
     @in_stg_view_nm = @l_stg_src_vw_nm,  
     @in_dq_view_nm = @l_dq_vw_nm,  
     @in_ld_status = @l_email_title_ld_status,  
     @in_dq_rcv_email_addr = @l_dq_rcv_email_addr,  
     @in_upld_usr_email_addr = @l_upld_usr_email_addr,  
     @in_upld_usr_nm = @l_upld_usr_nm,  
     @in_email_title_status = @l_email_title_ld_status,  
     @in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr,    
     @in_bypass_src_frmt_chk = @l_bypass_src_frmt_chk  
 END  
 IF @l_continue = 'Y'  
 BEGIN    
  SELECT @l_del_cnst = CNST_DEF  
  FROM df.FORECAST.CNST WITH(NOLOCK)  
  WHERE CNST_NM = @l_cnst_nm  
  SELECT @l_del_col = CNST_DEF  
  FROM df.FORECAST.CNST WITH(NOLOCK)  
  WHERE CNST_NM = @l_del_col_nm  
  IF ISNULL(@l_del_cnst,'') = '' 
  BEGIN  
   SET @l_del_stmt = CONCAT('DELETE ',@l_ck_stg_final_nm , ' WHERE FILE_NM IN ( SELECT DISTINCT FILE_NM FROM  #tmp_CNG_HANDSET_FOR_FNL_CK_TBL);')  
  END  
  ELSE  
   BEGIN  
  SET @l_del_stmt = CONCAT( '  
          DELETE top (100000) fnl  
          FROM ',@l_ck_stg_final_nm,' fnl  
          --INNER JOIN #tmp_CNG_HANDSET_FOR_FNL_CK_TBL stg  
          INNER JOIN #tmp_CNG_DELTE_SET stg  
          ON ',@l_del_cnst)  
  END  
  SET @l_load_raw_files= 'Y'  
  CREATE TABLE #tmpFilesToBeProcessed (FILE_NM varchar(200))  
  SET @l_SQL_String = CONCAT('SELECT DISTINCT FILE_NM FROM ' ,@l_ck_stg_nm_w_schema)  
  INSERT INTO #tmpFilesToBeProcessed  
  EXEC sp_executesql @l_SQL_String  
  SELECT @l_file_list += CONCAT('<BR><BR>',FILE_NM)  
  FROM #tmpFilesToBeProcessed   
  SET @l_file_list = STUFF(@l_file_list,1,8,'')  
  --Insert into Cook Table if no errors  
  INSERT INTO #tmp_LIST_OF_COLS (COL_NM,COL_NM_WITH_CAST_AND_CNVRSN)  
   SELECT  c.name AS COL_NM ,  
   forecast.FN_CNG_GET_COL_CASTING( @in_file_type ,c.name,LTRIM(RTRIM(c.name)),'COL_NM_WITH_CAST_AND_CNVRSN') COL_NM_WITH_CAST_AND_CNVRSN  
   FROM DF_DENORM.sys.tables T WITH(nolock)  
   INNER JOIN DF_DENORM.sys.columns C WITH(nolock) ON T.object_id = C.object_id  
   WHERE T.NAME =  @l_ck_stg_nm   
  UNION ALL  
  SELECT  c.name AS COL_NM   
    ,QUOTENAME(c.name) COL_NM_WITH_CAST_AND_CNVRSN  
  FROM DF_DENORM.sys.tables T with(nolock)  
  INNER JOIN DF_DENORM.sys.columns C with(nolock) ON T.object_id = C.object_id  
  WHERE T.NAME = 'CK_CNG_DIM_ID_MAPPING'   
  AND CASE when @in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB')AND c.name in ('CONSUMER_PRODUCT_ID','RAW_PRODUCT_ID','RAW_CHANNEL_ID')THEN 1  
     --WHEN @in_file_type like 'IDC%' AND c.name in ('CONSUMER_PRODUCT_ID','RAW_PRODUCT_ID') THEN 1  
     WHEN @in_file_type IN ('IDC','IDC_TAB','IDC_SVR','IDC_X86','EMEA_DISCOUNTER') AND c.name in ('CONSUMER_PRODUCT_ID','RAW_PRODUCT_ID') THEN 1  
     WHEN @in_file_type IN ( 'IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR') AND c.name in ('CONSUMER_PRODUCT_ID') THEN 1  
    -- WHEN @in_file_type IN ('IDC_X86') AND c.name in ('RAW_PRODUCT_ID') THEN 1  
  ELSE 0 END = 1  
  UNION ALL  
     SELECT 'NETBK_FF', 'CAST(CASE WHEN (FORM_FACTOR_LVL1 = ''PC Client'' AND FORM_FACTOR_LVL2 = ''Total Notebook'' AND FORM_FACTOR_LVL3 = ''Mini Notebook'') OR (FORM_FACTOR_LVL1 = ''PC Client'' AND FORM_FACTOR_LVL2 = ''Mininotebook''   AND FORM_FACTOR_LVL3 = ''Mininotebook'') THEN ''Netbook'' ELSE '''' END AS VARCHAR(200)) NETBK_FF'  
  WHERE @in_file_type IN ( 'APMSC','AMSC_US','AMSC_CANADA')  
  UNION ALL  
  SELECT 'NETBK_FF', 'CAST(CASE WHEN (FORM_FACTOR_LVL2 = ''Netbooks'' AND FORM_FACTOR_LVL3 = ''Netbooks'') OR (FORM_FACTOR_LVL2 = ''Mobile'' AND FORM_FACTOR_LVL3 = ''Netbooks'') OR (FORM_FACTOR_LVL2 = ''Portable''   AND FORM_FACTOR_LVL3 = ''Portable+Mininotebook'') THEN ''Netbook'' ELSE '''' END AS VARCHAR(200)) NETBK_FF'  
  WHERE @in_file_type IN ( 'EMS','AMSC_LAR')  
  SET @l_COLS_NM = ''  
  SET @l_COLS_Final = ''  
  SELECT  @l_COLS_NM  +=CONCAT( ',[' ,  COL_NM , ']') ,  
    @l_COLS_Final += CONCAT( ',' , COL_NM_WITH_CAST_AND_CNVRSN , CHAR(13) , CHAR(9) , CHAR(9))  
   FROM dbo.#tmp_LIST_OF_COLS  
  SELECT @l_COLS_NM = STUFF(@l_COLS_NM,1,1,'')  
  , @l_COLS_NM = CONCAT(CHAR(13) , CHAR(9) , CHAR(9) , @l_COLS_NM)  
  , @l_COLS_Final = STUFF(@l_COLS_Final,1,1,'')  
  , @l_COLS_Final = CONCAT(CHAR(13) , CHAR(9) , CHAR(9) , @l_COLS_Final)  
  IF @in_debug = 'Y'  
  BEGIN  
   PRINT '@l_COLS_NM = _____________________________________'  
   PRINT '-----------------------------------------------------'   
   EXECUTE forecast.PR_PRINTMAX @l_COLS_NM  
   PRINT '@l_COLS_Final = _____________________________________'  
   PRINT '-----------------------------------------------------'   
   EXECUTE forecast.PR_PRINTMAX @l_COLS_Final  
   EXEC forecast.PR_PRINTMAX @l_SQL_String  
   SELECT * from #tmp_LIST_OF_COLS  
  END  
   SELECT   
    @l_CNG_MAPPING_ID_CONSUMER_PRODUCT_VAL = MAX(CASE WHEN CNST_NM like '%MAPPING_ID_CONSUMER_PRODUCT_VAL'  
             THEN CNST_DEF  
             ELSE ''  
             END)  
    ,@l_CNG_MAPPING_ID_RAW_PRODUCT_VAL = MAX(CASE WHEN CNST_NM like '%MAPPING_ID_RAW_PRODUCT_VAL'  
             THEN CNST_DEF  
             ELSE ''  
             END)  
    ,@l_CNG_MAPPING_ID_RAW_CHANNEL_VAL = MAX(CASE WHEN CNST_NM like '%MAPPING_ID_RAW_CHANNEL_VAL'   
             THEN CNST_DEF  
             ELSE ''  
             END)  
   FROM [forecast].[CNST] with(NOLOCK)   
   WHERE CNST_NM LIKE CASE   
        WHEN @in_file_type ='GFK' THEN 'CNG_GFK_MAPPING_ID%'  
        WHEN @in_file_type ='GFK_BRAND' THEN 'CNG_GFK_BRAND_MAPPING_ID%'  
        WHEN @in_file_type = 'IDC' THEN 'CNG_IDC_MAPPING_ID%'  
        WHEN @in_file_type = 'IDC_TAB' THEN 'CNG_IDC_TAB_MAPPING_ID%'  
        WHEN @in_file_type = 'IDC_SVR' THEN 'CNG_IDC_SVR_MAPPING_ID%'  
        WHEN @in_file_type = 'IDC_X86' THEN 'CNG_IDC_X86_MAPPING_ID%'  
        WHEN @in_file_type = 'IDC_FCST' THEN 'CNG_IDC_FCST_MAPPING_ID%'  
								WHEN @in_file_type like 'NPD_TAB_US' THEN 'CNG_NPD_TAB_US_MAPPING_ID%'
								WHEN @in_file_type like 'NPD_TAB_CN' THEN 'CNG_NPD_TAB_CN_MAPPING_ID%'
        WHEN @in_file_type = 'NPD_COM_DIS' THEN 'CNG_NPD_COM_DIS_MAPPING_ID%'  
        WHEN @in_file_type = 'NPD_COM_RES' THEN 'CNG_NPD_COM_RES_MAPPING_ID%'  
        WHEN @in_file_type IN ('APMSC','AMSC_US','AMSC_CANADA') THEN 'CNG_APMSC_AMSC_MAPPING_ID%'  
        WHEN @in_file_type IN ('EMS','AMSC_LAR') THEN 'CNG_AMSC_EMS_MAPPING_ID%'  
        WHEN @in_file_type = 'EMEA_DISCOUNTER' THEN 'CNG_EMEA_DISCOUNTER_MAPPING_ID%'   
        WHEN @in_file_type = 'CTX' THEN 'CNG_CTX_MAPPING_ID%'        
        WHEN @in_file_type in ('NPD_WEEKLY','NPD_WEEKLY_BIZ') THEN 'CNG_NPD_WEEKLY_MAPPING_ID%'      
        WHEN @in_file_type IN ('NPD_WEEKLY_TAB') THEN 'CNG_NPD_WEEKLY_TAB_MAPPING_ID%'
        END  
   OR  
       (@in_file_type like 'NPD_PC_CN' AND CNST_NM IN  ('CNG_NPD_PC_CN_MAPPING_ID_CONSUMER_PRODUCT_VAL','CNG_NPD_PC_CN_MAPPING_ID_RAW_PRODUCT_VAL','CNG_NPD_PC_MAPPING_ID_RAW_CHANNEL_VAL'))  
   OR  
       (@in_file_type like 'NPD_PC_US' AND CNST_NM IN  ('CNG_NPD_PC_US_MAPPING_ID_CONSUMER_PRODUCT_VAL','CNG_NPD_PC_MAPPING_ID_RAW_PRODUCT_VAL','CNG_NPD_PC_MAPPING_ID_RAW_CHANNEL_VAL'))  
   OR  
       (@in_file_type like 'NPD_PC_MXN' AND CNST_NM IN  ('CNG_NPD_PC_MXN_MAPPING_ID_CONSUMER_PRODUCT_VAL','CNG_NPD_PC_MXN_MAPPING_ID_RAW_PRODUCT_VAL','CNG_NPD_PC_MAPPING_ID_RAW_CHANNEL_VAL'))  
   SET @l_CNG_MAPPING_ID_CONSUMER_PRODUCT_VAL = CONCAT(',STUFF(CONCAT(''X'',',REPLACE(@l_CNG_MAPPING_ID_CONSUMER_PRODUCT_VAL,',',',''~'','),'),1,1,'''') AS CONSUMER_PRODUCT_VAL')  
   IF @in_file_type NOT IN ('IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR')  
    SET @l_CNG_MAPPING_ID_RAW_PRODUCT_VAL = CONCAT(',STUFF(CONCAT(''X'',',REPLACE(@l_CNG_MAPPING_ID_RAW_PRODUCT_VAL,',',',''~'','),'),1,1,'''') AS RAW_PRODUCT_VAL')  
   IF @in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB','CTX')  
    SET @l_CNG_MAPPING_ID_RAW_CHANNEL_VAL = CONCAT(',STUFF(CONCAT(''X'',',REPLACE(@l_CNG_MAPPING_ID_RAW_CHANNEL_VAL,',',',''~'','),'),1,1,'''') AS RAW_CHANNEL_VAL')  
   SET @l_SQL_String = CONCAT( 'SELECT FILE_NM AS FL_NM ,ROW_ID AS RW_ID INTO #tmpSkipDQRow FROM DF_DENORM.forecast.CK_CNG_', @in_file_type , '_DQ WHERE LOAD_DATA_FLG = ''N''' , CHAR(13) , CHAR(13))  
   IF(@in_file_type IN ('GFK','GFK_BRAND','IDC','IDC_SVR','IDC_X86','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','IDC_TAB','IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR','EMEA_DISCOUNTER','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB'))  
   BEGIN  
   SET @l_SQL_String += CONCAT( 'SELECT  FILE_NM,ROW_ID',  
    --IIF(@in_file_type IN ('IDC_X86'),'',  
    STUFF(CONCAT('X',@l_CNG_MAPPING_ID_CONSUMER_PRODUCT_VAL,CHAR(13)),1,1,''), CHAR(13),               
    IIF(@in_file_type IN ('IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR'),'',STUFF(CONCAT('X',@l_CNG_MAPPING_ID_RAW_PRODUCT_VAL,CHAR(13)),1,1,'')), CHAR(13),  
    IIF(@in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB')  
    ,STUFF(CONCAT('X',@l_CNG_MAPPING_ID_RAW_CHANNEL_VAL,CHAR(13)),1,1,''),CHAR(13)),' INTO  #tmpConcatVal FROM ',@l_ck_stg_nm_w_schema, CHAR(13) , CHAR(13)  
   --,CASE WHEN @in_file_type NOT IN ('IDC_X86') THEN  
   ,'SELECT  FILE_NM ,STUFF(forecast.fn_StrConcat_Max(DISTINCT '','' + cast(ROW_ID as varchar(200))),1,1,'''') ROW_ID ,CONSUMER_PRODUCT_VAL  
    INTO #tmpConcatConsProdVal  FROM #tmpConcatVal  GROUP BY FILE_NM,CONSUMER_PRODUCT_VAL'  
   -- ELSE '' END  
   ,CHAR(13) , CHAR(13)  
   ,CASE WHEN @in_file_type NOT IN ('IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR') THEN  
    'SELECT  FILE_NM ,STUFF(forecast.fn_StrConcat_Max(DISTINCT '','' + cast(ROW_ID as varchar(200))),1,1,'''') ROW_ID ,RAW_PRODUCT_VAL  
    INTO #tmpConcatProdVal  FROM #tmpConcatVal  GROUP BY FILE_NM,RAW_PRODUCT_VAL'  
    ELSE '' END  
   ,CHAR(13) , CHAR(13)  
   ,CASE WHEN @in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB') THEN  
    'SELECT  FILE_NM ,STUFF(forecast.fn_StrConcat_Max(DISTINCT '','' + cast(ROW_ID as varchar(200))),1,1,'''') ROW_ID ,RAW_CHANNEL_VAL  
    INTO #tmpConcatChannelVal  FROM #tmpConcatVal  GROUP BY FILE_NM,RAW_CHANNEL_VAL'  
    ELSE '' END  
   ,CHAR(13) , CHAR(13)  
   --,CASE WHEN @in_file_type NOT IN ('IDC_X86') THEN CONCAT(  
   ,'SELECT FILE_NM AS FL_NM, ELEMENT AS ROW_NO, CONSUMER_PRODUCT_ID INTO #tmpConsumerProdID  
   FROM  
   (SELECT FILE_NM ,ROW_ID,CONSUMER_PRODUCT_VAL,CONSUMER_PRODUCT_ID FROM #tmpConcatConsProdVal        
    INNER JOIN [DF_DENORM].[forecast].CK_CNG_DIM_ID_MAPPING map ON   
    map.CONCAT_VAL = CONSUMER_PRODUCT_VAL   
    WHERE map.VNDR = ', QUOTENAME(@l_dim_mapping_cprd_vndr,''''), ' AND CONSUMER_PRODUCT_ID IS NOT NULL)tmp  
   CROSS APPLY forecast.FN_SPLITCLR_MAX(tmp.ROW_ID,'','')'  
   ,CHAR(13) , CHAR(13)  
   ,CASE WHEN @in_file_type NOT IN ('IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR') THEN CONCAT('SELECT FILE_NM AS FL_NM, ELEMENT AS ROW_NO, RAW_PRODUCT_ID INTO #tmpProdID  
   FROM  
   (SELECT FILE_NM ,ROW_ID,RAW_PRODUCT_VAL,RAW_PRODUCT_ID FROM #tmpConcatProdVal        
    INNER JOIN [DF_DENORM].[forecast].CK_CNG_DIM_ID_MAPPING map ON   
    map.CONCAT_VAL = RAW_PRODUCT_VAL   
    WHERE map.VNDR = ', QUOTENAME(@l_dim_mapping_prd_vndr,''''), ' AND RAW_PRODUCT_ID IS NOT NULL)tmp  
   CROSS APPLY forecast.FN_SPLITCLR_MAX(tmp.ROW_ID,'','')' ) ELSE '' END  
   ,CHAR(13) , CHAR(13)  
   ,CASE WHEN @in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB') THEN CONCAT('SELECT FILE_NM AS FL_NM, ELEMENT AS ROW_NO,RAW_CHANNEL_ID INTO #tmpChannelID  
   FROM  
   (SELECT FILE_NM ,ROW_ID,RAW_CHANNEL_VAL,RAW_CHANNEL_ID FROM #tmpConcatChannelVal        
    INNER JOIN [DF_DENORM].[forecast].CK_CNG_DIM_ID_MAPPING map ON   
    map.CONCAT_VAL = RAW_CHANNEL_VAL   
    WHERE map.VNDR = ', QUOTENAME(@l_dim_mapping_chnl_vndr,''''), ' AND RAW_CHANNEL_ID IS NOT NULL)tmp  
   CROSS APPLY forecast.FN_SPLITCLR_MAX(tmp.ROW_ID,'','')' ) ELSE '' END  
   )  
   END  
   SET @l_SQL_String += CONCAT('  
     SELECT ' ,     
     @l_COLS_Final ,   
     'INTO #tmp_CNG_HANDSET_FOR_FNL_CK_TBL  
     FROM  ' , '',@l_ck_stg_nm_w_schema,'' , ' STG' ,CHAR(13) ,   
     CASE WHEN @in_file_type IN ('GFK','GFK_BRAND','IDC','IDC_SVR','IDC_X86','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','IDC_TAB','IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR','EMEA_DISCOUNTER','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB') THEN   
      CONCAT(   
      --CASE WHEN @in_file_type NOT IN ('IDC_X86') THEN   
        'INNER JOIN #tmpConsumerProdID cprd ON STG.FILE_NM = cprd.FL_NM AND STG.ROW_ID = cprd.ROW_NO ',CHAR(13),  
        CASE WHEN @in_file_type NOT IN ('IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR') THEN   
        'INNER JOIN #tmpProdID prd ON STG.FILE_NM = prd.FL_NM AND STG.ROW_ID = prd.ROW_NO ' ELSE '' END,CHAR(13),  
        CASE WHEN @in_file_type IN ('GFK','GFK_BRAND','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB') THEN   
         'INNER JOIN #tmpChannelID Chnl ON STG.FILE_NM = Chnl.FL_NM AND STG.ROW_ID = Chnl.ROW_NO ' ELSE '' END   
       )ELSE '' END  
     ,'LEFT JOIN #tmpSkipDQRow TMP ON STG.FILE_NM = TMP.FL_NM AND STG.ROW_ID = TMP.RW_ID ', CHAR(13) ,  
      'WHERE TMP.FL_NM IS NULL')  
  DECLARE @l_tmp_tbl_nm varchar(100)= '#tmp_CNG_HANDSET_FOR_FNL_CK_TBL'  
  SET @l_SQL_String += CONCAT('  
  DECLARE @l_SQL_String_inner NVARCHAR(MAX) = ''''  
  , @l_stg_tbl_nm VARCHAR(200) = ','''',@l_tmp_tbl_nm,'''' , CHAR(13) , CHAR(9) , CHAR(9),'  
  , @l_ck_tbl_nm VARCHAR(200) = ','''',@l_ck_stg_final_nm,'''' , CHAR(13) , CHAR(9) , CHAR(9) , '  
  , @l_recreate varchar(1) = ''N'' ' , CHAR(13) , CHAR(9) , CHAR(9) ,  
       'IF OBJECT_ID(' , '''', @l_ck_stg_final_nm ,'''', ') IS NULL ' , CHAR(13) , CHAR(9) , CHAR(9) ,  
        ' SET @l_recreate =''Y'' ')  
  SET @l_SQL_String += '  
  EXEC forecast.PR_CREATE_CK_TBL @l_stg_tbl_nm,@l_ck_tbl_nm,NULL,NULL,''N'',@l_recreate '  
  SET @l_SQL_String += CONCAT('   
    SET @l_SQL_String_inner = ''',  
  CASE WHEN @in_file_type in('IDC_X86','IDC_FCST') THEN CONCAT(' TRUNCATE TABLE ' , @l_ck_stg_final_nm , ';' )  
  ELSE  
  CONCAT('  
    SELECT DISTINCT ', @l_del_col ,' INTO #tmp_CNG_DELTE_SET FROM #tmp_CNG_HANDSET_FOR_FNL_CK_TBL  
    --SET ROWCOUNT 100000  
    WHILE 1=1  
    BEGIN '  
     ,@l_del_stmt,  
    '  
    IF @@ROWCOUNT = 0   
    BREAK  
    END  
    SET ROWCOUNT 0') END ,  
  '  
   INSERT INTO ',@l_ck_stg_final_nm   
  ,'(' , @l_COLS_NM ,  ') ')  
  SET @l_SQL_String += CONCAT('  
    SELECT ', @l_COLS_NM  ,' FROM #tmp_CNG_HANDSET_FOR_FNL_CK_TBL'
,
case when @in_file_type  IN ( 'NPD_WEEKLY','npd_weekly_tab','NPD_pc_cn','npd_pc_us','npd_tab_cn','npd_tab_us','NPD_PC_MXN','NPD_COM_RES','NPD_COM_DIS')
then  concat(CHAR(13), CHAR(9),CHAR(9),'
update tbl set HQUARTER = cast( CASE WHEN LEFT(Timeper,3) IN (''''Jan'''',''''Feb'''',''''Mar'''')   
         THEN  CONCAT(''''Quarter 1 '''',RIGHT(Timeper,4))
			WHEN LEFT(Timeper,3) In (''''Apr'''', ''''May'''', ''''Jun'''')
		    THEN CONCAT(''''Quarter 2 '''',RIGHT(Timeper,4))
			WHEN LEFT(Timeper,3) In (''''Jul'''', ''''Aug'''', ''''Sep'''')
		    THEN CONCAT(''''Quarter 3 '''',RIGHT(Timeper,4))
			WHEN LEFT(Timeper,3) In (''''Oct'''', ''''Nov'''', ''''Dec'''')
		    THEN CONCAT(''''Quarter 4 '''',RIGHT(Timeper,4))
	   ELSE ''''INVALID QUARTER''''
	   END AS varchar(200)) ,ANNUAL=cast(RIGHT(timeper,4)AS varchar(200))  from ',@l_ck_stg_final_nm,' tbl',CHAR(13), CHAR(9),CHAR(9))
ELSE  concat(' ''',CHAR(13), CHAR(9),CHAR(9)) END,
CASE WHEN @in_file_type  IN ( 'NPD_COM_RES')
then  concat(CHAR(13), CHAR(9),CHAR(9),'
UPDATE TB SET DISCHANN=''''Not Applicable''''  from ',@l_ck_stg_final_nm,' tb''',CHAR(13), CHAR(9))
when  @in_file_type  IN ( 'NPD_WEEKLY','npd_weekly_tab','NPD_pc_cn','npd_pc_us','npd_tab_cn','npd_tab_us','NPD_PC_MXN','NPD_COM_RES','NPD_COM_DIS')
then concat(' ''',CHAR(13), CHAR(9),CHAR(9))
END)
  SET @l_SQL_String += CONCAT(' IF ''',@in_debug,''' = ''Y''  
   EXEC forecast.PR_PRINTMAX @l_SQL_String_inner  
  ELSE   
   EXEC SP_EXECUTESQL @l_SQL_String_inner')
  IF @in_file_type IN( 'GFK')  
  BEGIN  
   --EXEC forecast.[PR_CNG_UPD_GFK_DIM_ID_MAPPING] @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_UPD_GFK_DIM_ID_MAPPING ',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  ELSE IF @in_file_type IN( 'GFK_BRAND')  
  BEGIN  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_UPD_GFK_BRAND_DIM_ID_MAPPING ',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  ELSE IF @in_file_type = 'IDC'  
  BEGIN  
   --EXEC forecast.[PR_CNG_UPD_IDC_DIM_ID_MAPPING] @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_UPD_IDC_DIM_ID_MAPPING ',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  ELSE IF @in_file_type IN ('NPD_TAB_CN','IDC_SVR','IDC_X86','NPD_TAB_US','NPD_PC_CN','NPD_PC_US','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','IDC_TAB','IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR','EMEA_DISCOUNTER','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB')   
  BEGIN  
   --EXEC forecast.[PR_CNG_UPD_GFK_DIM_ID_MAPPING] @in_debug  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_UPD_DIM_ID_MAPPING ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  END  
  EXEC forecast.PR_PRINTMAX @l_SQL_String  
  IF @in_debug = 'Y'  
   EXEC forecast.PR_PRINTMAX @l_SQL_String  
  ELSE  
   EXEC SP_EXECUTESQL @l_SQL_String  
  SET @l_SQL_String = CONCAT('  
  USE DF;  
  EXEC  FORECAST.PR_CREATE_VIEW_STUB ''' , @l_fnl_src_vw_nm , '''')  
  IF @in_debug = 'Y'  
   EXEC forecast.PR_PRINTMAX @l_SQL_String  
  ELSE  
   EXEC SP_EXECUTESQL @l_SQL_String  
  /*FOR ADDING EXTENDED PROPERTY FOR THE AUTOGENERATED OBJECT END*/  
  IF @in_debug = 'N'  
  BEGIN  
   SET @AUTOCOMMENT =''  
   EXEC FORECAST.PR_PRINTD 'CREATING EXTENDED PROPERTY FOR AUTO GENERATED OBJECT'  
   EXEC [FORECAST].[PR_CRE_EXT_PROP_AUTOGEN] @l_fnl_src_vw_nm_wo_schema, 'FORECAST','PR_CNG_LD_CK_TBL_FROM_STG','VIEW',@AUTOCOMMENT OUTPUT    
   EXEC FORECAST.PR_PRINTD 'CREATING EXTENDED PROPERTY FOR AUTO GENERATED OBJECT COMPLETED'   
  END  
  /*FOR ADDING EXTENDED PROPERTY FOR THE AUTOGENERATED OBJECT END*/  
  SET @l_SQL_String = CONCAT(CHAR(13) , '   
  ALTER VIEW ' , @l_fnl_src_vw_nm , ' AS')  
  SET @l_SQL_String += CHAR(13)  
  SET @l_SQL_String += CONCAT('  
  /*************************************************************************  
  * Name          : ' , @l_fnl_src_vw_nm , '  
  * Author        : Mehul Shah  
  * Purpose       : STG View for ' , @in_file_type , '  
  * View          :   
  * Test       : SELECT TOP 100 * FROM ' , @l_fnl_src_vw_nm , '  
  ***************************************************************************  
  * Change Date Change By  Change DSC    
  * ----------- ------------- -------------------------------------------  
  * ' , CONVERT(VARCHAR(10), GetDate(), 101) , ' forecast.PR_CNG_LD_CK_TBL_FROM_STG  Created  
  ***************************************************************************/  
  ')  
  SET @l_SQL_String += CHAR(13)  
  SET @l_SQL_String += CONCAT('SELECT ' , CHAR(13) ,   
  'CK.* ' , CHAR(13) ,   
  'FROM ' , @l_ck_stg_final_nm ,' CK ')  
  IF @in_debug = 'Y'  
  BEGIN  
   EXEC forecast.PR_PRINTMAX @l_SQL_String  
  END  
  ELSE   
  BEGIN   
   EXEC SP_EXECUTESQL @l_SQL_String  
  END  
  -- Send processing for dimension, val txt map and num fact .....   
  IF @in_file_type IN ('GFK','GFK_BRAND','IDC','NPD_TAB_CN','NPD_TAB_US','NPD_PC_CN','NPD_PC_MXN','NPD_COM_DIS','NPD_COM_RES','NPD_PC_US','IDC_TAB','IDC_SVR','IDC_X86','IDC_FCST','APMSC','AMSC_US','AMSC_CANADA','EMS','AMSC_LAR','EMEA_DISCOUNTER','CTX','NPD_WEEKLY_BIZ','NPD_WEEKLY','NPD_WEEKLY_TAB')  
  BEGIN  
   PRINT 'Calling forecast.PR_CNG_POST_DATA_LD_PRCS...Started'  
   SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_POST_DATA_LD_PRCS ',QUOTENAME(@in_file_type,''''),',',QUOTENAME(@in_cntnr_strt_tm,''''),',',QUOTENAME(@in_debug,''''))  
   EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
   Update forecast.cnst set cnst_val = 0,LAST_MOD_DT = Getdate() where cnst_nm = 'CNG_FILE_LOAD_STATUS'
   PRINT 'Calling forecast.PR_CNG_POST_DATA_LD_PRCS...Completed'  
  END  
  EXEC sp_refreshview @viewname = @l_dq_vw_nm  
  EXEC sp_refreshview @viewname = @l_fnl_src_vw_nm  
  EXEC sp_refreshview @viewname = @l_stg_src_vw_nm  
--#############################################  
  -- Sending load status mail   
  --#############################################  
  SET @l_publish_status = CONCAT('Raw File Upload Status for ' , @l_email_hdr_nm)  
  SET @l_email_title_ld_status = 'Raw File Upload Successful'  
  SET @l_ld_status = ' Raw File Upload Successful. Latest raw file data is now available in reporting.'  
  SET @l_v2_subj_success = CONCAT(@l_env , ' : CNG : ' , @l_email_title_ld_status, ' for ', @l_email_hdr_nm , ': ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/') , ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)  
  IF @in_debug = 'N'  
   EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION]  
       @in_subj = @l_v2_subj_success  
      ,@in_status = @l_email_title_ld_status  
      ,@in_msg = @l_ld_status  
      ,@in_file_list = @l_file_list  
      ,@in_dq_email_addr = @l_dq_rcv_email_addr  
      ,@in_usr_email_addr = @l_upld_usr_email_addr  
      ,@in_usr_nm = @l_upld_usr_nm  
      ,@in_publish_status = @l_publish_status  
      ,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr  
      ,@in_bypass_src_frmt_chk = @l_bypass_src_frmt_chk  
 END -- IF @l_continue = 'Y'  
 -- delete STG_CONS data  
 IF @in_debug = 'N'  
 BEGIN  
  --EXEC FORECAST.PR_CNG_DEL_STG_DATA @in_file_type  
  SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DEL_STG_DATA ',IIF(@in_file_type IN ('GFK','IDC'),QUOTENAME(@in_file_type,''''),QUOTENAME(@l_vndr_file_nm,'''')))  
  EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
 END  
 EXEC forecast.PR_PRINTD 'Procedure forecast.PR_CNG_LD_CK_TBL_FROM_STG Completed.....'  
 --UPDATE TABLE CNG_AUDIT_PROCESS WHEN LOAD IS COMPLETED  
 IF (@STATUS <> 'FAILED')  
 SET @STATUS='SUCESSFULL'  
 --SET @ERR_MSG = @l_ld_status  
 EXEC [FORECAST].[PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY]   
      @JOBID  
     ,@STATUS  
     ,@ERR_MSG
	
	Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
	Update forecast.cnst set CNST_VAL = '', LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_FileName'  
END TRY   
BEGIN CATCH  
 IF @@trancount > 0  
  ROLLBACK  
 -- Send file uploaded failed message.....  
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_COPY_TO_HIST ',QUOTENAME(@in_file_type,''''),',',QUOTENAME('DQ',''''),',',QUOTENAME(@in_debug,''''))  
 EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
  SET @l_ERR_MSG = ERROR_MESSAGE()  
  EXEC forecast.PR_STG_LD_JOB_UPDATE @JOB_ID = @JOB_ID, @IN_V2_PKG_NM = @l_file_name,@IN_V2_STS = 'FAILED',@IN_ERR_MSG = @l_ERR_MSG, @MODULE = 'CNG'
 --EXEC FORECAST.PR_CNG_DEL_STG_DATA @in_file_type  
 SELECT @l_cmd_syntax = CONCAT('forecast.PR_CNG_DEL_STG_DATA ',IIF(@in_file_type IN ('GFK','IDC','APMSC','EMS'),QUOTENAME(@in_file_type,''''),QUOTENAME(@l_vndr_file_nm,'''')))  
 EXEC [forecast].[PR_DYN_CALL] @l_cmd_syntax,@in_cntnr_strt_tm, 'Y', 'CNG'
 SET @l_publish_status = CONCAT('Raw File Upload Status for ' , @l_email_hdr_nm)  
 SET @l_email_title_ld_status = 'Raw File Upload Failed'  
 SET @l_ld_status = CONCAT('Raw File Upload Failed due to System Failure. Latest raw file data is NOT available in reporting.<BR><BR>',ERROR_MESSAGE(),' at Line# ',ERROR_LINE(),' & Error# ',ERROR_NUMBER())  
 SET @l_v2_subj_success = CONCAT(@l_env , ' : CNG : ' , @l_email_title_ld_status, ' for ', @l_email_hdr_nm , ': ' , REPLACE(CONVERT(VARCHAR(10), GETDATE(),110) ,'-' ,'/'), ' ' , CONVERT(VARCHAR(8), GETDATE(),114) , @l_server)  
 ----UPDATE TABLE CNG_AUDIT_PROCESS WHEN LOAD FAILED  
 SET @STATUS='FAILED'  
 SET @ERR_MSG = ERROR_MESSAGE()  
 EXEC [FORECAST].[PR_CNG_UPD_CNG_AUDIT_PROCESS_ENTRY]   
      @JOBID  
     ,@STATUS  
     ,@ERR_MSG  
 EXEC [forecast].[PR_CNG_SEND_PUBLISH_NOTIFICATION]  
   @in_subj = @l_v2_subj_success  
   ,@in_status = @l_email_title_ld_status  
   ,@in_msg = @l_ld_status  
   ,@in_file_list = @l_file_list  
   ,@in_dq_email_addr = @l_dq_rcv_email_addr  
   ,@in_usr_email_addr = @l_upld_usr_email_addr  
   ,@in_usr_nm = @l_upld_usr_nm  
   ,@in_publish_status = @l_publish_status  
    ,@in_dq_tech_ownr_email_addr = @l_dq_tech_ownr_email_addr  
    ,@in_bypass_src_frmt_chk = @l_bypass_src_frmt_chk  
	
 EXEC forecast.PR_CUSTOM_ERRMSG @Exit_or_continue= 'CONTINUE';
 Update forecast.cnst set CNST_VAL = 0, LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_STATUS'
 Update forecast.cnst set CNST_VAL = '', LAST_MOD_DT = GETDATE() where CNST_NM = 'CNG_FILE_LOAD_FileName'  
 EXEC forecast.PR_STG_LD_JOB_UPDATE @JOB_ID = @JOB_ID, @IN_V2_PKG_NM = @l_file_name,@IN_V2_STS = 'FAILED',@IN_ERR_MSG = @l_ERR_MSG, @MODULE = 'CNG'
END CATCH;  
