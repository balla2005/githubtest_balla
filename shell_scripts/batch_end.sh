#! /bin/bash
#Author :Sballa
# Script Name:        batch_end.sh
# Description:        This script updates the UDAP_BATCH_DATE as ETL_BATCH_DATE after the batch finishes.
# Usage      :        batch_end.sh

set -e

. ${ENV_BASE_DIR}/env/${env}/etl/env.properties
. `which util_file.sh`

LOG_FILE=$LOG_DIR/batch_end$(date +%Y%m%d).log
BATCH_START_DT=$(cat $TMP_DIR/batch_curr_date.txt)
ORA_LOG_FILE_TO_CHECK=$LOG_DIR/execute_package${ETL_BATCH_DATE}.log

log_message "Starting batch_end.sh" ${LOG_FILE}

# Check the number of parameters passed
if [[ $# -ne 0 ]]; then
   log_error "ERROR: Invalid number of parameters passed.  Usage: batch_end.sh" ${LOG_FILE}
fi

# Check for dates in TMP files - Dates will be inserted in Temp files after batch success
if [[ -f $TMP_DIR/batch_curr_date.txt ]]; then

   if [[ ! -f $TMP_DIR/research_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/finance_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/student_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/stopone_batch_date.txt ]] || \
	  [[ ! -f $TMP_DIR/ids_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/ids_minerva_batch_date.txt ]] || \
	  [[ ! -f $TMP_DIR/ids_hyperion_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/dds_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/mv_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/mv_par_batch_date.txt ]] || \
      [[ ! -f $TMP_DIR/noncrit_batch_date.txt ]]; then

      if [[ ! -f $TMP_DIR/finance_batch_date.txt ]]; then
         log_message "Finance incremental Load did not commence or $TMP_DIR/finance_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "Finance incremental load did not commence or $TMP_DIR/finance_batch_date.txt doesn't exist"
         log_error "Failure - Please verify Finance Job Status or Batch Init" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/student_batch_date.txt ]]; then
         log_message "Student incremental Load did not commence or $TMP_DIR/student_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "Student incremental load did not commence or $TMP_DIR/student_batch_date.txt doesn't exist"
         log_error "Failure - Please verify Student Job Status or Batch Init" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/stopone_batch_date.txt ]]; then
         log_message "StopOne incremental Load did not commence or $TMP_DIR/stopone_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "StopOne incremental load did not commence or $TMP_DIR/stopone_batch_date.txt doesn't exist"
         log_error "Failure - Please verify StopOne Job Status or Batch Init" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/research_batch_date.txt ]]; then
         log_message "Research incremental load did not commence or $TMP_DIR/research_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "Research incremental load did not commence or $TMP_DIR/research_batch_date.txt doesn't exist"
         log_error "Failure - Please verify Research Job Status or Batch Init" ${LOG_FILE}
      fi
	  
	   
	  if [[ ! -f $TMP_DIR/ids_batch_date.txt ]]; then
         log_message "IDS jobs did not execute successfully or $TMP_DIR/ids_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "IDS jobs did not execute successfully or $TMP_DIR/ids_batch_date.txt doesn't exist"
         log_error "Failure - Please verify IDS Jobs Status for any failure" ${LOG_FILE}
      fi
	  
	  if [[ ! -f $TMP_DIR/ids_hyperion_batch_date.txt ]]; then
         log_message "Hyperion IDS jobs did not execute successfully or $TMP_DIR/ids_hyperion_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "Hyperion IDS jobs did not execute successfully or $TMP_DIR/ids_hyperion_batch_date.txt doesn't exist"
         log_error "Failure - Please verify Hyperion IDS Jobs Status for any failure" ${LOG_FILE}
      fi
	  
	  if [[ ! -f $TMP_DIR/ids_minerva_batch_date.txt ]]; then
         log_message "Minerva IDS jobs did not execute successfully or $TMP_DIR/ids_minerva_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "Minerva IDS jobs did not execute successfully or $TMP_DIR/ids_minerva_batch_date.txt doesn't exist"
         log_error "Failure - Please verify Minerva IDS Jobs Status for any failure" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/dds_batch_date.txt ]]; then
         log_message "DDS jobs did not execute successfully or $TMP_DIR/dds_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "DDS jobs did not execute successfully or $TMP_DIR/dds_batch_date.txt doesn't exist"
         log_error "Failure - Please verify DDS Jobs Status for any failure" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/mv_batch_date.txt ]]; then
         log_message "DDS MV jobs did not execute successfully or $TMP_DIR/mv_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "DDS MV jobs did not execute successfully or $TMP_DIR/mv_batch_date.txt doesn't exist"
         log_error "Failure - Please verify DDS MV Jobs Status for any failure" ${LOG_FILE}
      fi

      if [[ ! -f $TMP_DIR/mv_par_batch_date.txt ]]; then
         log_message "DDS Parallel MV jobs did not execute successfully or $TMP_DIR/mv_par_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "DDS Parallel MV jobs did not execute successfully or $TMP_DIR/mv_par_batch_date.txt doesn't exist"
         log_error "Failure - Please verify DDS Parallel MV Jobs Status for any failure" ${LOG_FILE}
      fi
      
      if [[ ! -f $TMP_DIR/noncrit_batch_date.txt ]]; then
         log_message "RDS Non-Critical jobs did not execute successfully or $TMP_DIR/noncrit_batch_date.txt doesn't exist " ${LOG_FILE}
         send_batch_status_email "Batch Failure. Please Investigate" "RDS Non-Critical jobs did not execute successfully or $TMP_DIR/noncrit_batch_date.txt doesn't exist"
         log_error "Failure - Please verify RDS Non-Critical Jobs Status for any failure" ${LOG_FILE}
      fi  


   else

      FIN_BATCH_END_DT=$(cat $TMP_DIR/finance_batch_date.txt)
      STU_BATCH_END_DT=$(cat $TMP_DIR/student_batch_date.txt)
      STPONE_BATCH_END_DT=$(cat $TMP_DIR/stopone_batch_date.txt)
      RSRCH_BATCH_END_DT=$(cat $TMP_DIR/research_batch_date.txt)
      IDS_BATCH_END_DT=$(cat $TMP_DIR/ids_batch_date.txt)
      IDS_HY_BATCH_END_DT=$(cat $TMP_DIR/ids_hyperion_batch_date.txt)
      IDS_MINERVA_BATCH_END_DT=$(cat $TMP_DIR/ids_minerva_batch_date.txt)
      DDS_BATCH_END_DT=$(cat $TMP_DIR/dds_batch_date.txt)
      MV_BATCH_END_DT=$(cat $TMP_DIR/mv_batch_date.txt)
      MV_PAR_BATCH_END_DT=$(cat $TMP_DIR/mv_par_batch_date.txt)
      NONCRIT_BATCH_END_DT=$(cat $TMP_DIR/noncrit_batch_date.txt)

      if [[ (${BATCH_START_DT} -eq ${IDS_BATCH_END_DT}) && \
	    (${BATCH_START_DT} -eq ${IDS_MINERVA_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${IDS_HY_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${STPONE_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${STU_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${FIN_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${RSRCH_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${DDS_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${MV_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${MV_PAR_BATCH_END_DT}) && \
            (${BATCH_START_DT} -eq ${NONCRIT_BATCH_END_DT}) ]] ; then

         log_message "Date in $TMP_DIR/batch_curr_date.txt:${BATCH_START_DT} is same as $TMP_DIR/student_batch_date.txt:${STU_BATCH_END_DT} and $TMP_DIR/finance_batch_date.txt:${FIN_BATCH_END_DT} and $TMP_DIR/stopone_batch_date.txt:${STPONE_BATCH_END_DT} and $TMP_DIR/ids_hyperion_batch_date.txt:${IDS_HY_BATCH_END_DT} and $TMP_DIR/ids_batch_date.txt:${IDS_BATCH_END_DT} and $TMP_DIR/ids_minerva_batch_date.txt:${IDS_MINERVA_BATCH_END_DT} and $TMP_DIR/research_batch_date.txt:${RSRCH_BATCH_END_DT} and $TMP_DIR/dds_batch_date.txt:${DDS_BATCH_END_DT} and $TMP_DIR/noncrit_batch_date.txt:${NONCRIT_BATCH_END_DT} Proceeding to batch_end.sh " ${LOG_FILE}

      else

         send_batch_status_email "Batch Failure. Please Investigate" "Date in $TMP_DIR/batch_curr_date.txt is not matching with one or more of the module batch_date.txt files. Please verify module batch status or batch init"

         log_error "Failure - Date in  $TMP_DIR/batch_curr_date.txt:${BATCH_START_DT}
                    IS NOT  same as $TMP_DIR/student_batch_date.txt:${STU_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/finance_batch_date.txt:${FIN_BATCH_END_DT}  AND
                    IS NOT same as $TMP_DIR/stopone_batch_date.txt:${STPONE_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/research_batch_date.txt:${RSRCH_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/ids_hyperion_batch_date.txt:${IDS_HY_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/ids_batch_date.txt:${IDS_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/ids_minerva_batch_date.txt:${IDS_MINERVA_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/dds_batch_date.txt:${DDS_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/mv_batch_date.txt:${MV_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/mv_par_batch_date.txt:${MV_PAR_BATCH_END_DT} AND
                    IS NOT same as $TMP_DIR/noncrit_batch_date.txt:${NONCRIT_BATCH_END_DT} .
                    Please verify Student or Finance or StopOne or Research or IDS or Non-Critical Job Status or Batch Init" ${LOG_FILE}
      fi
   fi

else

   send_batch_status_email "Batch Failure. Please Investigate" "$TMP_DIR/batch_curr_date.txt does not exists"
   log_error "Missing $TMP_DIR/batch_curr_date.txt:${BATCH_START_DT} " ${LOG_FILE}

fi


log_message "Setting UDAP_BATCH_DATE to ${ETL_BATCH_DATE}" ${LOG_FILE}

return_val2=$(sqlplus -s $CONNECTION_STR <<EOL>> $LOG_DIR/batch_end$(date +%Y%m%d).log
              SET HEAD OFF ECHO OFF FEED OFF SERVEROUT ON SIZE 1000000
              WHENEVER SQLERROR EXIT FAILURE
              DECLARE
                 V_VAR VARCHAR2(1000);
              BEGIN
         V_VAR:=CTL.PKG_DML_UTL.F_SET_DATE_SYS_PARAM('UDAP_BATCH_DATE',TO_DATE('${ETL_BATCH_DATE}','YYYYMMDD'));
                 DBMS_OUTPUT.PUT_LINE(V_VAR);
              END;
              /
              EXIT;
EOL
             )

if [[ $? != 0 ]]; then
   send_batch_status_email "Batch Failure. Please Investigate" "Setting up UDAP_BATCH_DATE to ETL_BATCH_DATE failed"
   log_error "Failure: $? " ${LOG_FILE}
fi

log_message "All batches successfully completed. batch_end.sh successfully executed" ${LOG_FILE}

get_send_success_email_flg

if [ ${SEND_SUCCESS_EMAIL_FLAG} = "Y" ];then
    send_batch_status_email "Batch Completed Successfully" "Refer below to the current status of batch"
fi
