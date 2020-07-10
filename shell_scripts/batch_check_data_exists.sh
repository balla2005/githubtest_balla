#! /bin/bash

# Script Name:   batch_check_data_exists.sh
# Description:   Script to check if data exists in a given table for a given where clause.
# Usage:         batch_check_data_exists <OWNER> <TABLE_NAME> <WHERE_CLAUSE>
# Usage Example: batch_check_data_exists RDS VTAC_D1_MASTER_VW "WHERE SNAPSHOT_DT = TO_DATE('${ETL_BATCH_DT}','YYYYMMDD')"

set -e

. ${ENV_BASE_DIR}/env/${env}/etl/env.properties
. `which util_file.sh`

P_OWNER=$1
P_TABLE_NAME=$2
P_WHERE_CLAUSE=$3

LOG_FILE=$LOG_DIR/batch_check_data_exists_$(date +%Y%m%d).log

log_message "Checking data exists in ${P_OWNER}.${P_TABLE_NAME} matching ${P_WHERE_CLAUSE}" ${LOG_FILE}

DATA_EXISTS_FLAG=$(sqlplus -s $CONNECTION_STR<<EOL
       set head off echo off feed off serverout on size 1000000
       WHENEVER SQLERROR EXIT SQL.SQLCODE
       SELECT NVL((SELECT 0
                    FROM DUAL
                    WHERE EXISTS (SELECT 1
                                    FROM ${P_OWNER}.${P_TABLE_NAME}
                                   ${P_WHERE_CLAUSE})),1)
         FROM DUAL;
       EXIT;
EOL
)

log_message "DATA_EXISTS_FLAG: $DATA_EXISTS_FLAG" ${LOG_FILE}

exit $DATA_EXISTS_FLAG
